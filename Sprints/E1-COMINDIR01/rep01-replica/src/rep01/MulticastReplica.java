package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.TreeMap;
import java.util.concurrent.*;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;
    static final long BURST_IDLE_MS = 2000; // 2 s sem registos SIM: a rajada terminou

    private static RecordFile file;
    private static long last; // seq do último registo escrito no ficheiro
    private static final TreeMap<Long, SensorRecord> pending = new TreeMap<>(); // retidos em memória, por seq

    // estatística da rajada (registos do sensor SIM, ponto 7)
    private static long burstFirst, burstLast; // instantes de receção
    private static int burstReceived, burstWritten, burstDuplicates;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        file = new RecordFile("replica-" + id + ".txt");
        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            last = file.lastSeq(); // último número de sequência já aplicado
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group); // passa a receber o que é enviado ao grupo
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT + " (último registo aplicado: " + last + ")");
            startBurstReporter();
            byte[] buffer = new byte[1000];
            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p); // BLOQUEIA: à espera de um datagrama enviado ao grupo (de quem quer que seja)
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                if (p.getLength() == buffer.length) { // encheu o buffer: o datagrama pode ter sido truncado, sem aviso
                    System.out.println("REJEITADO: " + line);
                    continue;
                }
                onMessage(line);
            }
        } catch (IOException e) {
            System.out.println("IO: " + e);
        }
    }

    // aplica a regra do número de sequência: o ficheiro só cresce pela ordem do líder, sem saltos
    static synchronized void onMessage(String line) throws IOException {
        SensorRecord r;
        try {
            r = SensorRecord.fromLine(line); // aqui a réplica CONFIA que o que recebeu é um registo válido
            if (r.getSeq() < 1) {
                throw new IllegalArgumentException("seq inválido");
            }
            LocalDateTime.parse(r.getTimestamp()); // o instante também tem de ser válido
        } catch (IllegalArgumentException | DateTimeException e) { // inclui NumberFormatException
            System.out.println("REJEITADO: " + line);
            return;
        }
        boolean sim = r.getSensorId().equals("SIM");
        if (sim) {
            burstLast = System.currentTimeMillis();
            if (burstReceived++ == 0) burstFirst = burstLast;
        }
        long seq = r.getSeq();
        if (seq <= last || pending.containsKey(seq)) {
            if (sim) burstDuplicates++;
            System.out.println("DUPLICADO: " + seq);
        } else if (seq > last + 1) {
            pending.put(seq, r);
            System.out.println("EM ESPERA: " + seq + " (falta o registo " + (last + 1) + ")");
        } else {
            write(r);
            while (!pending.isEmpty() && pending.firstKey() == last + 1) {
                write(pending.pollFirstEntry().getValue()); // retidos que passaram a estar em sequência
            }
        }
    }

    private static void write(SensorRecord r) throws IOException {
        file.append(r);
        last = r.getSeq();
        if (r.getSensorId().equals("SIM")) {
            burstWritten++; // os registos da rajada não são mostrados um a um: a consola atrasaria a receção
        } else {
            System.out.println("Aplicado: " + r.toLine());
        }
    }

    // thread auxiliar: quando deixam de chegar registos SIM, mostra o resumo da rajada
    private static void startBurstReporter() {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread t = new Thread(task, "resumo-rajada");
            t.setDaemon(true);
            return t;
        });
        timer.scheduleWithFixedDelay(MulticastReplica::reportBurstIfIdle, 500, 500, TimeUnit.MILLISECONDS);
    }

    static synchronized void reportBurstIfIdle() {
        if (burstReceived == 0 || System.currentTimeMillis() - burstLast < BURST_IDLE_MS) return;
        long holes = pending.isEmpty() ? 0 : pending.lastKey() - last - pending.size();
        System.out.println("Rajada: recebidos " + burstReceived + " | escritos no ficheiro " + burstWritten
                + " | retidos em memória " + pending.size() + " | duplicados " + burstDuplicates
                + " | em falta (buracos detetados) " + holes + " | último seq escrito " + last);
        System.out.println("  receção do primeiro: " + burstFirst + " | receção do último: " + burstLast
                + " (" + (burstLast - burstFirst) + " ms)");
        burstReceived = burstWritten = burstDuplicates = 0;
    }
}
