package rep02;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class PartitionReplica {
    static final long IDLE_MS = 2000; // sem mensagens durante este tempo → mostra o resumo da partição

    // simulação de perdas, aplicada só às mensagens recebidas pelo PubSub
    static final double PERDA = Double.parseDouble(System.getProperty("perda", "0"));
    static final long DESCARTAR = Long.parseLong(System.getProperty("descartar", "0"));

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <partições> [portoApi]   (ex.: PartitionReplica 1 0,1)");
            return;
        }
        String id = args[0];
        String api = "http://127.0.0.1:" + (args.length > 2 ? args[2] : "5001") + "/api/v0";
        List<Partition> parts = new ArrayList<>();
        try {
            for (String s : args[1].split(",")) {
                parts.add(new Partition(id, Integer.parseInt(s.trim()), api));
            }
        } catch (NumberFormatException e) {
            System.out.println("Lista de partições inválida: " + args[1]);
            return;
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Não foi possível ler o ficheiro da réplica: " + e);
            return;
        }
        System.out.println("Réplica " + id + " | partições " + args[1] + " | nó " + api
                + (PERDA > 0 ? " | perda simulada " + PERDA : "") + (DESCARTAR > 0 ? " | descartar " + DESCARTAR : ""));
        List<Thread> threads = new ArrayList<>();
        for (Partition p : parts) {
            System.out.println("[p" + p.k + "] " + p.file + " (último registo no ficheiro: " + p.last + ")");
            Thread t = new Thread(p::run, "sub-p" + p.k); // uma thread de subscrição por partição
            threads.add(t);
            t.start();
        }
        startMonitor(parts);
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    // estado de uma partição: só é alterado pela sua thread de subscrição (e lido pelo monitor),
    // por isso os métodos são synchronized no próprio objeto; partições diferentes não partilham estado
    static class Partition {
        final int k;
        final String file;
        final RecordFile records;
        final IpfsPubSub ps;
        long last; // último seq escrito no ficheiro
        final TreeMap<Long, SensorRecord> pending = new TreeMap<>(); // retidos, por seq
        String leaderHost; // aprendidos com cada REG recebido
        int leaderPort;
        boolean discardedOnce;

        // estatística desde o último resumo
        int viaPubSub, viaTcp, connections, discarded;
        long lastReceiveAt, lastWriteAt;
        boolean active;

        Partition(String id, int k, String api) throws IOException {
            this.k = k;
            this.file = "replica-" + id + "-p" + k + ".txt";
            this.records = new RecordFile(file);
            this.ps = new IpfsPubSub(api);
            this.last = records.lastSeq();
        }

        void run() {
            try {
                ps.subscribe(Wire.topicData(k), this::onMessage); // BLOQUEIA: não termina; cada mensagem chama onMessage
                log("a subscrição terminou (o daemon IPFS fechou a ligação).");
            } catch (IOException | UncheckedIOException e) {
                log("IO: " + e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log("interrompida: " + e);
            }
        }

        synchronized void onMessage(String msg) {
            lastReceiveAt = System.currentTimeMillis();
            active = true;
            String host;
            int port;
            SensorRecord r;
            try {
                String[] f = Wire.regParts(msg);
                host = f[0];
                port = Integer.parseInt(f[1]);
                r = SensorRecord.fromLine(f[2]); // aqui a réplica confia que o que recebeu é um registo válido
                if (r.getSeq() < 1 || host.isBlank() || port < 1 || port > 65535) {
                    throw new IllegalArgumentException();
                }
            } catch (IllegalArgumentException e) { // inclui NumberFormatException
                log("REJEITADO: " + msg);
                return;
            }
            long seq = r.getSeq();
            if (PERDA > 0 && ThreadLocalRandom.current().nextDouble() < PERDA
                    || seq == DESCARTAR && !discardedOnce) {
                if (seq == DESCARTAR) discardedOnce = true;
                discarded++;
                log("DESCARTADO (simulação): " + seq);
                return;
            }
            leaderHost = host; // o endereço do líder vem em CADA mensagem: segue-o se mudar
            leaderPort = port;
            try {
                apply(r, false);
                if (pending.size() > Wire.LIMITE_RETIDOS) {
                    recover(); // na própria thread de subscrição: o PubSub desta partição espera
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e); // falha a escrever no ficheiro local
            }
        }

        // regra de aplicação: o ficheiro só cresce pela ordem do líder, sem saltos
        void apply(SensorRecord r, boolean viaTcp) throws IOException {
            long seq = r.getSeq();
            if (seq <= last || pending.containsKey(seq)) {
                log("DUPLICADO: " + seq);
                return;
            }
            if (seq > last + 1) {
                pending.put(seq, r);
                log("EM ESPERA: " + seq + " (falta o registo " + (last + 1) + ")");
                return;
            }
            write(r, viaTcp);
            while (pending.containsKey(last + 1)) {
                write(pending.remove(last + 1), false); // retidos que passaram a estar em sequência
            }
        }

        void write(SensorRecord r, boolean tcp) throws IOException {
            records.append(r);
            long now = System.currentTimeMillis();
            boolean isolated = now - lastWriteAt > 200;
            last = r.getSeq();
            lastWriteAt = now;
            if (tcp) viaTcp++; else viaPubSub++;
            if (isolated) { // durante uma rajada não se mostra cada registo, para não atrasar a receção
                log("Aplicado: " + r.toLine());
            }
        }

        // pede ao líder, por TCP, os registos entre o último escrito e o primeiro retido
        void recover() {
            long from = last + 1;
            long to = pending.firstKey() - 1;
            connections++;
            log("RECUPERAÇÃO: " + from + ".." + to + " pedida a " + leaderHost + ":" + leaderPort);
            int got = 0;
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(leaderHost, leaderPort), Wire.T_TCP_MS); // BLOQUEIA: até a ligação ser aceite
                s.setSoTimeout(Wire.T_TCP_MS);                                           // readLine deixa de bloquear indefinidamente
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
                out.println("PEDIDO " + from + " " + to);
                while (true) {
                    String line = in.readLine(); // BLOQUEIA: até chegar a linha seguinte (no máximo T_TCP_MS)
                    if (line == null) throw new EOFException("ligação fechada antes de FIM");
                    if (line.equals("FIM")) break;
                    if (line.startsWith("ERRO")) throw new IOException("o líder respondeu: " + line);
                    SensorRecord r = SensorRecord.fromLine(line);
                    log("RECUPERADO: " + r.getSeq());
                    got++;
                    apply(r, true); // mesma regra: escreve e despeja os retidos que passaram a estar em sequência
                }
                log("RECUPERAÇÃO CONCLUÍDA: " + got + " registos recebidos (último no ficheiro: " + last
                        + ", retidos: " + pending.size() + ")");
            } catch (IOException | IllegalArgumentException e) {
                // os registos já escritos ficam; os retidos continuam em memória; tenta de novo na próxima receção
                log("RECUPERAÇÃO FALHOU: " + e + " (" + got + " registos recebidos antes da falha)");
            }
        }

        synchronized void report() {
            if (active && System.currentTimeMillis() - lastReceiveAt > IDLE_MS) {
                log("RESUMO: escritos " + (viaPubSub + viaTcp) + " (PubSub " + viaPubSub + ", TCP " + viaTcp + ")"
                        + " | ligações de recuperação " + connections + " | descartados " + discarded
                        + " | retidos " + pending.size() + " | último seq " + last + " | última escrita " + lastWriteAt);
                viaPubSub = viaTcp = connections = discarded = 0;
                active = false;
            }
        }

        void log(String s) {
            System.out.println("[p" + k + "] " + s);
        }
    }

    static void startMonitor(List<Partition> parts) {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    return;
                }
                for (Partition p : parts) {
                    p.report();
                }
            }
        }, "resumo");
        t.setDaemon(true);
        t.start();
    }
}
