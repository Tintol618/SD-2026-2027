package rep04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class Producer {
    static final int SENSORES = 10; // as rajadas usam os sensores S01..S10

    static int n;
    static Partitioner partitioner;
    static IpfsPubSub ps;
    static HeartbeatMonitor[] monitores; // um por partição; cada um tem o seu lock interno
    static final Path monitorFile = Paths.get("monitor-produtor.txt");

    public static void main(String[] args) {
        try {
            n = args.length > 0 ? Integer.parseInt(args[0]) : 1;
            partitioner = new Partitioner(n);
        } catch (RuntimeException e) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        ps = new IpfsPubSub(api); // não abre ligação: só guarda o endereço da API do daemon local
        monitores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, Producer::event);
            final int part = k;
            Thread t = new Thread(() -> subscribe(part), "sub-p" + k);
            t.setDaemon(true); // 'sair' termina o produtor sem esperar pelas subscrições
            t.start();
        }
        startVerification();
        System.out.println("Produtor pronto (N = " + n + ", API " + api + ", Δ = " + Wire.PERIODO_HB_MS
                + " ms, TIMEOUT = " + Wire.FATOR_TIMEOUT * Wire.PERIODO_HB_MS + " ms).");
        System.out.println("Formato: <sensor> <temperatura> | 'rajada <n> <ms>' | 'rajada-quente <n> <ms>' | 'sair'");
        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break; // BLOQUEIA: à espera de que o utilizador escreva uma linha
                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;
                if (line.equalsIgnoreCase("sair")) break;
                String[] p = line.split("\\s+");
                if (p.length == 3 && (p[0].equalsIgnoreCase("rajada") || p[0].equalsIgnoreCase("rajada-quente"))) {
                    burst(p[1], p[2], p[0].equalsIgnoreCase("rajada-quente"));
                    continue;
                }
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }
                int k = partitioner.of(p[0]);
                String msg = Wire.in(p[0], temp, 0, new VectorClock(n)); // o produtor não observa registos: L = 0, V = 0
                if (send(k, p[0], msg)) {
                    System.out.println("Publicado: " + msg + " → p" + k);
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Interrompido: " + e);
        }
    }

    static synchronized void event(String e) {
        String line = System.currentTimeMillis() + ";" + e;
        System.out.println(line);
        try {
            Files.writeString(monitorFile, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ex) {
            System.out.println("Não foi possível escrever em " + monitorFile + ": " + ex);
        }
    }

    static void startVerification() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(Wire.VERIFICACAO_MS);
                } catch (InterruptedException e) {
                    return;
                }
                for (HeartbeatMonitor m : monitores) {
                    m.check();
                }
            }
        }, "verificacao");
        t.setDaemon(true);
        t.start();
    }

    static void subscribe(int k) {
        try {
            ps.subscribe(Wire.topicData(k), msg -> onLote(k, msg));
            System.out.println("[p" + k + "] a subscrição terminou (o daemon IPFS fechou a ligação).");
        } catch (IOException | UncheckedIOException e) {
            System.out.println("[p" + k + "] IO: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // o produtor só precisa do sinal de vida: interpreta apenas o cabeçalho do LOTE
    static void onLote(int k, String msg) {
        Batch b;
        int count;
        try {
            b = Batch.decodeHeader(msg);
            count = Batch.headerCount(msg);
        } catch (IllegalArgumentException e) {
            System.out.println("[p" + k + "] REJEITADO: " + msg.replace('\n', '|'));
            return;
        }
        monitores[k].onHeartbeat(b.inc, b.hb);
        event("RECEBIDO;p" + k + ";inc=" + b.inc + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq + ";n=" + count);
    }

    // rajada <n> <ms>: n leituras de sensores aleatórios entre S01 e S10, com ms de intervalo entre envios
    // rajada-quente <n> <ms>: igual, mas 80 % das leituras vêm de S01
    static void burst(String nArg, String msArg, boolean hot) throws IOException, InterruptedException {
        int count;
        long pause;
        try {
            count = Integer.parseInt(nArg);
            pause = Long.parseLong(msArg);
        } catch (NumberFormatException e) {
            System.out.println("Formato: rajada <n> <ms>, com n >= 1 e ms >= 0");
            return;
        }
        if (count < 1 || pause < 0) {
            System.out.println("Formato: rajada <n> <ms>, com n >= 1 e ms >= 0");
            return;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int[] perPartition = new int[n];
        int failed = 0;
        long first = 0, last = 0;
        for (int i = 0; i < count; i++) {
            String sensor = hot && rnd.nextDouble() < 0.8 ? "S01" : String.format("S%02d", rnd.nextInt(1, SENSORES + 1));
            double temp = Math.round(rnd.nextDouble(15.0, 30.0) * 10) / 10.0;
            int k = partitioner.of(sensor);
            if (send(k, sensor, Wire.in(sensor, temp, 0, new VectorClock(n)))) {
                perPartition[k]++;
            } else {
                failed++;
            }
            last = System.currentTimeMillis();
            if (i == 0) first = last;
            if (pause > 0 && i < count - 1) Thread.sleep(pause);
        }
        long ms = last - first;
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < perPartition.length; k++) {
            sb.append(k == 0 ? "" : " | ").append("p").append(k).append(": ").append(perPartition[k]);
        }
        System.out.println((hot ? "Rajada quente" : "Rajada") + " de " + count + " leituras (S01..S" + SENSORES + ", "
                + pause + " ms) → " + sb + (failed > 0 ? " | publicações recusadas pelo daemon: " + failed : ""));
        System.out.println("  primeira publicação: " + first + " | última publicação: " + last);
        System.out.printf("  tempo de envio: %d ms | débito: %.0f leituras/s%n", ms, count * 1000.0 / Math.max(ms, 1));
    }

    // publica uma leitura no tópico de entrada da partição k. Se o líder de k estiver suspeito, avisa e envia
    // na mesma (o produtor não retém leituras). Se o daemon recusar a publicação, a leitura perde-se e o
    // produtor continua; se o daemon não estiver acessível (ConnectException), a exceção termina o produtor.
    static boolean send(int k, String sensor, String msg) throws IOException, InterruptedException {
        if (monitores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: líder p" + k + " suspeito; leitura " + sensor + " pode perder-se");
        }
        try {
            ps.publish(Wire.topicIn(k), msg); // BLOQUEIA: até o daemon local responder
            return true;
        } catch (ConnectException e) {
            throw e;
        } catch (IOException e) {
            System.out.println("PUBLICAÇÃO RECUSADA (" + msg + "): " + e.getMessage());
            return false;
        }
    }
}
