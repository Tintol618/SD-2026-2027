package raft04;

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
    static HeartbeatMonitor[] monitores;
    static final Path monitorFile = Paths.get("monitor-produtor.txt");
    static final Path sentFile = Paths.get("produtor.txt");

    public static void main(String[] args) {
        try {
            n = args.length > 0 ? Integer.parseInt(args[0]) : 3;
            partitioner = new Partitioner(n);
        } catch (RuntimeException e) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        ps = new IpfsPubSub(api);
        monitores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, Producer::event);
            final int part = k;
            Thread t = new Thread(() -> subscribe(part), "sub-p" + k);
            t.setDaemon(true);
            t.start();
        }
        startVerification();
        System.out.println("Produtor pronto (N = " + n + ", API " + api + ", Δ = " + Wire.PERIODO_HB_MS
                + " ms, TIMEOUT = " + Wire.FATOR_TIMEOUT * Wire.PERIODO_HB_MS + " ms).");
        System.out.println("Formato: <sensor> <temperatura> | 'rajada <n> <ms>' | 'sair'");
        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;
                if (line.equalsIgnoreCase("sair")) break;
                String[] p = line.split("\\s+");
                if (p.length == 3 && p[0].equalsIgnoreCase("rajada")) {
                    burst(p[1], p[2]);
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
                if (send(p[0], temp)) {
                    System.out.println("Publicado: " + p[0] + " " + temp + " → p" + partitioner.of(p[0]));
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static synchronized void event(String e) {
        String line = System.currentTimeMillis() + ";" + e;
        System.out.println(line);
        append(monitorFile, line);
    }

    static synchronized void append(Path f, String line) {
        try {
            Files.writeString(f, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ex) {
            System.out.println("Não foi possível escrever em " + f + ": " + ex);
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
                for (HeartbeatMonitor m : monitores) m.check();
            }
        }, "verificacao");
        t.setDaemon(true);
        t.start();
    }

    static void subscribe(int k) {
        while (true) {
            try {
                ps.subscribe(Wire.topicData(k), msg -> onLote(k, msg));
            } catch (IOException | UncheckedIOException e) {
                System.out.println("[p" + k + "] IO: " + e);
            } catch (InterruptedException e) {
                return;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    // o produtor só precisa do sinal de vida: interpreta apenas o cabeçalho do LOTE
    static void onLote(int k, String msg) {
        try {
            Batch b = Batch.decodeHeader(msg);
            monitores[k].onHeartbeat(b.term, b.leader, b.hb);
        } catch (IllegalArgumentException e) {
            System.out.println("[p" + k + "] REJEITADO: " + msg.split("\n")[0]);
        }
    }

    // rajada <n> <ms>: n leituras de sensores aleatórios entre S01 e S10, com ms de intervalo entre envios
    static void burst(String nArg, String msArg) throws IOException, InterruptedException {
        int count;
        long pause;
        try {
            count = Integer.parseInt(nArg);
            pause = Long.parseLong(msArg);
            if (count < 1 || pause < 0) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            System.out.println("Formato: rajada <n> <ms>, com n >= 1 e ms >= 0");
            return;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int[] perPartition = new int[n];
        long first = System.currentTimeMillis();
        for (int c = 0; c < count; c++) {
            String sensor = String.format("S%02d", rnd.nextInt(1, SENSORES + 1));
            double temp = Math.round(rnd.nextDouble(15.0, 30.0) * 10) / 10.0;
            if (send(sensor, temp)) perPartition[partitioner.of(sensor)]++;
            if (pause > 0 && c < count - 1) Thread.sleep(pause);
        }
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < n; k++) sb.append(k == 0 ? "" : " | ").append("p").append(k).append(": ").append(perPartition[k]);
        System.out.println("Rajada de " + count + " leituras (" + pause + " ms) em " + (System.currentTimeMillis() - first) + " ms → " + sb);
    }

    // publica uma leitura no tópico de entrada da partição; se a partição não tiver líder conhecido,
    // avisa e envia na mesma (o produtor não retém leituras). Cada IN enviado fica em produtor.txt.
    static boolean send(String sensor, double temp) throws IOException, InterruptedException {
        int k = partitioner.of(sensor);
        if (monitores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: partição p" + k + " sem líder conhecido; leitura " + sensor + " pode perder-se");
        }
        try {
            ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0, new VectorClock(n)));
        } catch (ConnectException e) {
            throw e;
        } catch (IOException e) {
            System.out.println("PUBLICAÇÃO RECUSADA (" + sensor + " " + temp + "): " + e.getMessage());
            return false;
        }
        append(sentFile, System.currentTimeMillis() + ";p" + k + ";" + sensor + ";" + temp);
        return true;
    }
}
