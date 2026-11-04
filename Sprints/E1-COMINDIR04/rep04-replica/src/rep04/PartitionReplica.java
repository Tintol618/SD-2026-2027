package rep04;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class PartitionReplica {
    static final long IDLE_MS = 2000; // sem mensagens com registos durante este tempo → mostra o resumo da partição

    // simulação de perdas, aplicada a cada registo recebido dentro de um LOTE
    static final double PERDA = Double.parseDouble(System.getProperty("perda", "0"));
    static final long DESCARTAR = Long.parseLong(System.getProperty("descartar", "0"));

    static Path monitorFile;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <partições> [portoApi]   (ex.: PartitionReplica 1 0,1)");
            return;
        }
        String id = args[0];
        String api = "http://127.0.0.1:" + (args.length > 2 ? args[2] : "5001") + "/api/v0";
        monitorFile = Paths.get("monitor-replica" + id + ".txt");
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
                + " | Δ = " + Wire.PERIODO_HB_MS + " ms | TIMEOUT = " + Wire.FATOR_TIMEOUT * Wire.PERIODO_HB_MS + " ms"
                + (PERDA > 0 ? " | perda simulada " + PERDA : "") + (DESCARTAR > 0 ? " | descartar " + DESCARTAR : ""));
        List<Thread> threads = new ArrayList<>();
        for (Partition p : parts) {
            System.out.println("[p" + p.k + "] " + p.file + " (último registo no ficheiro: " + p.last + ")");
            Thread t = new Thread(p::run, "sub-p" + p.k); // uma thread de subscrição por partição
            threads.add(t);
            t.start();
        }
        startVerification(parts);
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    // evento do detetor ou da receção: consola e monitor-replica<r>.txt, com o instante local à frente
    // (synchronized: as threads de subscrição e a de verificação escrevem no mesmo ficheiro)
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

    // estado de uma partição: alterado pela sua thread de subscrição e lido pela de verificação,
    // por isso os métodos são synchronized no próprio objeto; partições diferentes não partilham estado
    static class Partition {
        final int k;
        final String file;
        final RecordFile records;
        final IpfsPubSub ps;
        final HeartbeatMonitor monitor; // tem o seu próprio monitor (lock) interno
        long last; // último seq escrito no ficheiro
        final TreeMap<Long, SensorRecord> pending = new TreeMap<>(); // retidos, por seq
        String leaderHost; // aprendidos com cada LOTE recebido
        int leaderPort;
        boolean discardedOnce;

        // estatística desde o último resumo
        int viaPubSub, viaTcp, connections, discarded, batches;
        long lastReceiveAt, lastWriteAt;
        boolean active;

        Partition(String id, int k, String api) throws IOException {
            this.k = k;
            this.file = "replica-" + id + "-p" + k + ".txt";
            this.records = new RecordFile(file);
            this.ps = new IpfsPubSub(api);
            this.last = records.lastSeq();
            this.monitor = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, PartitionReplica::event);
        }

        void run() {
            try {
                ps.subscribe(Wire.topicData(k), this::onLote); // BLOQUEIA: não termina; cada mensagem chama onLote
                log("a subscrição terminou (o daemon IPFS fechou a ligação).");
            } catch (IOException | UncheckedIOException e) {
                log("IO: " + e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log("interrompida: " + e);
            }
        }

        synchronized void onLote(String msg) {
            Batch b;
            try {
                b = Batch.decode(msg, 0); // a réplica não conhece N: aceita a dimensão dos vetores que vier
            } catch (IllegalArgumentException e) { // inclui NumberFormatException
                log("REJEITADO: " + msg.replace('\n', '|'));
                return;
            }
            // 1) heartbeat: sinal de vida, mesmo que o lote venha vazio
            monitor.onHeartbeat(b.inc, b.hb);
            event("RECEBIDO;p" + k + ";inc=" + b.inc + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq + ";n=" + b.records.size()
                    + ";seq=" + (b.records.isEmpty() ? "-" : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq()));
            leaderHost = b.address; // o endereço do líder vem em CADA lote: segue-o se mudar
            leaderPort = b.port;
            batches++;
            try {
                // 2) registos do lote, pela ordem, com a regra da COMINDIR02
                for (SensorRecord r : b.records) {
                    onReg(r);
                }
                if (pending.size() > Wire.LIMITE_RETIDOS) {
                    recover(last + 1, pending.firstKey() - 1); // lacuna com demasiados retidos
                }
                // 3) falta o fim do lote anterior (ou deste): só o ultimoSeq o revela, mesmo num lote vazio
                if (last < b.lastSeq) {
                    recover(last + 1, b.lastSeq);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e); // falha a escrever no ficheiro local
            }
        }

        void onReg(SensorRecord r) throws IOException {
            long seq = r.getSeq();
            if (seq < 1) {
                log("REJEITADO: registo com seq " + seq);
                return;
            }
            lastReceiveAt = System.currentTimeMillis();
            active = true;
            if (PERDA > 0 && ThreadLocalRandom.current().nextDouble() < PERDA
                    || seq == DESCARTAR && !discardedOnce) {
                if (seq == DESCARTAR) discardedOnce = true;
                discarded++;
                log("DESCARTADO (simulação): " + seq);
                return;
            }
            apply(r, false);
        }

        // regra de aplicação: o ficheiro só cresce pela ordem do líder, sem saltos
        void apply(SensorRecord r, boolean viaTcp) throws IOException {
            long seq = r.getSeq();
            if (seq <= last || pending.containsKey(seq)) {
                if (!viaTcp) log("DUPLICADO: " + seq);
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
                log("Aplicado: " + r.toLine() + " (escrito " + now + ")");
            }
        }

        // pede ao líder, por TCP, os registos from..to (endereço e porto do último cabeçalho)
        void recover(long from, long to) {
            if (from > to || leaderHost == null) return;
            connections++;
            event("RECUPERACAO;p" + k + ";de=" + from + ";ate=" + to);
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
                    got++;
                    apply(r, true); // mesma regra: escreve e despeja os retidos que passaram a estar em sequência
                }
                log("RECUPERAÇÃO CONCLUÍDA: " + from + ".." + to + ", " + got + " registos recebidos (último no ficheiro: " + last
                        + ", retidos: " + pending.size() + ")");
            } catch (IOException | IllegalArgumentException e) {
                // os registos já escritos ficam; os retidos continuam em memória; tenta de novo no próximo lote
                log("RECUPERAÇÃO FALHOU: " + e + " (" + got + " registos recebidos antes da falha)");
            }
        }

        synchronized void report() {
            if (active && System.currentTimeMillis() - lastReceiveAt > IDLE_MS) {
                log("RESUMO: escritos " + (viaPubSub + viaTcp) + " (PubSub " + viaPubSub + ", TCP " + viaTcp + ")"
                        + " | lotes " + batches + " | ligações de recuperação " + connections + " | descartados " + discarded
                        + " | retidos " + pending.size() + " | último seq " + last + " | última escrita " + lastWriteAt);
                viaPubSub = viaTcp = connections = discarded = batches = 0;
                active = false;
            }
        }

        void log(String s) {
            System.out.println("[p" + k + "] " + s);
        }
    }

    // thread de verificação: a cada VERIFICACAO_MS, check() em todos os monitores (e o resumo, se for altura)
    static void startVerification(List<Partition> parts) {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(Wire.VERIFICACAO_MS);
                } catch (InterruptedException e) {
                    return;
                }
                for (Partition p : parts) {
                    p.monitor.check(); // não toma o lock da partição: uma recuperação TCP lenta não atrasa a suspeita
                }
                for (Partition p : parts) {
                    p.report();
                }
            }
        }, "verificacao");
        t.setDaemon(true);
        t.start();
    }
}
