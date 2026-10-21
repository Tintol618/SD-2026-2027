package rep02;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {
    static final long IDLE_MS = 2000; // sem registos durante este tempo → o lote terminou

    static int k;
    static Partitioner partitioner;
    static String host;
    static int porto;
    static IpfsPubSub ps;
    static RecordFile file;

    // estado partilhado entre a entrada (escreve) e a recuperação (lê): o último seq atribuído
    static final Object lock = new Object();
    static long seq;

    // estatística do lote atual (para medir o débito do líder); também protegida por lock
    static int batchCount;
    static long batchFirstSeq, batchFirstAt, batchLastAt;

    public static void main(String[] args) {
        int n;
        try {
            k = Integer.parseInt(args[0]);
            n = Integer.parseInt(args[1]);
            partitioner = new Partitioner(n);
            if (k < 0 || k >= n) throw new IllegalArgumentException();
        } catch (RuntimeException e) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereço] [api], com 0 <= k < N");
            return;
        }
        host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
        ps = new IpfsPubSub(api);
        file = new RecordFile("lider-p" + k + ".txt");
        porto = Wire.PORTO_BASE + k;
        try {
            seq = file.lastSeq(); // continua a numeração após reinício
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Não foi possível ler lider-p" + k + ".txt: " + e);
            return;
        }
        System.out.println("Líder da partição " + k + " (N = " + n + ") | entrada " + Wire.topicIn(k)
                + " | dados " + Wire.topicData(k) + " | anuncia " + host + ":" + porto + " | último seq " + seq);

        Thread entrada = new Thread(PartitionLeader::entrada, "entrada-p" + k);
        entrada.start();
        startBatchMonitor();

        try (ServerSocket server = new ServerSocket(porto)) {
            System.out.println("Recuperação à escuta no porto " + porto);
            while (true) {
                try (Socket s = server.accept();                       // BLOQUEIA: até uma réplica se ligar
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(
                             new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();                     // BLOQUEIA: até chegar o pedido (no máximo T_TCP_MS)
                    serve(pedido, out, s.getRemoteSocketAddress());
                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            // a thread de entrada continua: o líder regista e publica, mas ninguém consegue recuperar
            System.out.println("Servidor de recuperação indisponível no porto " + porto + ": " + e);
        }
    }

    // atividade de recuperação: responde a "PEDIDO <de> <até>" com os registos do ficheiro e "FIM"
    static void serve(String pedido, PrintWriter out, SocketAddress remote) throws IOException {
        String[] p = pedido == null ? new String[0] : pedido.trim().split("\\s+");
        long from, to;
        try {
            if (p.length != 3 || !p[0].equals("PEDIDO")) throw new IllegalArgumentException();
            from = Long.parseLong(p[1]);
            to = Long.parseLong(p[2]);
        } catch (IllegalArgumentException e) {
            out.println("ERRO pedido mal formado: " + pedido);
            System.out.println("RECUPERAÇÃO RECUSADA para " + remote + ": pedido mal formado: " + pedido);
            return;
        }
        if (from < 1 || from > to) {
            out.println("ERRO intervalo inválido: " + from + ".." + to);
            System.out.println("RECUPERAÇÃO RECUSADA para " + remote + ": intervalo inválido " + from + ".." + to);
            return;
        }
        long last;
        synchronized (lock) {
            last = seq; // tudo até aqui já está no ficheiro (o seq só avança depois do append)
        }
        long upTo = Math.min(to, last);
        List<SensorRecord> rs = upTo >= from ? file.range(from, upTo) : List.of();
        for (SensorRecord r : rs) {
            out.println(r.toLine());
        }
        out.println("FIM");
        if (out.checkError()) { // o PrintWriter não lança exceções: é preciso perguntar
            throw new IOException("a réplica fechou a ligação antes do FIM");
        }
        System.out.println("RECUPERAÇÃO: " + from + ".." + upTo + " para " + remote + " (" + rs.size() + " registos)");
    }

    // atividade de entrada: subscreve o tópico de entrada da partição
    static void entrada() {
        try {
            ps.subscribe(Wire.topicIn(k), PartitionLeader::onIn); // BLOQUEIA: não termina; cada mensagem chama onIn
            System.out.println("A subscrição de entrada terminou (o daemon IPFS fechou a ligação).");
        } catch (IOException | UncheckedIOException e) {
            System.out.println("Entrada terminou: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Entrada interrompida: " + e);
        }
    }

    static void onIn(String msg) {
        String sensor;
        double temp;
        try {
            if (!Wire.type(msg).equals("IN")) throw new IllegalArgumentException();
            String[] f = Wire.body(msg).split(";");
            if (f.length != 2 || f[0].isBlank()) throw new IllegalArgumentException();
            sensor = f[0].trim();
            temp = Double.parseDouble(f[1]);
        } catch (IllegalArgumentException e) { // inclui NumberFormatException
            System.out.println("REJEITADO: " + msg);
            return;
        }
        int j = partitioner.of(sensor);
        if (j != k) {
            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
            return;
        }
        SensorRecord r;
        boolean isolated;
        try {
            synchronized (lock) { // atribuição do seq e append na MESMA secção sincronizada
                r = SensorRecord.now(seq + 1, sensor, temp);
                file.append(r);   // 1) escreve localmente
                seq = r.getSeq(); // só avança depois de o registo estar no ficheiro
                long now = System.currentTimeMillis();
                isolated = batchCount == 0 || now - batchLastAt > 200;
                if (batchCount == 0) {
                    batchFirstSeq = seq;
                    batchFirstAt = now;
                }
                batchCount++;
                batchLastAt = now;
            }
        } catch (IOException e) {
            System.out.println("ESCRITA FALHOU (" + msg + "): " + e);
            return;
        }
        try {
            ps.publish(Wire.topicData(k), Wire.reg(host, porto, r)); // 2) propaga, com o endereço de recuperação
        } catch (IOException e) {
            // o registo está no ficheiro: as réplicas recuperam-no quando derem pela falta
            System.out.println("PUBLICAÇÃO FALHOU (seq " + r.getSeq() + "): " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (isolated) { // durante uma rajada não se mostra cada registo, para não atrasar o líder
            System.out.println("Registado e publicado: " + r.toLine());
        }
    }

    // quando deixam de chegar registos, mostra o resumo do lote (débito do líder)
    static void startBatchMonitor() {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    return;
                }
                synchronized (lock) {
                    if (batchCount > 1 && System.currentTimeMillis() - batchLastAt > IDLE_MS) {
                        long ms = batchLastAt - batchFirstAt;
                        System.out.printf("LOTE p%d: %d registos (seq %d..%d) | primeiro %d | último %d | %d ms | débito %.0f registos/s%n",
                                k, batchCount, batchFirstSeq, seq, batchFirstAt, batchLastAt, ms,
                                batchCount * 1000.0 / Math.max(ms, 1));
                        batchCount = 0;
                    } else if (batchCount == 1 && System.currentTimeMillis() - batchLastAt > IDLE_MS) {
                        batchCount = 0;
                    }
                }
            }
        }, "resumo-p" + k);
        t.setDaemon(true);
        t.start();
    }
}
