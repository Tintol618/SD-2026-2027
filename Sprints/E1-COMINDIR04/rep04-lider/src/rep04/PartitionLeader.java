package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionLeader {
    // falhas introduzidas deliberadamente no ponto 9 (0 = código correto):
    //  1 só publica o LOTE quando pendentes não está vazia
    //  2 incrementa L e V[k] em cada heartbeat
    //  3 lê ultimoSeq fora da secção sincronizada em que troca pendentes
    //  4 envia sempre ultimoSeq = 0 e descarta um em cada dez lotes antes de publicar
    static final int ERRO = Integer.getInteger("rep04.erro", 0);

    static int k, n;
    static Partitioner partitioner;
    static String host;
    static int porto;
    static IpfsPubSub ps;
    static RecordFile file;
    static Path hbFile;

    // estado partilhado entre a entrada (escreve), o envio de lotes e a recuperação (leem):
    // o último seq atribuído, os relógios do líder e os registos à espera do próximo lote; tudo protegido por lock
    static final Object lock = new Object();
    static long seq;
    static long lamport;
    static VectorClock vector;
    static List<SensorRecord> pendentes = new ArrayList<>();
    static long hb;          // número do último heartbeat (protegido por lock)
    static long inc;         // incarnação: instante de arranque (só escrito no arranque)

    public static void main(String[] args) {
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
        hbFile = Paths.get("hb-p" + k + ".txt");
        porto = Wire.PORTO_BASE + k;
        try {
            // continua a numeração e os relógios a partir do último registo do ficheiro
            SensorRecord last = file.last();
            if (last == null) {
                seq = 0;
                lamport = 0;
                vector = new VectorClock(n);
            } else {
                seq = last.getSeq();
                lamport = last.getLamport();
                vector = VectorClock.parse(last.getVector().toString(), n); // confirma a dimensão N
                if (vector.get(k) != seq) {
                    throw new IllegalArgumentException("último registo com V[" + k + "] = " + vector.get(k) + " e seq = " + seq);
                }
            }
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Não foi possível ler lider-p" + k + ".txt: " + e);
            return;
        }
        inc = System.currentTimeMillis();
        hb = 0;
        System.out.println("Líder da partição " + k + " (N = " + n + ") | entrada " + Wire.topicIn(k)
                + " | dados " + Wire.topicData(k) + " | anuncia " + host + ":" + porto
                + " | último seq " + seq + " | L = " + lamport + " | V = " + vector
                + " | inc " + inc + " | Δ = " + Wire.PERIODO_HB_MS + " ms"
                + (ERRO != 0 ? " | FALHA INTRODUZIDA " + ERRO : ""));

        Thread entrada = new Thread(PartitionLeader::entrada, "entrada-p" + k);
        entrada.start();

        // heartbeat + lote: no arranque e depois a cada Δ (ritmo fixo, medido desde o arranque)
        ScheduledExecutorService agenda = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lote-p" + k);
            t.setDaemon(true);
            return t;
        });
        agenda.scheduleAtFixedRate(PartitionLeader::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

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
            // a entrada e os lotes continuam: o líder regista e publica, mas ninguém consegue recuperar
            System.out.println("Servidor de recuperação indisponível no porto " + porto + ": " + e);
        }
    }

    // atividade de recuperação: responde a "PEDIDO <de> <até>" com os registos do ficheiro e "FIM" (inalterada)
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
        long lMsg;
        VectorClock vMsg;
        try {
            if (!Wire.type(msg).equals("IN")) throw new IllegalArgumentException();
            String[] f = Wire.body(msg).split(";");
            if (f.length != 4 || f[0].isBlank()) throw new IllegalArgumentException();
            sensor = f[0].trim();
            temp = Double.parseDouble(f[1]);
            lMsg = Long.parseLong(f[2]);
            vMsg = VectorClock.parse(f[3], n); // um vetor com dimensão diferente de N é rejeitado
            if (lMsg < 0) throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) { // inclui NumberFormatException
            System.out.println("REJEITADO: " + msg + (e.getMessage() != null ? " (" + e.getMessage() + ")" : ""));
            return;
        }
        int j = partitioner.of(sensor);
        if (j != k) {
            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
            return;
        }
        try {
            // relógios, atribuição do seq, verificação, append e entrada em pendentes na MESMA secção sincronizada
            synchronized (lock) {
                long newL = Math.max(lamport, lMsg) + 1; // regra de Lamport
                VectorClock newV = vector.copy();         // regra vetorial: máximo e depois o evento local
                newV.merge(vMsg);
                newV.tick(k);
                long newSeq = seq + 1;
                if (newV.get(k) != newSeq) {
                    System.out.println("RECUSADO: V[" + k + "] = " + newV.get(k) + " diferente de seq = " + newSeq
                            + " (" + msg + ")");
                    return;
                }
                SensorRecord r = SensorRecord.now(newSeq, sensor, temp, newL, newV);
                file.append(r);   // 1) escreve localmente
                // o estado só avança depois de o registo estar no ficheiro
                seq = newSeq;
                lamport = newL;
                vector = newV;
                pendentes.add(r); // 2) fica à espera do próximo lote (já não se publica um REG por registo)
                if (pendentes.size() == 1) { // durante uma rajada não se mostra cada registo
                    System.out.println("EVENTO: seq=" + seq + " L=" + lamport + " V=" + vector + " (pendente)");
                }
            }
        } catch (IOException e) {
            System.out.println("ESCRITA FALHOU (" + msg + "): " + e);
        }
    }

    // executada no arranque e a cada PERIODO_HB_MS pela agenda: fecha o lote e publica-o (é também o heartbeat)
    static void enviarLote() {
        try {
            List<SensorRecord> lote;
            long ultimoSeq, nHb;
            synchronized (lock) { // o MESMO monitor de onIn: nenhum registo fica entre a troca e a leitura de seq
                if (ERRO == 1 && pendentes.isEmpty()) {
                    return;       // FALHA 1: sem registos não há heartbeat
                }
                lote = pendentes;
                pendentes = new ArrayList<>();
                ultimoSeq = seq;
                nHb = ++hb;
                if (ERRO == 2) {  // FALHA 2: o heartbeat tratado como evento
                    lamport++;
                    vector.tick(k);
                }
            }
            if (ERRO == 3) {      // FALHA 3: ultimoSeq lido depois de largar o lock
                Thread.sleep(20); // alarga a janela para a corrida se observar numa rajada
                ultimoSeq = seq;
            }
            if (ERRO == 4) {
                ultimoSeq = 0;
            }
            // fora da secção sincronizada: a publicação bloqueia à espera do daemon e não deve parar a entrada
            String msg = new Batch(host, porto, inc, nHb, ultimoSeq, lote).encode();
            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            if (ERRO == 4 && nHb % 10 == 0) {
                System.out.println("DESCARTADO (falha 4): hb=" + nHb + " n=" + lote.size());
                return;
            }
            long instante = System.currentTimeMillis();
            try {
                ps.publish(Wire.topicData(k), msg);
            } catch (IOException e) {
                // não se volta a publicar: os participantes recuperam pelo ultimoSeq do lote seguinte
                System.out.println("PUBLICAÇÃO FALHOU (hb " + nHb + ", " + lote.size() + " registos): " + e.getMessage());
                return;
            }
            Files.writeString(hbFile, inc + ";" + nHb + ";" + instante + ";" + ultimoSeq + ";" + lote.size() + ";" + bytes
                            + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("HEARTBEAT: hb=" + nHb + " n=" + lote.size() + " ultimoSeq=" + ultimoSeq
                    + (lote.isEmpty() ? "" : " seq " + lote.get(0).getSeq() + ".." + lote.get(lote.size() - 1).getSeq())
                    + " (" + bytes + " bytes)");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            // uma exceção não apanhada cancelaria silenciosamente todos os heartbeats seguintes
            System.out.println("LOTE FALHOU: " + e);
        }
    }
}
