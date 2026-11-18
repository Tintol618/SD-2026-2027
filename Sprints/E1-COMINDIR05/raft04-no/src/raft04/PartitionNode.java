package raft04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

// Nó de um grupo de três que serve a partição k: seguidor, candidato ou líder (eleição ao estilo RAFT).
// Junta o PartitionLeader e a Replica da ficha anterior.
public class PartitionNode {
    // falhas introduzidas deliberadamente no ponto 10 (as falhas 1, 2 e 6 estão em RaftState):
    //  3 voto concedido sem a verificação upToDate
    //  4 seguidor que aplica lotes de termo inferior ao seu
    //  5 novo líder que inicializa L e V a zero
    static final int ERRO = Integer.getInteger("raft04.erro", 0);

    final int k, i, n;
    final String host;
    final int port;
    final IpfsPubSub ps;
    final Partitioner partitioner;
    final Path logFile, hbFile, eventFile, truncFile;
    final RaftState raft;

    // monitor do nó; quando são precisos os dois, a ordem é sempre: lock do nó → monitor do RaftState
    final Object lock = new Object();
    final Object evLock = new Object();    // só para escrever eventos (nunca adquire outro lock)

    // registos do nó, espelho de no-p<k>-n<i>.txt (índice = seq - 1)
    final List<SensorRecord> log = new ArrayList<>();
    // seguidor: retidos por lacuna e endereço de recuperação do líder (aprendido em cada LOTE)
    final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
    String leaderHost;
    int leaderPort;
    // líder
    long lamport;
    VectorClock vector;
    List<SensorRecord> pendentes = new ArrayList<>();
    long hb;
    long leaderTerm = -1;
    ScheduledFuture<?> agendamento;
    final ScheduledExecutorService agenda;

    public static void main(String[] args) {
        int k, i, n;
        try {
            k = Integer.parseInt(args[0]);
            i = Integer.parseInt(args[1]);
            n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
            if (n < 1 || k < 0 || k >= n || i < 1 || i > Wire.GRUPO) throw new IllegalArgumentException();
        } catch (RuntimeException e) {
            System.out.println("Uso: PartitionNode <k> <i> [N], com 0 <= k < N e 1 <= i <= " + Wire.GRUPO);
            return;
        }
        String api = System.getProperty("raft04.api", "http://127.0.0.1:5001/api/v0");
        // arranque sincronizado (ensaio 9(b) com prazo fixo): espera por um instante comum antes de iniciar o prazo
        long inicio = Long.getLong("raft04.inicio", 0);
        try {
            if (inicio > System.currentTimeMillis()) Thread.sleep(inicio - System.currentTimeMillis());
        } catch (InterruptedException e) {
            return;
        }
        PartitionNode node;
        try {
            node = new PartitionNode(k, i, n, "127.0.0.1", api);
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("Não foi possível arrancar o nó: " + e);
            return;
        }
        node.start();
    }

    PartitionNode(int k, int i, int n, String host, String api) throws IOException {
        this.k = k;
        this.i = i;
        this.n = n;
        this.host = host;
        this.port = Wire.nodePort(k, i);
        this.ps = new IpfsPubSub(api);
        this.partitioner = new Partitioner(n);
        String id = "p" + k + "-n" + i;
        this.logFile = Paths.get("no-" + id + ".txt");
        this.hbFile = Paths.get("hb-" + id + ".txt");
        this.eventFile = Paths.get("eventos-" + id + ".txt");
        this.truncFile = Paths.get("truncados-" + id + ".txt");
        if (Files.exists(logFile)) {
            for (String line : Files.readAllLines(logFile, StandardCharsets.UTF_8)) {
                if (line.isBlank()) continue;
                SensorRecord r = SensorRecord.fromLine(line, n);
                if (r.getSeq() != log.size() + 1) {
                    throw new IllegalArgumentException(logFile + ": seq " + r.getSeq() + " fora de ordem");
                }
                log.add(r);
            }
        }
        this.raft = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                Paths.get("estado-" + id + ".txt"), this::event);
        this.agenda = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lote-" + id);
            t.setDaemon(true);
            return t;
        });
    }

    void start() {
        event("ARRANQUE;p" + k + ";n=" + i + ";termo=" + raft.term() + ";ultimoSeq=" + lastSeq() + ";ultimoTermo=" + lastTerm());
        System.out.println("Nó n" + i + " da partição p" + k + " | porto " + port + " | prazo de eleição ["
                + Wire.ELEICAO_MIN_MS + ", " + Wire.ELEICAO_MAX_MS + "] ms | Δ = " + Wire.PERIODO_HB_MS + " ms"
                + (ERRO != 0 ? " | FALHA INTRODUZIDA " + ERRO : ""));
        thread("recuperacao", this::serveLoop);
        thread("sub-in", () -> subscribeLoop(Wire.topicIn(k), this::onIn));
        thread("sub-dados", () -> subscribeLoop(Wire.topicData(k), this::onData));
        thread("sub-eleicao", () -> subscribeLoop(Wire.topicElection(k), this::onElection));
        verificationLoop();
    }

    static void thread(String name, Runnable r) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        t.start();
    }

    // ------------------------------------------------------------ eventos e registos locais

    void event(String e) {
        String line = System.currentTimeMillis() + ";" + e;
        synchronized (evLock) {
            System.out.println(line);
            try {
                Files.writeString(eventFile, line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException ex) {
                System.out.println("Não foi possível escrever em " + eventFile + ": " + ex);
            }
        }
    }

    long lastSeq()  { return log.size(); }
    long lastTerm() { return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm(); }

    // acrescenta ao ficheiro e à memória (lock do nó adquirido)
    void appendRecord(SensorRecord r) {
        try {
            Files.writeString(logFile, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        log.add(r);
    }

    // reescreve o ficheiro de forma atómica com o conteúdo de log (lock do nó adquirido)
    void rewriteLog() {
        try {
            Path tmp = logFile.resolveSibling(logFile.getFileName() + ".tmp");
            StringBuilder sb = new StringBuilder();
            for (SensorRecord r : log) sb.append(r.toLine()).append(System.lineSeparator());
            Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
            Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------ publicação e subscrição

    void publish(String topic, String msg) {
        if (Wire.isolated(k, i)) return;   // partição de rede simulada: nada sai do nó
        try {
            ps.publish(topic, msg);
        } catch (IOException e) {
            System.out.println("PUBLICAÇÃO FALHOU (" + msg.split("\n")[0] + "): " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void subscribeLoop(String topic, java.util.function.Consumer<String> handler) {
        while (true) {
            try {
                ps.subscribe(topic, msg -> {
                    try {
                        handler.accept(msg);
                    } catch (RuntimeException e) {
                        System.out.println("ERRO a tratar mensagem de " + topic + ": " + e);
                    }
                });
                System.out.println("Subscrição de " + topic + " terminou; a voltar a subscrever.");
            } catch (IOException | UncheckedIOException e) {
                System.out.println("Subscrição de " + topic + " falhou: " + e + "; nova tentativa em 1 s.");
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

    // ------------------------------------------------------------ eleição

    // thread de verificação: a cada VERIFICACAO_MS, tick(); se houver candidatura, publica PEDIDO_VOTO
    void verificationLoop() {
        while (true) {
            try {
                Thread.sleep(Wire.VERIFICACAO_MS);
            } catch (InterruptedException e) {
                return;
            }
            long t;
            long ls = 0, lt = 0;
            boolean alone = false;
            synchronized (lock) {
                t = raft.tick();
                if (t >= 0) {
                    ls = lastSeq();
                    lt = lastTerm();
                    if (raft.faultyMajorityCheck()) {   // só com a falha 6
                        becomeLeader();
                        alone = true;
                    }
                }
            }
            if (t >= 0 && !alone) {
                publish(Wire.topicElection(k), new VoteRequest(t, i, ls, lt).encode());
            }
        }
    }

    void onElection(String msg) {
        if (Wire.isolated(k, i)) return;
        String type = Wire.type(msg);
        if (type.equals("PEDIDO_VOTO")) {
            VoteRequest vr = VoteRequest.decode(msg);
            if (vr.candidate == i) return;                 // a própria mensagem
            RaftState.Vote v;
            synchronized (lock) {
                long myLs = ERRO == 3 ? 0 : lastSeq();     // FALHA 3: sem a restrição de voto
                long myLt = ERRO == 3 ? 0 : lastTerm();
                v = raft.onVoteRequest(vr.term, vr.candidate, vr.lastSeq, vr.lastTerm, myLs, myLt);
                afterRaft();
            }
            // responde sempre, mesmo a recusar: o candidato fica a saber o termo deste nó
            publish(Wire.topicElection(k), new VoteReply(v.term, i, vr.candidate, v.granted).encode());
        } else if (type.equals("VOTO")) {
            VoteReply v = VoteReply.decode(msg);
            if (v.voter == i || v.candidate != i) return;  // só os votos dirigidos a este nó
            synchronized (lock) {
                boolean leader = raft.onVote(v.term, v.voter, v.candidate, v.granted);
                afterRaft();
                if (leader) becomeLeader();
            }
        }
    }

    // depois de qualquer chamada ao RaftState: se o nó deixou de ser líder, para os lotes (lock do nó adquirido)
    void afterRaft() {
        if (agendamento != null && raft.termIfLeader() != leaderTerm) {
            stopLeader();
        }
    }

    void stopLeader() {
        if (agendamento != null) agendamento.cancel(false);
        agendamento = null;
        leaderTerm = -1;
        pendentes = new ArrayList<>();   // já estão no ficheiro; se divergirem, o novo líder trunca-os
    }

    // passagem a líder, com o lock do nó adquirido: seq, L e V a partir do último registo do ficheiro
    void becomeLeader() {
        if (agendamento != null) agendamento.cancel(false);
        leaderTerm = raft.termIfLeader();
        retidos.clear();
        SensorRecord last = log.isEmpty() ? null : log.get(log.size() - 1);
        if (last == null || ERRO == 5) {   // FALHA 5: L e V a zero
            lamport = 0;
            vector = new VectorClock(n);
        } else {
            lamport = last.getLamport();
            vector = last.getVector();
        }
        hb = 0;
        pendentes = new ArrayList<>();
        System.out.println("LÍDER no termo " + leaderTerm + ": seq a partir de " + lastSeq() + ", L = " + lamport + ", V = " + vector);
        agendamento = agenda.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------ líder

    void onIn(String msg) {
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
            vMsg = VectorClock.parse(f[3], n);
            if (lMsg < 0) throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + msg);
            return;
        }
        if (partitioner.of(sensor) != k) return;
        synchronized (lock) {
            long t = raft.termIfLeader();                 // no mesmo lock da demissão: não há seq com termo antigo
            if (t == -1) {
                event("IGNORADO;p" + k + ";in=" + sensor + " " + temp + ";papel=" + raft.role() + ";termo=" + raft.term());
                return;
            }
            long newL = Math.max(lamport, lMsg) + 1;
            VectorClock newV = vector.copy();
            newV.merge(vMsg);
            newV.tick(k);
            long newSeq = lastSeq() + 1;
            if (newV.get(k) != newSeq) {
                event("ERRO;p" + k + ";recusado V[" + k + "]=" + newV.get(k) + " diferente de seq=" + newSeq + ";in=" + sensor + " " + temp);
                return;
            }
            SensorRecord r = SensorRecord.now(newSeq, t, sensor, temp, newL, newV);
            appendRecord(r);
            lamport = newL;
            vector = newV;
            pendentes.add(r);
        }
    }

    // a cada PERIODO_HB_MS enquanto o nó for líder: fecha o lote (é também o heartbeat) e publica-o
    void enviarLote() {
        try {
            List<SensorRecord> lote;
            long t, ultimoSeq, ultimoTermo, nHb;
            synchronized (lock) {
                t = raft.termIfLeader();
                if (t == -1 || t != leaderTerm) {           // demitido entretanto: cancela o agendamento
                    if (t == -1) stopLeader();
                    return;
                }
                lote = pendentes;
                pendentes = new ArrayList<>();
                ultimoSeq = lastSeq();
                ultimoTermo = lastTerm();
                nHb = ++hb;
            }
            // fora da secção sincronizada: a publicação bloqueia à espera do daemon
            String msg = new Batch(t, i, host, port, nHb, ultimoSeq, ultimoTermo, lote).encode();
            if (Wire.isolated(k, i)) {
                System.out.println("ISOLADO: lote termo=" + t + " hb=" + nHb + " n=" + lote.size() + " não publicado");
                return;
            }
            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            long instante = System.currentTimeMillis();
            try {
                ps.publish(Wire.topicData(k), msg);
            } catch (IOException e) {
                System.out.println("PUBLICAÇÃO FALHOU (termo " + t + " hb " + nHb + "): " + e.getMessage());
                return;
            }
            Files.writeString(hbFile, t + ";" + nHb + ";" + instante + ";" + ultimoSeq + ";" + ultimoTermo + ";"
                            + lote.size() + ";" + bytes + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            if (!lote.isEmpty() || nHb % 5 == 1) {
                System.out.println("HEARTBEAT: termo=" + t + " hb=" + nHb + " n=" + lote.size() + " ultimoSeq=" + ultimoSeq);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException e) {
            System.out.println("LOTE FALHOU: " + e);
        }
    }

    // ------------------------------------------------------------ seguidor

    void onData(String msg) {
        if (Wire.isolated(k, i)) return;
        Batch b;
        try {
            b = Batch.decode(msg, n);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + msg.split("\n")[0]);
            return;
        }
        if (b.leader == i) return;                         // o próprio lote
        synchronized (lock) {
            boolean ok = raft.onBatch(b.term, b.leader);
            afterRaft();
            if (!ok) {
                if (ERRO == 4 && b.term < raft.term()) {
                    // FALHA 4: aplica um lote de termo inferior
                } else {
                    event("IGNORADO;p" + k + ";lote termo=" + b.term + ";lider=n" + b.leader + ";hb=" + b.hb + ";meu termo=" + raft.term());
                    return;
                }
            }
            event("RECEBIDO;p" + k + ";termo=" + b.term + ";lider=n" + b.leader + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq
                    + ";ultimoTermo=" + b.lastTerm + ";n=" + b.records.size() + ";seq="
                    + (b.records.isEmpty() ? "-" : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq()));
            leaderHost = b.address;
            leaderPort = b.port;
            applyBatch(b);
        }
    }

    // regra de aplicação, com o lock do nó adquirido
    void applyBatch(Batch b) {
        boolean divergent = false;
        for (SensorRecord r : b.records) {
            long s = r.getSeq();
            if (s <= lastSeq()) {
                if (log.get((int) s - 1).sameEntry(r)) continue;   // duplicado
                divergent = true;                                // mesmo seq, outro termo
                break;
            }
            if (s == lastSeq() + 1) {
                appendRecord(r);
                drainRetidos();
            } else {
                retidos.put(s, r);                               // lacuna
            }
        }
        if (!divergent && retidos.size() > Wire.LIMITE_RETIDOS) {
            divergent = !catchUp(retidos.firstKey() - 1);
        }
        if (!divergent) {
            long ls = lastSeq();
            if (ls > b.lastSeq
                    || (b.lastSeq > 0 && ls >= b.lastSeq && log.get((int) b.lastSeq - 1).getTerm() != b.lastTerm)) {
                divergent = true;
            } else if (ls < b.lastSeq) {
                divergent = !catchUp(b.lastSeq);
            }
        }
        if (divergent) resolveDivergence();
    }

    void drainRetidos() {
        while (retidos.containsKey(lastSeq() + 1)) {
            appendRecord(retidos.remove(lastSeq() + 1));
        }
        retidos.headMap(lastSeq() + 1).clear();
    }

    // recupera até target, pedindo também o último registo local para confirmar que coincide com o do líder.
    // Devolve false se encontrar uma divergência.
    boolean catchUp(long target) {
        long from = Math.max(1, lastSeq());
        event("RECUPERACAO;p" + k + ";de=" + (lastSeq() + 1) + ";ate=" + target);
        List<SensorRecord> got;
        try {
            got = fetch(from, target);
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("RECUPERAÇÃO FALHOU: " + e);
            return true;   // tenta outra vez no próximo lote
        }
        for (SensorRecord r : got) {
            long s = r.getSeq();
            if (s <= lastSeq()) {
                if (!log.get((int) s - 1).sameEntry(r)) return false;
            } else if (s == lastSeq() + 1) {
                appendRecord(r);
                drainRetidos();
            }
        }
        return true;
    }

    // primeiro seq do bloco de termo que contém s
    long blockStart(long s) {
        if (s < 1) return 1;
        long term = log.get((int) s - 1).getTerm();
        long b = s;
        while (b > 1 && log.get((int) b - 2).getTerm() == term) b--;
        return b;
    }

    // divergência: recua bloco de termo a bloco de termo até coincidir com o líder, trunca e aplica os do líder
    void resolveDivergence() {
        long start = blockStart(lastSeq());
        List<SensorRecord> got;
        try {
            while (true) {
                got = fetch(start, Long.MAX_VALUE);
                if (start > lastSeq() || start == 1) break;
                if (!got.isEmpty() && got.get(0).getSeq() == start && log.get((int) start - 1).sameEntry(got.get(0))) break;
                start = blockStart(start - 1);
            }
        } catch (IOException | IllegalArgumentException e) {
            System.out.println("RESOLUÇÃO DE DIVERGÊNCIA FALHOU: " + e);
            return;
        }
        Map<Long, SensorRecord> theirs = new HashMap<>();
        for (SensorRecord r : got) theirs.put(r.getSeq(), r);
        long diff = lastSeq() + 1;
        for (long s = start; s <= lastSeq(); s++) {
            if (!log.get((int) s - 1).sameEntry(theirs.get(s))) {
                diff = s;
                break;
            }
        }
        if (diff <= lastSeq()) {
            List<SensorRecord> dropped = new ArrayList<>(log.subList((int) diff - 1, log.size()));
            TreeSet<Long> terms = new TreeSet<>();
            StringBuilder sb = new StringBuilder();
            long now = System.currentTimeMillis();
            for (SensorRecord r : dropped) {
                terms.add(r.getTerm());
                sb.append(now).append(';').append(r.toLine()).append(System.lineSeparator());
            }
            log.subList((int) diff - 1, log.size()).clear();
            rewriteLog();
            try {
                Files.writeString(truncFile, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("Não foi possível escrever " + truncFile + ": " + e);
            }
            event("TRUNCADO;p" + k + ";de=" + diff + ";descartados=" + dropped.size() + ";termos=" + terms);
        }
        retidos.clear();
        List<Long> seqs = new ArrayList<>(theirs.keySet());
        Collections.sort(seqs);
        for (long s : seqs) {
            if (s == lastSeq() + 1) appendRecord(theirs.get(s));
        }
    }

    // pede ao líder, por TCP, os registos from..to (o líder serve até ao seu último)
    List<SensorRecord> fetch(long from, long to) throws IOException {
        if (Wire.isolated(k, i)) throw new IOException("nó isolado");
        if (leaderHost == null) throw new IOException("líder desconhecido");
        List<SensorRecord> out = new ArrayList<>();
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(leaderHost, leaderPort), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
            pw.println("PEDIDO " + from + " " + to);
            while (true) {
                String line = in.readLine();
                if (line == null) throw new EOFException("ligação fechada antes de FIM");
                if (line.equals("FIM")) break;
                if (line.startsWith("ERRO")) throw new IOException("o nó respondeu: " + line);
                out.add(SensorRecord.fromLine(line, n));
            }
        }
        return out;
    }

    // ------------------------------------------------------------ servidor de recuperação (qualquer nó)

    void serveLoop() {
        try (ServerSocket server = new ServerSocket(port)) {
            while (true) {
                try (Socket s = server.accept()) {
                    if (Wire.isolated(k, i)) continue;     // partição de rede: a ligação falha do lado do cliente
                    s.setSoTimeout(Wire.T_TCP_MS);
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                    PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
                    serve(in.readLine(), out);
                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Servidor de recuperação indisponível no porto " + port + ": " + e);
        }
    }

    void serve(String pedido, PrintWriter out) {
        String[] p = pedido == null ? new String[0] : pedido.trim().split("\\s+");
        long from, to;
        try {
            if (p.length != 3 || !p[0].equals("PEDIDO")) throw new IllegalArgumentException();
            from = Long.parseLong(p[1]);
            to = Long.parseLong(p[2]);
            if (from < 1 || from > to) throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
            out.println("ERRO pedido inválido: " + pedido);
            return;
        }
        List<SensorRecord> rs;
        synchronized (lock) {
            long upTo = Math.min(to, lastSeq());
            rs = upTo >= from ? new ArrayList<>(log.subList((int) from - 1, (int) upTo)) : List.of();
        }
        for (SensorRecord r : rs) out.println(r.toLine());
        out.println("FIM");
    }
}
