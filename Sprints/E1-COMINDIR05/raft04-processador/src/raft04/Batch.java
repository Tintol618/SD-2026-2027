package raft04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// LOTE;<termo>;<lider>;<endereço>;<porto>;<hb>;<ultimoSeq>;<ultimoTermo>;<n>
// <seq>;<termo>;<sensor>;<temperatura>;<instante>;<L>;<V>      (n linhas)
// Um lote vazio (n = 0) é um heartbeat puro.
public final class Batch {
    public final long term;
    public final int leader;
    public final String address;
    public final int port;
    public final long hb;
    public final long lastSeq;
    public final long lastTerm;
    public final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm,
                 List<SensorRecord> records) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(term).append(';').append(leader).append(';').append(address).append(';')
                .append(port).append(';').append(hb).append(';').append(lastSeq).append(';').append(lastTerm)
                .append(';').append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    // interpreta um LOTE completo; n é a dimensão dos vetores (n <= 0: aceita a que vier).
    // Valida que o número de linhas de registo coincide com o n do cabeçalho.
    public static Batch decode(String msg, int n) {
        String[] lines = lines(msg);
        Batch h = decodeHeader(lines[0]);
        int count = headerCount(lines[0]);
        if (lines.length - 1 != count) {
            throw new IllegalArgumentException("LOTE com " + (lines.length - 1) + " linhas de registo, cabeçalho indica " + count);
        }
        List<SensorRecord> rs = new ArrayList<>(count);
        for (int i = 1; i < lines.length; i++) {
            rs.add(n > 0 ? SensorRecord.fromLine(lines[i], n) : SensorRecord.fromLine(lines[i]));
        }
        return new Batch(h.term, h.leader, h.address, h.port, h.hb, h.lastSeq, h.lastTerm, rs);
    }

    // só o cabeçalho (usado pelo produtor): records fica vazia
    public static Batch decodeHeader(String msg) {
        String[] f = lines(msg)[0].split(";");
        if (f.length != 9 || !f[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho de LOTE mal formado: " + msg);
        }
        long term = Long.parseLong(f[1]);
        int leader = Integer.parseInt(f[2]);
        int port = Integer.parseInt(f[4]);
        long hb = Long.parseLong(f[5]);
        long last = Long.parseLong(f[6]);
        long lastTerm = Long.parseLong(f[7]);
        int count = Integer.parseInt(f[8]);
        if (term < 1 || leader < 1 || f[3].isBlank() || port < 1 || port > 65535 || hb < 1 || last < 0 || lastTerm < 0 || count < 0) {
            throw new IllegalArgumentException("Cabeçalho de LOTE inválido: " + msg);
        }
        return new Batch(term, leader, f[3], port, hb, last, lastTerm, List.of());
    }

    public static int headerCount(String msg) {
        return Integer.parseInt(lines(msg)[0].split(";")[8]);
    }

    private static String[] lines(String msg) {
        List<String> out = new ArrayList<>();
        for (String l : msg.trim().split("\\r?\\n")) {
            if (!l.isBlank()) out.add(l.trim());
        }
        if (out.isEmpty()) throw new IllegalArgumentException("LOTE vazio");
        return out.toArray(new String[0]);
    }
}
