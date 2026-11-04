package rep04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// LOTE;<endereço>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>
// <seq>;<sensor>;<temperatura>;<instante>;<L>;<V>      (n linhas)
// Um lote vazio (n = 0) é um heartbeat puro.
public final class Batch {
    public final String address;
    public final int port;
    public final long inc;
    public final long hb;
    public final long lastSeq;
    public final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(address).append(';').append(port).append(';').append(inc).append(';')
                .append(hb).append(';').append(lastSeq).append(';').append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    // interpreta um LOTE completo; n é a dimensão dos vetores (n <= 0: aceita a dimensão que vier,
    // como faz a réplica, que não conhece N). Valida que o número de linhas de registo coincide
    // com o n do cabeçalho. Lança IllegalArgumentException se a mensagem estiver mal formada.
    public static Batch decode(String msg, int n) {
        String[] lines = lines(msg);
        Batch h = parseHeader(lines[0]);
        int count = Integer.parseInt(lines[0].split(";")[6]);
        if (lines.length - 1 != count) {
            throw new IllegalArgumentException("LOTE com " + (lines.length - 1) + " linhas de registo, cabeçalho indica " + count);
        }
        List<SensorRecord> rs = new ArrayList<>(count);
        for (int i = 1; i < lines.length; i++) {
            rs.add(n > 0 ? SensorRecord.fromLine(lines[i], n) : SensorRecord.fromLine(lines[i]));
        }
        return new Batch(h.address, h.port, h.inc, h.hb, h.lastSeq, rs);
    }

    // interpreta só o cabeçalho (usado pelo produtor): records fica vazia; n obtém-se com headerCount
    public static Batch decodeHeader(String msg) {
        return parseHeader(lines(msg)[0]);
    }

    // n indicado no cabeçalho (sem interpretar as linhas de registo)
    public static int headerCount(String msg) {
        return Integer.parseInt(lines(msg)[0].split(";")[6]);
    }

    private static String[] lines(String msg) {
        String[] raw = msg.trim().split("\\r?\\n");
        List<String> out = new ArrayList<>();
        for (String l : raw) {
            if (!l.isBlank()) out.add(l.trim());
        }
        if (out.isEmpty()) throw new IllegalArgumentException("LOTE vazio");
        return out.toArray(new String[0]);
    }

    private static Batch parseHeader(String header) {
        String[] f = header.split(";");
        if (f.length != 7 || !f[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho de LOTE mal formado: " + header);
        }
        int port = Integer.parseInt(f[2]);
        long inc = Long.parseLong(f[3]);
        long hb = Long.parseLong(f[4]);
        long last = Long.parseLong(f[5]);
        int count = Integer.parseInt(f[6]);
        if (f[1].isBlank() || port < 1 || port > 65535 || hb < 1 || last < 0 || count < 0) {
            throw new IllegalArgumentException("Cabeçalho de LOTE inválido: " + header);
        }
        return new Batch(f[1], port, inc, hb, last, List.of());
    }
}
