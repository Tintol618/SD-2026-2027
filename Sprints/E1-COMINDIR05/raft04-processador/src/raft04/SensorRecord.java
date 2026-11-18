package raft04;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class SensorRecord {
    private final long seq;
    private final long term;      // termo em que o registo foi criado pelo líder
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, long term, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    // cria um registo com o instante atual (usado só pelo líder), ao milissegundo; guarda uma cópia do vetor
    public static SensorRecord now(long seq, long term, String sensorId, double temperature, long lamport, VectorClock vector) {
        return new SensorRecord(seq, term, sensorId, temperature,
                LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS).toString(), lamport, vector.copy());
    }

    // seq;termo;sensor;temperatura;instante;L;V
    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    // a dimensão do vetor é a que vier na linha
    public static SensorRecord fromLine(String line) {
        String[] f = fields(line);
        return fromFields(f, f[6].split(",").length);
    }

    // a dimensão do vetor tem de ser n
    public static SensorRecord fromLine(String line, int n) {
        return fromFields(fields(line), n);
    }

    private static String[] fields(String line) {
        String[] f = line.trim().split(";");
        if (f.length != 7) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return f;
    }

    private static SensorRecord fromFields(String[] f, int n) {
        return new SensorRecord(Long.parseLong(f[0]), Long.parseLong(f[1]), f[2], Double.parseDouble(f[3]), f[4],
                Long.parseLong(f[5]), VectorClock.parse(f[6], n));
    }

    // mesmo registo: mesmo seq e mesmo termo
    public boolean sameEntry(SensorRecord o) {
        return o != null && seq == o.seq && term == o.term;
    }

    public long getSeq() { return seq; }
    public long getTerm() { return term; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector.copy(); }
}
