package rep03;

import java.time.LocalDateTime;

public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, String sensorId, double temperature, String timestamp, long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    // cria um registo com o instante atual (usado só pelo líder); guarda uma cópia do vetor
    public static SensorRecord now(long seq, String sensorId, double temperature, long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensorId, temperature, LocalDateTime.now().withNano(0).toString(), lamport, vector.copy());
    }

    // representação textual: é o que se guarda no ficheiro e o que se envia pela rede
    // seq;sensor;temperatura;instante;L;V
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    // a dimensão do vetor é a que vier na linha (as réplicas não conhecem N)
    public static SensorRecord fromLine(String line) {
        String[] f = fields(line);
        return fromFields(f, f[5].split(",").length);
    }

    // a dimensão do vetor tem de ser n (lança IllegalArgumentException caso contrário)
    public static SensorRecord fromLine(String line, int n) {
        return fromFields(fields(line), n);
    }

    private static String[] fields(String line) {
        String[] f = line.trim().split(";");
        if (f.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return f;
    }

    private static SensorRecord fromFields(String[] f, int n) {
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3],
                Long.parseLong(f[4]), VectorClock.parse(f[5], n));
    }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector.copy(); }
}
