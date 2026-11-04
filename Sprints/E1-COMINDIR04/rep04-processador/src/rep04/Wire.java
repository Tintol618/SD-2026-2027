package rep04;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;   // acima deste número de retidos, a réplica recupera
    public static final int T_TCP_MS = 5000;       // tempo máximo de ligação e de espera por cada linha
    public static final int PORTO_BASE = 7000;     // porto de recuperação do líder da partição k: 7000 + k
    public static final double LIMIAR_ALARME = 28.0; // temperatura a partir da qual o processador publica um alarme

    // heartbeat: período Δ, fator do TIMEOUT (TIMEOUT = FATOR_TIMEOUT × Δ) e período de verificação
    public static final long PERIODO_HB_MS = Long.getLong("rep04.periodo", 5000);
    public static final int FATOR_TIMEOUT = Integer.getInteger("rep04.fator", 3);
    public static final long VERIFICACAO_MS = 1000;

    private Wire() { }

    public static String topicIn(int k)   { return "rep04-in-p" + k; }
    public static String topicData(int k) { return "rep04-p" + k; }

    // IN;<sensor>;<temperatura>;<L>;<V>
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    // tipo da mensagem (IN, LOTE); lança IllegalArgumentException se não houver tipo
    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    // conteúdo da mensagem sem o tipo
    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }
}
