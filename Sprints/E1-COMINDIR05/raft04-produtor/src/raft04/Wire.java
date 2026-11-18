package raft04;

import java.nio.file.Files;
import java.nio.file.Paths;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;     // acima deste número de retidos, o seguidor recupera
    public static final int T_TCP_MS = 5000;         // tempo máximo de ligação e de espera por cada linha
    public static final int PORTO_BASE = 7000;       // porto de recuperação do nó i da partição k: 7000 + 10k + i
    public static final double LIMIAR_ALARME = 28.0; // temperatura a partir da qual o processador publica um alarme

    // heartbeat e detetor (produtor e processador)
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000);
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);
    public static final long VERIFICACAO_MS = 200;

    // eleição
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3 * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6 * PERIODO_HB_MS);
    public static final int GRUPO = 3;

    private Wire() { }

    public static String topicIn(int k)       { return "raft04-in-p" + k; }
    public static String topicData(int k)     { return "raft04-p" + k; }
    public static String topicElection(int k) { return "raft04-eleicao-p" + k; }

    public static int nodePort(int k, int i)  { return PORTO_BASE + 10 * k + i; }

    // partição de rede simulada: enquanto existir o ficheiro isolar-p<k>-n<i> na pasta de trabalho,
    // o nó não publica nem processa mensagens de dados e de eleição, e a recuperação TCP falha
    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }

    // IN;<sensor>;<temperatura>;<L>;<V>
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    // tipo da mensagem (IN, LOTE, PEDIDO_VOTO, VOTO); lança IllegalArgumentException se não houver tipo
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
