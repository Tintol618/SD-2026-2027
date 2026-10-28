package rep03;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

public class AlarmProcessor {
    static final String PREFIXO = "ALARM-";

    static int n;
    static Partitioner partitioner;
    static IpfsPubSub ps;

    // conhecimento causal do processador, partilhado pelas threads de subscrição; protegido por lock
    static final Object lock = new Object();
    static long lamport;      // Lp
    static VectorClock vector; // Vp
    static List<Set<Long>> seen = new ArrayList<>(); // seq já recebidos, por partição (para ignorar duplicados)

    public static void main(String[] args) {
        try {
            n = Integer.parseInt(args[0]);
            partitioner = new Partitioner(n);
        } catch (RuntimeException e) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        ps = new IpfsPubSub(api);
        vector = new VectorClock(n);
        for (int k = 0; k < n; k++) {
            seen.add(new HashSet<>());
        }
        System.out.println("Processador de alarmes (N = " + n + ", limiar " + Wire.LIMIAR_ALARME + " °C, API " + api + ")");
        List<Thread> threads = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            final int part = k;
            Thread t = new Thread(() -> subscribe(part), "sub-p" + k); // uma thread de subscrição por partição
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    static void subscribe(int k) {
        try {
            ps.subscribe(Wire.topicData(k), msg -> onReg(k, msg)); // BLOQUEIA: não termina; cada mensagem chama onReg
            System.out.println("[p" + k + "] a subscrição terminou (o daemon IPFS fechou a ligação).");
        } catch (IOException | UncheckedIOException e) {
            System.out.println("[p" + k + "] IO: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[p" + k + "] interrompida: " + e);
        }
    }

    static void onReg(int k, String msg) {
        SensorRecord r;
        try {
            r = SensorRecord.fromLine(Wire.regParts(msg)[2], n); // um vetor com dimensão diferente de N é rejeitado
        } catch (IllegalArgumentException e) { // inclui NumberFormatException
            System.out.println("[p" + k + "] REJEITADO: " + msg);
            return;
        }
        String sensor = r.getSensorId();
        if (sensor.startsWith(PREFIXO)) {
            return; // alarmes sobre alarmes: ignorados
        }
        long l;
        VectorClock v;
        synchronized (lock) {
            if (!seen.get(k).add(r.getSeq())) {
                System.out.println("[p" + k + "] DUPLICADO: " + r.getSeq());
                return;
            }
            // acumula o máximo de TODOS os registos recebidos, de todas as partições
            lamport = Math.max(lamport, r.getLamport());
            vector.merge(r.getVector());
            if (r.getTemperature() < Wire.LIMIAR_ALARME) {
                return;
            }
            // cópia tirada dentro da secção sincronizada: outra thread pode alterar o vetor logo a seguir
            l = lamport;
            v = vector.copy();
        }
        String alarm = PREFIXO + sensor;
        int j = partitioner.of(alarm);
        try {
            ps.publish(Wire.topicIn(j), Wire.in(alarm, r.getTemperature(), l, v));
            System.out.println("ALARME: " + sensor + " p" + k + "#" + r.getSeq() + " → p" + j
                    + " (L=" + l + " V=" + v + ")");
        } catch (IOException e) {
            // o alarme perde-se no troço processador → líder, como uma leitura do produtor
            System.out.println("PUBLICAÇÃO FALHOU (alarme de " + sensor + " p" + k + "#" + r.getSeq() + "): " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
