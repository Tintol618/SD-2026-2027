package raft04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class AlarmProcessor {
    static final String PREFIXO = "ALARM-";

    static int n;
    static Partitioner partitioner;
    static IpfsPubSub ps;
    static HeartbeatMonitor[] monitores;
    static final Path monitorFile = Paths.get("monitor-processador.txt");

    // conhecimento causal do processador e controlo de lotes, partilhados pelas threads de subscrição; protegidos por lock
    static final Object lock = new Object();
    static long lamport;
    static VectorClock vector;
    static long[] maxTerm;                               // maior termo observado por partição
    static List<Set<String>> seen = new ArrayList<>();   // pares seq#termo já tratados, por partição
    static int alarms, repeated;

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
        maxTerm = new long[n];
        monitores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            seen.add(new HashSet<>());
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, AlarmProcessor::event);
        }
        System.out.println("Processador de alarmes (N = " + n + ", limiar " + Wire.LIMIAR_ALARME + " °C)");
        List<Thread> threads = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            final int part = k;
            Thread t = new Thread(() -> subscribe(part), "sub-p" + k);
            threads.add(t);
            t.start();
        }
        Thread v = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(Wire.VERIFICACAO_MS);
                } catch (InterruptedException e) {
                    return;
                }
                for (HeartbeatMonitor m : monitores) m.check();
            }
        }, "verificacao");
        v.setDaemon(true);
        v.start();
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    static synchronized void event(String e) {
        String line = System.currentTimeMillis() + ";" + e;
        System.out.println(line);
        try {
            Files.writeString(monitorFile, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ex) {
            System.out.println("Não foi possível escrever em " + monitorFile + ": " + ex);
        }
    }

    static void subscribe(int k) {
        while (true) {
            try {
                ps.subscribe(Wire.topicData(k), msg -> onLote(k, msg));
            } catch (IOException | UncheckedIOException e) {
                System.out.println("[p" + k + "] IO: " + e);
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

    static void onLote(int k, String msg) {
        Batch b;
        try {
            b = Batch.decode(msg, n);
        } catch (IllegalArgumentException e) {
            System.out.println("[p" + k + "] REJEITADO: " + msg.split("\n")[0]);
            return;
        }
        synchronized (lock) {
            if (b.term < maxTerm[k]) {                       // lote de um líder obsoleto
                event("IGNORADO;p" + k + ";lote termo=" + b.term + ";lider=n" + b.leader + ";maior termo=" + maxTerm[k]);
                return;
            }
            maxTerm[k] = b.term;
        }
        monitores[k].onHeartbeat(b.term, b.leader, b.hb);
        event("RECEBIDO;p" + k + ";termo=" + b.term + ";lider=n" + b.leader + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq
                + ";n=" + b.records.size() + ";seq=" + (b.records.isEmpty() ? "-"
                : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq()));
        for (SensorRecord r : b.records) onReg(k, r);
    }

    // cada registo é identificado pelo par (seq, termo): um (seq 7, termo 3) depois de (seq 7, termo 2) é outro registo
    static void onReg(int k, SensorRecord r) {
        String sensor = r.getSensorId();
        if (sensor.startsWith(PREFIXO)) return;
        long l;
        VectorClock v;
        synchronized (lock) {
            String id = r.getSeq() + "#" + r.getTerm();
            if (!seen.get(k).add(id)) return;              // par já tratado
            lamport = Math.max(lamport, r.getLamport());
            vector.merge(r.getVector());
            if (r.getTemperature() < Wire.LIMIAR_ALARME) return;
            for (String s : seen.get(k)) {                  // o mesmo seq já tratado noutro termo?
                if (s.startsWith(r.getSeq() + "#") && !s.equals(id)) { repeated++; break; }
            }
            alarms++;
            l = lamport;
            v = vector.copy();
        }
        String alarm = PREFIXO + sensor;
        int j = partitioner.of(alarm);
        if (monitores[j].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: partição p" + j + " sem líder conhecido; alarme " + alarm + " pode perder-se");
        }
        try {
            ps.publish(Wire.topicIn(j), Wire.in(alarm, r.getTemperature(), l, v));
            System.out.println("ALARME: " + sensor + " p" + k + "#" + r.getSeq() + "(t" + r.getTerm() + ") → p" + j
                    + " (L=" + l + " V=" + v + ") | alarmes " + alarms + ", de seq já tratado noutro termo " + repeated);
        } catch (IOException e) {
            System.out.println("PUBLICAÇÃO FALHOU (alarme de " + sensor + "): " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
