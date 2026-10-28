package rep03;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class Producer {

    public static void main(String[] args) {
        int n;
        try {
            n = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        } catch (NumberFormatException e) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api); // não abre ligação: só guarda o endereço da API do daemon local
        System.out.println("Produtor pronto (N = " + n + ", API " + api + ").");
        System.out.println("Formato: <sensor> <temperatura> | 'rajada <n> <s>' | 'rajada-quente <n> <s>' | 'sair'");
        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break; // BLOQUEIA: à espera de que o utilizador escreva uma linha
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                String[] p = line.split("\\s+");
                if (p.length == 3 && (p[0].equalsIgnoreCase("rajada") || p[0].equalsIgnoreCase("rajada-quente"))) {
                    burst(ps, partitioner, p[1], p[2], p[0].equalsIgnoreCase("rajada-quente"));
                    continue;
                }
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }
                int k = partitioner.of(p[0]);
                String msg = Wire.in(p[0], temp, 0, new VectorClock(n)); // o produtor não observa registos: L = 0, V = 0
                if (send(ps, k, msg)) {
                    System.out.println("Publicado: " + msg + " → p" + k);
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Interrompido: " + e);
        }
    }

    // rajada <n> <s>: n leituras de sensores aleatórios entre S01 e S<s>, o mais depressa possível
    // rajada-quente <n> <s>: igual, mas 80 % das leituras vêm de S01 e as restantes de S02..S<s>
    static void burst(IpfsPubSub ps, Partitioner partitioner, String nArg, String sArg, boolean hot)
            throws IOException, InterruptedException {
        int n, s;
        try {
            n = Integer.parseInt(nArg);
            s = Integer.parseInt(sArg);
        } catch (NumberFormatException e) {
            System.out.println("Formato: rajada <n> <s>, com n >= 1 e s >= 1");
            return;
        }
        if (n < 1 || s < 1) {
            System.out.println("Formato: rajada <n> <s>, com n >= 1 e s >= 1");
            return;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        int[] perPartition = new int[partitioner.count()];
        int failed = 0;
        long first = 0, last = 0;
        for (int i = 0; i < n; i++) {
            String sensor;
            if (hot && (s < 2 || rnd.nextDouble() < 0.8)) {
                sensor = "S01";
            } else {
                sensor = sensorName(hot ? rnd.nextInt(2, s + 1) : rnd.nextInt(1, s + 1));
            }
            double temp = Math.round(rnd.nextDouble(15.0, 30.0) * 10) / 10.0;
            int k = partitioner.of(sensor);
            if (send(ps, k, Wire.in(sensor, temp, 0, new VectorClock(partitioner.count())))) {
                perPartition[k]++;
            } else {
                failed++;
            }
            last = System.currentTimeMillis();
            if (i == 0) first = last;
        }
        long ms = last - first;
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < perPartition.length; k++) {
            sb.append(k == 0 ? "" : " | ").append("p").append(k).append(": ").append(perPartition[k]);
        }
        System.out.println((hot ? "Rajada quente" : "Rajada") + " de " + n + " leituras (S01..S" + String.format("%02d", s) + ") → " + sb
                + (failed > 0 ? " | publicações recusadas pelo daemon: " + failed : ""));
        System.out.println("  primeira publicação: " + first + " | última publicação: " + last);
        System.out.printf("  tempo de envio: %d ms | débito: %.0f leituras/s%n", ms, n * 1000.0 / Math.max(ms, 1));
    }

    // publica uma leitura no tópico de entrada da partição k. Se o daemon local recusar a publicação
    // (resposta diferente de 200), a leitura perde-se no troço produtor → líder: é mostrada e o produtor
    // continua. Se o daemon não estiver acessível (ConnectException), a exceção termina o produtor.
    static boolean send(IpfsPubSub ps, int k, String msg) throws IOException, InterruptedException {
        try {
            ps.publish(Wire.topicIn(k), msg); // BLOQUEIA: até o daemon local responder
            return true;
        } catch (ConnectException e) {
            throw e;
        } catch (IOException e) {
            System.out.println("PUBLICAÇÃO RECUSADA (" + msg + "): " + e.getMessage());
            return false;
        }
    }

    static String sensorName(int i) {
        return String.format("S%02d", i);
    }
}
