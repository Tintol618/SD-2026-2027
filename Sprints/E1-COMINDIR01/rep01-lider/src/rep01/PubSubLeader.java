package rep01;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub ipfs = new IpfsPubSub(api); // não abre ligação: só guarda o endereço da API do daemon local
        try (Scanner sc = new Scanner(System.in)) {
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq(); // continua a numeração após reinício
            System.out.println("Líder pronto (API " + api + ", tópico " + TOPIC + ").");
            System.out.println("Formato: <sensor> <temperatura> | 'rajada <n>' | 'desordem' | 'sair' para terminar");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break; // BLOQUEIA: à espera de que o utilizador escreva uma linha na CLI
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                String[] p = line.split("\\s+");
                if (p[0].equalsIgnoreCase("rajada")) {
                    seq = burst(ipfs, file, seq, p);
                    continue;
                }
                if (p.length == 1 && p[0].equalsIgnoreCase("desordem")) {
                    seq = outOfOrder(ipfs, file, seq);
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
                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r); // 1) escreve localmente
                send(ipfs, r);  // 2) propaga para o tópico
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Interrompido: " + e);
        }
    }

    // propagação: publica no tópico através do daemon IPFS local; o líder não sabe quem (nem quantos) subscrevem
    static void send(IpfsPubSub ipfs, SensorRecord r) throws IOException, InterruptedException {
        ipfs.publish(TOPIC, r.toLine()); // BLOQUEIA: até o daemon local responder ao POST /pubsub/pub
    }

    // comando 'rajada <n>' (ponto 7): n registos do sensor SIM, enviados o mais depressa possível, sem pausas
    static long burst(IpfsPubSub ipfs, RecordFile file, long seq, String[] p) throws IOException, InterruptedException {
        int n;
        try {
            n = p.length == 2 ? Integer.parseInt(p[1]) : 0;
        } catch (NumberFormatException e) {
            n = 0;
        }
        if (n < 1) {
            System.out.println("Formato: rajada <n>, com n >= 1");
            return seq;
        }
        long first = 0, last = 0;
        for (int i = 0; i < n; i++) {
            SensorRecord r = SensorRecord.now(++seq, "SIM", randomTemperature());
            file.append(r); // 1) escreve localmente
            send(ipfs, r);  // 2) propaga para o tópico
            last = System.currentTimeMillis();
            if (i == 0) first = last;
        }
        printBurst(n, seq, first, last);
        return seq;
    }

    // MODO DE TESTE (ponto 6): regista três registos pela ordem normal, mas envia o 2.º e o 3.º antes do 1.º
    static long outOfOrder(IpfsPubSub ipfs, RecordFile file, long seq) throws IOException, InterruptedException {
        SensorRecord[] r = new SensorRecord[3];
        for (int i = 0; i < r.length; i++) {
            r[i] = SensorRecord.now(++seq, "ORD", randomTemperature());
            file.append(r[i]); // 1) escreve localmente, pela ordem do líder
        }
        for (int i : new int[] {1, 2, 0}) {
            send(ipfs, r[i]);  // 2) propaga fora de ordem
            System.out.println("Registado e enviado (fora de ordem): " + r[i].toLine());
        }
        return seq;
    }

    static double randomTemperature() {
        return Math.round(ThreadLocalRandom.current().nextDouble(15.0, 30.0) * 10) / 10.0;
    }

    static void printBurst(int n, long lastSeq, long first, long last) {
        long ms = last - first;
        System.out.println("Rajada de " + n + " registos (seq " + (lastSeq - n + 1) + ".." + lastSeq + ")");
        System.out.println("  envio do primeiro: " + first + " | envio do último: " + last);
        System.out.printf("  tempo de envio: %d ms | débito: %.0f registos/s%n", ms, n * 1000.0 / Math.max(ms, 1));
    }
}
