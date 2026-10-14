package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1); // o datagrama não sai da rede local
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq(); // continua a numeração após reinício
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'desordem' | 'sair' para terminar");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break; // BLOQUEIA: à espera de que o utilizador escreva uma linha na CLI
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                String[] p = line.split("\\s+");
                if (p[0].equalsIgnoreCase("rajada")) {
                    seq = burst(socket, group, file, seq, p);
                    continue;
                }
                if (p.length == 1 && p[0].equalsIgnoreCase("desordem")) {
                    seq = outOfOrder(socket, group, file, seq);
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
                file.append(r);         // 1) escreve localmente
                send(socket, group, r); // 2) propaga para o grupo
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e);
        }
    }

    // propagação: um único datagrama para o endereço de grupo; o líder não sabe quem (nem quantos) o recebem
    static void send(MulticastSocket socket, InetAddress group, SensorRecord r) throws IOException {
        byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
        socket.send(new DatagramPacket(m, m.length, group, PORT)); // não espera pelas réplicas: entrega o datagrama ao SO e retorna
    }

    // comando 'rajada <n>' (ponto 7): n registos do sensor SIM, enviados o mais depressa possível, sem pausas
    static long burst(MulticastSocket socket, InetAddress group, RecordFile file, long seq, String[] p) throws IOException {
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
            file.append(r);         // 1) escreve localmente
            send(socket, group, r); // 2) propaga para o grupo
            last = System.currentTimeMillis();
            if (i == 0) first = last;
        }
        printBurst(n, seq, first, last);
        return seq;
    }

    // MODO DE TESTE (ponto 6): regista três registos pela ordem normal, mas envia o 2.º e o 3.º antes do 1.º
    static long outOfOrder(MulticastSocket socket, InetAddress group, RecordFile file, long seq) throws IOException {
        SensorRecord[] r = new SensorRecord[3];
        for (int i = 0; i < r.length; i++) {
            r[i] = SensorRecord.now(++seq, "ORD", randomTemperature());
            file.append(r[i]);         // 1) escreve localmente, pela ordem do líder
        }
        for (int i : new int[] {1, 2, 0}) {
            send(socket, group, r[i]); // 2) propaga fora de ordem
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
