import java.net.*;
import java.io.*;

public class UDPServer {

    public static void main(String args[]) {
        DatagramSocket aSocket = null;
        int lastInOrder = 0;
        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];
            System.out.println("Servidor UDP à escuta na porta 6789 (L = " + lastInOrder + ")");
            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);
                String received = new String(request.getData(), 0, request.getLength());
                String answer;
                int comma = received.indexOf(',');
                int n = -1;
                if (comma > 0) {
                    try {
                        n = Integer.parseInt(received.substring(0, comma).trim());
                    } catch (NumberFormatException e) {
                        n = -1;
                    }
                }
                int before = lastInOrder;
                if (n == lastInOrder + 1) {
                    lastInOrder = n;
                    answer = received;
                } else {
                    answer = "waitingfor," + (lastInOrder + 1);
                }
                System.out.println("De " + request.getAddress().getHostAddress() + ":" + request.getPort()
                        + " | recebido \"" + received + "\" | L: " + before + " -> " + lastInOrder
                        + " | resposta \"" + answer + "\"");
                byte[] m = answer.getBytes();
                DatagramPacket reply = new DatagramPacket(m, m.length,
                        request.getAddress(), request.getPort());
                aSocket.send(reply);
            }
        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); }
    }
}