import java.net.*;
import java.io.*;

public class UDPClient {

    public static void main(String args[]) {
        DatagramSocket aSocket = null;

        try {
            aSocket = new DatagramSocket();
            InetAddress aHost = InetAddress.getByName("localhost");
            int serverPort = 6789;
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            boolean manual = false;
            int nextNumber = 1;
            System.out.println("Cliente UDP -> " + serverPort);
            while (true) {
                System.out.print(manual ? "<N>,<mensagem>: " : "[#" + nextNumber + "] mensagem: ");
                String line = in.readLine();
                if (line == null || line.equals("/sair")) break;
                if (line.equals("/auto"))   { manual = false; System.out.println("Modo automático."); continue; }
                if (line.equals("/manual")) { manual = true;  System.out.println("Modo manual."); continue; }
                if (line.isEmpty()) continue;
                String text = manual ? line : nextNumber++ + "," + line;
                byte[] m = text.getBytes();
                DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort);
                aSocket.send(request);
                byte[] buffer = new byte[1000];
                DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(reply);
                String answer = new String(reply.getData(), 0, reply.getLength());
                if (answer.startsWith("waitingfor,")) {
                    System.out.println("Enviado: " + text + " | FORA DE ORDEM -> o servidor espera a mensagem "
                            + answer.substring("waitingfor,".length()));
                } else {
                    System.out.println("Enviado: " + text + " | Echo: " + answer);
                }
            }
        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); }
    }
}