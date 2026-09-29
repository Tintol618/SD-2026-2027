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
            System.out.println("Cliente UDP -> " + aHost.getHostAddress() + ":" + serverPort);
            while (true) {
                System.out.print(manual ? "[manual] <N>,<mensagem>: " : "[auto #" + nextNumber + "] mensagem: ");
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
                if (answer.startsWith("ok,")) {
                    System.out.println("Enviado: " + text + " | ENTREGUE -> o servidor já entregou até à mensagem "
                            + answer.substring("ok,".length()));
                } else if (answer.startsWith("waitingfor,")) {
                    System.out.println("Enviado: " + text + " | GUARDADA (fora de ordem) -> o servidor espera a mensagem "
                            + answer.substring("waitingfor,".length()));
                } else if (answer.startsWith("dup,")) {
                    System.out.println("Enviado: " + text + " | DUPLICADA -> a mensagem "
                            + answer.substring("dup,".length()) + " já tinha sido recebida");
                } else {
                    System.out.println("Enviado: " + text + " | Resposta: " + answer);
                }
            }
        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); }
    }
}
