import java.net.*;
import java.io.*;
import java.util.*;

public class UDPServer {

    // Lista de receção: mensagens ENTREGUES, pela ordem do número de sequência (posição i -> mensagem i+1).
    static List<String> receptionList = new ArrayList<>();
    // Estrutura temporária: mensagens RECEBIDAS fora de ordem, ainda não entregues (N -> mensagem).
    static TreeMap<Integer, String> pending = new TreeMap<>();

    /**
     * Processa a mensagem N recebida, dado L (última mensagem entregue em ordem).
     * - N == L+1: entrega-a na lista de receção e, em cascata, todas as que estavam
     *   guardadas e passam a ser a seguinte (L+2, L+3, ...), retirando-as da estrutura temporária.
     * - N > L+1: guarda-a na estrutura temporária (recebida, não entregue).
     * - N <= L ou N já guardada: duplicado, não altera nada.
     * Devolve o novo L (a última mensagem entregue depois do processamento).
     */
    static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
        if (nCurrentMessage <= nLastMessageInOrder || pending.containsKey(nCurrentMessage)) {
            return nLastMessageInOrder;
        }
        if (nCurrentMessage != nLastMessageInOrder + 1) {
            pending.put(nCurrentMessage, currentMessage);
            return nLastMessageInOrder;
        }
        receptionList.add(currentMessage);
        int last = nCurrentMessage;
        while (pending.containsKey(last + 1)) {
            last++;
            receptionList.add(pending.remove(last));
        }
        return last;
    }

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
                int comma = received.indexOf(',');
                int n = -1;
                String text = "";
                if (comma > 0) {
                    try {
                        n = Integer.parseInt(received.substring(0, comma).trim());
                        text = received.substring(comma + 1);
                    } catch (NumberFormatException e) {
                        n = -1;
                    }
                }
                String answer;
                int before = lastInOrder;
                if (n < 1) {
                    answer = "waitingfor," + (lastInOrder + 1);
                } else if (n <= lastInOrder || pending.containsKey(n)) {
                    answer = "dup," + n;
                } else {
                    lastInOrder = processDeliveredMessages(lastInOrder, n, text);
                    answer = lastInOrder > before ? "ok," + lastInOrder : "waitingfor," + (lastInOrder + 1);
                }
                System.out.println("De " + request.getAddress().getHostAddress() + ":" + request.getPort()
                        + " | recebido \"" + received + "\" | L: " + before + " -> " + lastInOrder
                        + " | resposta \"" + answer + "\"");
                System.out.println("    entregues " + receptionList + " | temporárias " + pending);
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
