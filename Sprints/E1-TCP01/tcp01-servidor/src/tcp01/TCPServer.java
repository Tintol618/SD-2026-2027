package tcp01;

import java.io.*;
import java.net.*;

public class TCPServer {
    public static void main(String[] args) {
        try {
            int serverPort = 7896;
            ServerSocket listenSocket = new ServerSocket(serverPort);
            System.out.println("Servidor TCP à escuta na porta " + serverPort);
            while (true) {
                Socket clientSocket = listenSocket.accept();    // BLOQUEIA: à espera de que um cliente se ligue
                System.out.println("Ligação aceite de " + clientSocket.getInetAddress().getHostAddress()
                        + ":" + clientSocket.getPort());
                Connection c = new Connection(clientSocket);    // processa o pedido noutra thread
            }
        } catch (IOException e) {
            System.out.println("Listen: " + e.getMessage());
        }
    }
}
