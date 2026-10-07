package tcp01;

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;                              // porto do servidor
            s = new Socket("localhost", serverPort);            // BLOQUEIA: à espera de que a ligação seja estabelecida (falha se não houver servidor)
            DataInputStream in = new DataInputStream(s.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            Person person = new Person("Ana Silva", new Place("3504-510", "Viseu"), 2000);
            out.writeObject(person);                            // envia a Person e, com ela, o Place que referencia
            out.flush();
            String data = in.readUTF();                         // BLOQUEIA: à espera da resposta do servidor
            System.out.println("Received: " + data);
        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e);
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}
