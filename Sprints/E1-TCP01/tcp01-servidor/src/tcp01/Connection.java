package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    ObjectInputStream in;
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            out = new DataOutputStream(clientSocket.getOutputStream());
            // O ObjectInputStream NÃO é criado aqui: a sua criação bloqueia até chegar o cabeçalho
            // do ObjectOutputStream do cliente, e o construtor corre na thread principal (a do accept()).
            this.start();                                       // executa run() numa thread separada
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            in = new ObjectInputStream(clientSocket.getInputStream()); // BLOQUEIA: à espera do cabeçalho do ObjectOutputStream do cliente
            Object received = in.readObject();                  // BLOQUEIA: à espera de um objeto completo
            if (received instanceof Person) {
                Person person = (Person) received;
                System.out.println("[" + getName() + "] recebido: " + person);
                out.writeUTF(person.getPlace().getLocality());  // envia a localidade ao cliente
            } else {
                System.out.println("[" + getName() + "] objeto inesperado: " + received.getClass().getName());
                out.writeUTF("erro: esperava um objeto Person");
            }
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFound: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                /* falha ao fechar */
            }
        }
    }
}
