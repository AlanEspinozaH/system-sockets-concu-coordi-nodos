/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package central;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.*;

public class CentralServer {

    public static void main(String[] args) throws Exception {
        int chatPort = 6001;
        int bankPort = 6002;

        // Routing 3 partes con primarios N1,N2,N3
        RoutingTable rt = new RoutingTable();
        rt.putPart(0, new RoutingTable.Node("N1","localhost",6101),
                      new RoutingTable.Node("N2","localhost",6102),
                      new RoutingTable.Node("N3","localhost",6103));
        rt.putPart(1, new RoutingTable.Node("N2","localhost",6102),
                      new RoutingTable.Node("N3","localhost",6103),
                      new RoutingTable.Node("N1","localhost",6101));
        rt.putPart(2, new RoutingTable.Node("N3","localhost",6103),
                      new RoutingTable.Node("N1","localhost",6101),
                      new RoutingTable.Node("N2","localhost",6102));

        ShardLocator loc = new ShardLocator(3);
        TwoPC two = new TwoPC(rt, loc, 1500, 1000);

        ExecutorService pool = Executors.newFixedThreadPool(64);

        Thread chat = new Thread(() -> listen(chatPort, pool, rt, loc, two), "chat-listener");
        Thread bank = new Thread(() -> listen(bankPort, pool, rt, loc, two), "bank-listener");
        chat.start(); bank.start();
        System.out.println("Central listo. Chat="+chatPort+" Banco="+bankPort);
        chat.join(); bank.join();
    }

    private static void listen(int port, ExecutorService pool, RoutingTable rt, ShardLocator loc, TwoPC two) {
        try (ServerSocket ss = new ServerSocket(port)) {
            while (true) {
                Socket s = ss.accept();
                pool.submit(new ClientHandler(s, rt, loc, two));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
