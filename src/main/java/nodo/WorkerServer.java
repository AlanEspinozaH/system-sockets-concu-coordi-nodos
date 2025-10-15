/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package nodo;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;

public class WorkerServer {

    public static void main(String[] args) throws Exception {
        int port = (args.length >= 1) ? Integer.parseInt(args[0]) : 6101;
        int poolSize = 32;

        ShardStore store = new ShardStore(port);
        TxLog txlog = new TxLog(port);

        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        try (ServerSocket ss = new ServerSocket(port)) {
            System.out.println("WorkerServer en puerto " + port);
            while (true) {
                Socket s = ss.accept();
                pool.submit(new WorkerHandler(s, store, txlog));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pool.shutdownNow();
        }
    }
}
