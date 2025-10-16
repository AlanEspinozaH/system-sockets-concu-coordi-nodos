package central;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
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

        ShardLocator locator = new ShardLocator(3);
        TwoPC twoPC = new TwoPC(rt, locator, 1500, 1000);

        ExecutorService pool = Executors.newFixedThreadPool(64);

        // 💡 Declaramos las variables finales para usarlas dentro de las lambdas
        final int chatPortFinal = chatPort;
        final int bankPortFinal = bankPort;
        final RoutingTable rtFinal = rt;
        final ShardLocator locatorFinal = locator;
        final TwoPC twoPCFinal = twoPC;

        Thread chatListener = new Thread(() -> listen(chatPortFinal, pool, rtFinal, locatorFinal, twoPCFinal), "chat-listener");
        Thread bankListener = new Thread(() -> listen(bankPortFinal, pool, rtFinal, locatorFinal, twoPCFinal), "bank-listener");
        chatListener.start(); 
        bankListener.start();

        System.out.println("Central listo. Chat=" + chatPort + " Banco=" + bankPort);
        chatListener.join(); 
        bankListener.join();
    }



    private static void listen(int port, ExecutorService pool,
                               RoutingTable rt, ShardLocator locator, TwoPC twoPC) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.printf("Escuchando en puerto %d...%n", port);
            while (true) {
                try {
                    Socket client = serverSocket.accept();
                    pool.submit(new ClientHandler(client, rt, locator, twoPC));
                } catch (IOException e) {
                    System.err.println("Error aceptando conexión: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.printf("No se pudo abrir el puerto %d: %s%n", port, e.getMessage());
        }
    }
}
