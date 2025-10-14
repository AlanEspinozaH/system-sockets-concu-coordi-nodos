/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */

package clientbank;

import util.LengthPrefixedCodec;
import util.JsonLite;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class LoadGen {
   
    static class Client {
        final String host; final int port;
        Client(String h,int p){host=h;port=p;}
        String send(String type, Map<String,Object> data) throws IOException {
            Map<String,Object> m = new LinkedHashMap<>();
            m.put("type", type);
            m.put("req_id", UUID.randomUUID().toString());
            m.put("client_id", "LOAD");
            m.put("ts", System.currentTimeMillis());
            m.put("data", data);
            try (Socket s = new Socket(host, port)) {
                LengthPrefixedCodec.write(s.getOutputStream(), JsonLite.obj(m));
                return LengthPrefixedCodec.read(s.getInputStream());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost"; int port = 6002;
        int threads = 50, tx = 1000, minMs = 5, maxMs = 100, maxId = 10000;
        for (int i=0;i<args.length;i++){
            switch (args[i]) {
                case "--server": String[] hp = args[++i].split(":"); host=hp[0]; port=Integer.parseInt(hp[1]); break;
                case "--threads": threads = Integer.parseInt(args[++i]); break;
                case "--tx": tx = Integer.parseInt(args[++i]); break;
                case "--sleep": String[] mm = args[++i].split("\\.\\."); minMs=Integer.parseInt(mm[0]); maxMs=Integer.parseInt(mm[1]); break;
                case "--maxId": maxId = Integer.parseInt(args[++i]); break;
            }
        }

        Client c = new Client(host, port);
        
         final int fThreads = threads;
         final int fTx = tx;
         final int fMinMs = minMs;
         final int fMaxMs = maxMs;
         final int fMaxId = maxId;
         final int per = Math.max(1, fTx / fThreads);
         final Client client = c;
        
        List<Thread> ths = new ArrayList<>();
        for (int t = 0; t < fThreads; t++) {
          Thread th = new Thread(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (int k = 0; k < per; k++) {
            long o = 1 + rnd.nextInt(fMaxId);
            long d = 1 + rnd.nextInt(fMaxId);
            while (d == o) d = 1 + rnd.nextInt(fMaxId);
            double m = 1 + rnd.nextInt(50);

            Map<String,Object> data = new LinkedHashMap<>();
            data.put("origen", o);
            data.put("destino", d);
            data.put("monto", m);
            data.put("tx_id", UUID.randomUUID().toString());

            try { client.send("TRANSFERIR_CUENTA", data); } catch (Exception ignore) {}
            try { Thread.sleep(rnd.nextInt(fMinMs, fMaxMs + 1)); } catch (InterruptedException ie) { break; }
            }
          }, "lg-" + t);
          ths.add(th);
          th.start();
        }
        
        for (Thread th: ths) th.join();
        System.out.println("LoadGen OK: threads="+threads+" tx≈"+(per*threads));
        // arqueo
        Map<String,Object> empty = new LinkedHashMap<>();
        String r = client.send("ARQUEO_CUENTAS", empty);
        System.out.println("ARQUEO: " + r);
    }
}
