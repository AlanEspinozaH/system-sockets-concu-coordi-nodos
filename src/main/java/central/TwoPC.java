/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package central;

import util.LengthPrefixedCodec;
import util.JsonLite;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class TwoPC {
    private final RoutingTable routing;
    private final ShardLocator locator;
    private final int prepareTimeoutMs;
    private final int commitTimeoutMs;

    public TwoPC(RoutingTable routing, ShardLocator locator, int prepareTimeoutMs, int commitTimeoutMs) {
        this.routing = routing;
        this.locator = locator;
        this.prepareTimeoutMs = prepareTimeoutMs;
        this.commitTimeoutMs = commitTimeoutMs;
    }

    private static String msg(String type, String txId, long id, double delta) {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("req_id", UUID.randomUUID().toString());
        m.put("client_id", "CENTRAL");
        m.put("ts", System.currentTimeMillis());
        Map<String,Object> d = new LinkedHashMap<>();
        d.put("tx_id", txId);
        List<Object> ops = new ArrayList<>();
        Map<String,Object> op = new LinkedHashMap<>();
        op.put("id", id);
        op.put("delta", delta);
        ops.add(op);
        d.put("ops", ops); // el Worker maneja lista de ops; aquí una sola por nodo
        m.put("data", d);
        return JsonLite.obj(m);
    }

    private static String commitAbort(String type, String txId) {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("req_id", UUID.randomUUID().toString());
        m.put("client_id", "CENTRAL");
        m.put("ts", System.currentTimeMillis());
        Map<String,Object> d = new LinkedHashMap<>();
        d.put("tx_id", txId);
        m.put("data", d);
        return JsonLite.obj(m);
    }

    private static boolean voteCommit(String resp) {
        String t = JsonLite.getString(resp, "type");
        return "VOTE_COMMIT".equals(t) || "OK".equals(t);
    }

    public String transfer(long origen, long destino, double monto, String txId) {
        int pO = locator.shardFor(origen);
        int pD = locator.shardFor(destino);

        RoutingTable.Node nO = routing.primaryOf(pO);
        RoutingTable.Node nD = routing.primaryOf(pD);

        if (pO == pD) {
            // Transferencia intra-shard: enviar APPLY única al primario
            try (Socket s = new Socket(nO.host, nO.port)) {
                String apply = msg("APPLY_TRANSFER_LOCAL", txId, origen, -monto);
                LengthPrefixedCodec.write(s.getOutputStream(), apply);
                // segundo delta en mismo shard
                String apply2 = msg("APPLY_TRANSFER_LOCAL", txId, destino, +monto);
                LengthPrefixedCodec.write(s.getOutputStream(), apply2);
                String r1 = LengthPrefixedCodec.read(s.getInputStream());
                String r2 = LengthPrefixedCodec.read(s.getInputStream());
                if (voteCommit(r1) && voteCommit(r2)) {
                    // replicación asíncrona la maneja el Central (simple)
                    replicateAsync(pO, txId, new long[]{origen, destino}, new double[]{-monto, +monto});
                    return ok("OK", "tx="+txId);
                }
                return err("ERROR", "intra-shard failed");
            } catch (Exception e) {
                return err("ERROR", "intra-shard exception: " + e.getMessage());
            }
        }

        // 2PC: PREPARE a ambos
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<Boolean> f1 = pool.submit(() -> sendAndVote(nO, msg("PREPARE", txId, origen, -monto), prepareTimeoutMs));
        Future<Boolean> f2 = pool.submit(() -> sendAndVote(nD, msg("PREPARE", txId, destino, +monto), prepareTimeoutMs));
        boolean v1=false, v2=false;
        try {
            v1 = f1.get(prepareTimeoutMs + 200, TimeUnit.MILLISECONDS);
            v2 = f2.get(prepareTimeoutMs + 200, TimeUnit.MILLISECONDS);
        } catch (Exception e) { /* timeout / fail */ }
        pool.shutdownNow();

        if (v1 && v2) {
            // COMMIT
            boolean c1 = sendSimple(nO, commitAbort("COMMIT", txId), commitTimeoutMs);
            boolean c2 = sendSimple(nD, commitAbort("COMMIT", txId), commitTimeoutMs);
            if (c1 && c2) {
                replicateAsync(pO, txId, new long[]{origen}, new double[]{-monto});
                replicateAsync(pD, txId, new long[]{destino}, new double[]{+monto});
                return ok("OK", "tx="+txId);
            } else {
                // intento de rollback "best effort"
                sendSimple(nO, commitAbort("ABORT", txId), commitTimeoutMs);
                sendSimple(nD, commitAbort("ABORT", txId), commitTimeoutMs);
                return err("ERROR", "commit failed");
            }
        } else {
            // ABORT
            sendSimple(nO, commitAbort("ABORT", txId), commitTimeoutMs);
            sendSimple(nD, commitAbort("ABORT", txId), commitTimeoutMs);
            return err("ERROR", "vote abort");
        }
    }

    private boolean sendSimple(RoutingTable.Node n, String msg, int timeoutMs) {
        try (Socket s = new Socket(n.host, n.port)) {
            s.setSoTimeout(timeoutMs);
            LengthPrefixedCodec.write(s.getOutputStream(), msg);
            String resp = LengthPrefixedCodec.read(s.getInputStream());
            return voteCommit(resp);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean sendAndVote(RoutingTable.Node n, String msg, int timeoutMs) {
        try (Socket s = new Socket(n.host, n.port)) {
            s.setSoTimeout(timeoutMs);
            LengthPrefixedCodec.write(s.getOutputStream(), msg);
            String resp = LengthPrefixedCodec.read(s.getInputStream());
            return voteCommit(resp);
        } catch (Exception e) {
            return false;
        }
    }

    private void replicateAsync(int part, String txId, long[] ids, double[] deltas) {
      for (RoutingTable.Node rep : routing.replicasOf(part)) {
        for (int i = 0; i < ids.length; i++) {
            final int idx = i;
            new Thread(() -> {
                try (Socket s = new Socket(rep.host, rep.port)) {
                    Map<String,Object> m = new LinkedHashMap<>();
                    m.put("type","REPLICATE_APPLY");
                    m.put("req_id", UUID.randomUUID().toString());
                    m.put("client_id","CENTRAL");
                    m.put("ts", System.currentTimeMillis());
                    Map<String,Object> d = new LinkedHashMap<>();
                    d.put("tx_id", txId + "-" + idx); // idempotencia por op
                    d.put("id", ids[idx]);
                    d.put("delta", deltas[idx]);
                    m.put("data", d);
                    LengthPrefixedCodec.write(s.getOutputStream(), JsonLite.obj(m));
                    LengthPrefixedCodec.read(s.getInputStream()); // OK
                } catch (Exception ignore) {}
            },"replicate-"+rep.id+"-op"+i).start();
        }
      }
    }

    
    private static String ok(String type, String msg) {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("msg", msg);
        return JsonLite.obj(m);
    }
    private static String err(String type, String msg) { return ok(type,msg); }
}