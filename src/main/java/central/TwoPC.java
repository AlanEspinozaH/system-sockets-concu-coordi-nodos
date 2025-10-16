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
        d.put("ops", ops);
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
        Boolean ok = "VOTE_COMMIT".equals(t) || "OK".equals(t);
        return ok != null && ok;
    }


    public String transfer(long origen, long destino, double monto, String txId) {
        int pO = locator.shardFor(origen);
        int pD = locator.shardFor(destino);

        RoutingTable.Node nO = routing.primaryOf(pO);
        RoutingTable.Node nD = routing.primaryOf(pD);

        System.out.printf("💱 Iniciando transfer tx=%s de %d→%d (%.2f)%n", txId, origen, destino, monto);

        
        if (pO == pD) {
            try (Socket s = new Socket(nO.host, nO.port)) {
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("type", "APPLY_TRANSFER_LOCAL");
                m.put("req_id", UUID.randomUUID().toString());
                m.put("client_id", "CENTRAL");
                m.put("ts", System.currentTimeMillis());

                Map<String,Object> d = new LinkedHashMap<>();
                d.put("tx_id", txId);
                List<Object> ops = new ArrayList<>();
                ops.add(Map.of("id", origen, "delta", -monto));
                ops.add(Map.of("id", destino, "delta", +monto));
                d.put("ops", ops);
                m.put("data", d);

                LengthPrefixedCodec.write(s.getOutputStream(), JsonLite.obj(m));
                String resp = LengthPrefixedCodec.read(s.getInputStream());

                if (voteCommit(resp)) {
                    return wrapOk(txId, Map.of("msg", "transfer local OK"));
                }
                return wrapError(txId, "intra-shard failed: " + resp);
            } catch (Exception e) {
                return wrapError(txId, "intra-shard exception: " + e.getMessage());
            }
        }

     
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<Boolean> f1 = pool.submit(() -> sendAndVote(nO, msg("PREPARE", txId, origen, -monto), prepareTimeoutMs));
        Future<Boolean> f2 = pool.submit(() -> sendAndVote(nD, msg("PREPARE", txId, destino, +monto), prepareTimeoutMs));

        boolean v1 = false, v2 = false;
        try {
            v1 = f1.get(prepareTimeoutMs + 200, TimeUnit.MILLISECONDS);
            v2 = f2.get(prepareTimeoutMs + 200, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.err.println(" Timeout o fallo durante PREPARE: " + e.getMessage());
        } finally {
            pool.shutdownNow();
        }

        if (v1 && v2) {
            System.out.printf(" tx=%s PREPARE OK en ambos nodos%n", txId);
            boolean c1 = sendSimple(nO, msg("REPLICATE_APPLY", txId, origen, -monto), commitTimeoutMs);
            boolean c2 = sendSimple(nD, msg("REPLICATE_APPLY", txId, destino, +monto), commitTimeoutMs);
            if (c1 && c2) {
                replicateAsync(pO, txId, new long[]{origen}, new double[]{-monto});
                replicateAsync(pD, txId, new long[]{destino}, new double[]{+monto});
                System.out.printf(" tx=%s COMMIT exitoso%n", txId);
                return wrapOk(txId, Map.of("msg", "transfer OK"));
            } else {
                sendSimple(nO, commitAbort("ABORT", txId), commitTimeoutMs);
                sendSimple(nD, commitAbort("ABORT", txId), commitTimeoutMs);
                System.err.printf(" tx=%s commit failed%n", txId);
                return wrapError(txId, "commit failed");
            }
        } else {
            sendSimple(nO, commitAbort("ABORT", txId), commitTimeoutMs);
            sendSimple(nD, commitAbort("ABORT", txId), commitTimeoutMs);
            System.err.printf(" tx=%s abort por voto negativo%n", txId);
            return wrapError(txId, "vote abort");
        }
    }



    private boolean sendSimple(RoutingTable.Node n, String msg, int timeoutMs) {
        try (Socket s = new Socket(n.host, n.port)) {
            s.setSoTimeout(timeoutMs);
            LengthPrefixedCodec.write(s.getOutputStream(), msg);
            String resp = LengthPrefixedCodec.read(s.getInputStream());
            return voteCommit(resp);
        } catch (Exception e) {
            System.err.printf(" Falla al contactar nodo %s:%d (%s)%n", n.host, n.port, e.getMessage());
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
            System.err.printf(" PREPARE fallido en %s:%d (%s)%n", n.host, n.port, e.getMessage());
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
                        d.put("tx_id", txId + "-" + idx);
                        d.put("id", ids[idx]);
                        d.put("delta", deltas[idx]);
                        m.put("data", d);
                        LengthPrefixedCodec.write(s.getOutputStream(), JsonLite.obj(m));
                        LengthPrefixedCodec.read(s.getInputStream());
                    } catch (Exception ignore) {}
                },"replicate-"+rep.id+"-op"+i).start();
            }
        }
    }



    private static String wrapOk(String txId, Map<String,Object> data) {
        Map<String,Object> r = new LinkedHashMap<>();
        r.put("ok", true);
        r.put("req_id", txId);
        r.put("data", data);
        r.put("error", null);
        return JsonLite.obj(r);
    }

    private static String wrapError(String txId, String errMsg) {
        Map<String,Object> r = new LinkedHashMap<>();
        r.put("ok", false);
        r.put("req_id", txId);
        r.put("data", null);
        r.put("error", errMsg);
        return JsonLite.obj(r);
    }
}
