package central;

import util.LengthPrefixedCodec;
import util.JsonLite;

import java.io.*;
import java.net.Socket;
import java.util.*;


public class ClientHandler implements Runnable {

    private final Socket socket;
    private final RoutingTable routing;
    private final ShardLocator locator;
    private final TwoPC twoPC;

    public ClientHandler(Socket s, RoutingTable routing, ShardLocator locator, TwoPC twoPC) {
        this.socket = s;
        this.routing = routing;
        this.locator = locator;
        this.twoPC = twoPC;
    }

    @Override
    public void run() {
        try (Socket s = socket) {
            while (true) {
                String req = LengthPrefixedCodec.read(s.getInputStream());
                if (req == null || req.isEmpty()) break;

                
                String type = JsonLite.getString(req, "type");
                String reqId = JsonLite.getString(req, "req_id");
                String clientId = JsonLite.getString(req, "client_id");

                if (type == null || reqId == null) {
                    writeError(s, reqId, "bad_request: missing type/req_id");
                    continue;
                }

                System.out.printf("[%s] Req %s de %s%n", type, reqId, clientId);

                try {
                    switch (type) {
                        case "CONSULTAR_CUENTA" -> handleConsultar(s, req, reqId);
                        case "ESTADO_PAGO_PRESTAMO" -> handlePrestamo(s, req, reqId);
                        case "TRANSFERIR_CUENTA" -> handleTransfer(s, req, reqId);
                        case "CREATE_CUENTA" -> handleCreateCuenta(s, req, reqId);
                        case "ARQUEO_CUENTAS" -> handleArqueo(s, reqId);
                        default -> writeError(s, reqId, "unknown_type:" + type);
                    }
                } catch (IOException ioe) {
                    System.err.println("Error procesando req_id=" + reqId + ": " + ioe.getMessage());
                    writeError(s, reqId, "io_exception: " + ioe.getMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                    writeError(s, reqId, "internal_error: " + e.getMessage());
                }
            }
        } catch (EOFException e) {
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   

    private void handleConsultar(Socket s, String req, String reqId) throws IOException {
        Long id = JsonLite.getDataLong(req, "id");
        if (id == null) {
            writeError(s, reqId, "missing id");
            return;
        }

        int part = locator.shardFor(id);
        RoutingTable.Node p = routing.primaryOf(part);

        try (Socket n = new Socket(p.host, p.port)) {
            String forward = reqReplaceType(req, "GET");
            LengthPrefixedCodec.write(n.getOutputStream(), forward);
            String resp = LengthPrefixedCodec.read(n.getInputStream());
            LengthPrefixedCodec.write(s.getOutputStream(), wrapOk(reqId, resp));
        } catch (IOException e) {
            writeError(s, reqId, "node_unreachable: " + p.id);
        }
    }

    private void handlePrestamo(Socket s, String req, String reqId) throws IOException {
        Long id = JsonLite.getDataLong(req, "id");
        if (id == null) {
            writeError(s, reqId, "missing id");
            return;
        }
        int part = locator.shardFor(id);
        RoutingTable.Node p = routing.primaryOf(part);
        try (Socket n = new Socket(p.host, p.port)) {
            String forward = reqReplaceType(req, "GET_LOAN_STATUS");
            LengthPrefixedCodec.write(n.getOutputStream(), forward);
            String resp = LengthPrefixedCodec.read(n.getInputStream());
            LengthPrefixedCodec.write(s.getOutputStream(), wrapOk(reqId, resp));
        } catch (IOException e) {
            writeError(s, reqId, "node_unreachable: " + p.id);
        }
    }

    private void handleTransfer(Socket s, String req, String reqId) throws IOException {
        Long origen = JsonLite.getDataLong(req, "origen");
        Long destino = JsonLite.getDataLong(req, "destino");
        Double monto = JsonLite.getDataDouble(req, "monto");
        String txId = JsonLite.getDataString(req, "tx_id");

        if (origen == null || destino == null || monto == null) {
            writeError(s, reqId, "missing fields");
            return;
        }
        if (txId == null) txId = UUID.randomUUID().toString();

        String result = twoPC.transfer(origen, destino, monto, txId);
        LengthPrefixedCodec.write(s.getOutputStream(), wrapOk(reqId, result));
    }

    private void handleCreateCuenta(Socket s, String req, String reqId) throws IOException {
        Long id = JsonLite.getDataLong(req, "id");
        Double saldo = JsonLite.getDataDouble(req, "saldo");

        if (id == null || saldo == null) {
            writeError(s, reqId, "missing id/saldo");
            return;
        }

        int part = locator.shardFor(id);
        RoutingTable.Node primary = routing.primaryOf(part);
        String txId = UUID.randomUUID().toString();

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "APPLY_CREATE");
        m.put("req_id", reqId);
        m.put("client_id", "CENTRAL");
        m.put("ts", System.currentTimeMillis());

        Map<String, Object> d = new LinkedHashMap<>();
        d.put("tx_id", txId);
        d.put("id", id);
        d.put("saldo", saldo);
        m.put("data", d);

        String apply = JsonLite.obj(m);

        try (Socket n = new Socket(primary.host, primary.port)) {
            LengthPrefixedCodec.write(n.getOutputStream(), apply);
            String resp = LengthPrefixedCodec.read(n.getInputStream());

            
            for (RoutingTable.Node rep : routing.replicasOf(part)) {
                try (Socket rS = new Socket(rep.host, rep.port)) {
                    m.put("type", "REPLICATE_CREATE");
                    LengthPrefixedCodec.write(rS.getOutputStream(), JsonLite.obj(m));
                    LengthPrefixedCodec.read(rS.getInputStream());
                } catch (Exception ignored) {}
            }

            LengthPrefixedCodec.write(s.getOutputStream(), wrapOk(reqId, resp));
        } catch (IOException e) {
            writeError(s, reqId, "node_unreachable: " + primary.id);
        }
    }

    private void handleArqueo(Socket s, String reqId) throws IOException {
        double total = 0.0;
        RoutingTable.Node p = routing.primaryOf(0);

        try (Socket n = new Socket(p.host, p.port)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("type", "SUM_SALDOS");
            m.put("req_id", reqId);
            m.put("client_id", "CENTRAL");
            m.put("ts", System.currentTimeMillis());
            m.put("data", Collections.emptyMap());

            LengthPrefixedCodec.write(n.getOutputStream(), JsonLite.obj(m));
            String resp = LengthPrefixedCodec.read(n.getInputStream());
            Double sum = JsonLite.getDataDouble(resp, "sum");
            if (sum != null) total = sum;
        } catch (Exception e) {
            System.err.println("Error al contactar nodo primario para arqueo: " + e.getMessage());
        }

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("total_saldos", total);
        LengthPrefixedCodec.write(s.getOutputStream(), wrapOk(reqId, JsonLite.obj(data)));
    }

   

    private static void writeError(Socket s, String reqId, String msg) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", false);
        m.put("req_id", reqId != null ? reqId : UUID.randomUUID().toString());
        m.put("data", null);
        m.put("error", msg);
        LengthPrefixedCodec.write(s.getOutputStream(), JsonLite.obj(m));
    }

    private static String wrapOk(String reqId, String payload) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ok", true);
        m.put("req_id", reqId);
        try {
            Object parsed = JsonLite.parse(payload);
            m.put("data", parsed);
        } catch (Exception e) {
            m.put("data", payload);
        }
        m.put("error", null);
        return JsonLite.obj(m);
    }

    private static String reqReplaceType(String json, String newType) {
        String t = JsonLite.getString(json, "type");
        if (t == null) return json;
        return json.replace("\"type\":\"" + t + "\"", "\"type\":\"" + newType + "\"");
    }
}
