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

public class ClientHandler implements Runnable {
    private final Socket socket;
    private final RoutingTable routing;
    private final ShardLocator locator;
    private final TwoPC twoPC;

    public ClientHandler(Socket s, RoutingTable routing, ShardLocator locator, TwoPC twoPC) {
        this.socket = s; this.routing = routing; this.locator = locator; this.twoPC = twoPC;
    }

    @Override public void run() {
        try (Socket s = socket) {
            while (true) {
                String req = LengthPrefixedCodec.read(s.getInputStream());
                String type = JsonLite.getString(req, "type");
                if (type == null) { writeErr(s, "bad_request"); continue; }

                switch (type) {
                    case "CONSULTAR_CUENTA": handleConsultar(s, req); break;
                    case "ESTADO_PAGO_PRESTAMO": handlePrestamo(s, req); break;
                    case "TRANSFERIR_CUENTA": handleTransfer(s, req); break;
                    case "CREATE_CUENTA": handleCreateCuenta(s, req); break;
                    case "ARQUEO_CUENTAS": handleArqueo(s); break;
                    default: writeErr(s, "unknown_type:"+type);
                }
            }
        } catch (EOFException e) {
            // cliente cerró
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleConsultar(Socket s, String req) throws IOException {
        Long id = JsonLite.getDataLong(req, "id");
        if (id == null) { writeErr(s,"missing id"); return; }
        int part = locator.shardFor(id);
        RoutingTable.Node p = routing.primaryOf(part);
        try (Socket n = new Socket(p.host, p.port)) {
            LengthPrefixedCodec.write(n.getOutputStream(), reqReplaceType(req, "GET"));
            String resp = LengthPrefixedCodec.read(n.getInputStream());
            LengthPrefixedCodec.write(s.getOutputStream(), resp);
        }
    }

    private void handlePrestamo(Socket s, String req) throws IOException {
        Long id = JsonLite.getDataLong(req, "id");
        if (id == null) { writeErr(s,"missing id"); return; }
        int part = locator.shardFor(id);
        RoutingTable.Node p = routing.primaryOf(part);
        try (Socket n = new Socket(p.host, p.port)) {
            LengthPrefixedCodec.write(n.getOutputStream(), reqReplaceType(req, "GET_LOAN_STATUS"));
            String resp = LengthPrefixedCodec.read(n.getInputStream());
            LengthPrefixedCodec.write(s.getOutputStream(), resp);
        }
    }

    private void handleTransfer(Socket s, String req) throws IOException {
        Long origen = JsonLite.getDataLong(req, "origen");
        Long destino = JsonLite.getDataLong(req, "destino");
        Double monto = JsonLite.getDataDouble(req, "monto");
        String txId = JsonLite.getDataString(req, "tx_id");
        if (origen==null || destino==null || monto==null || txId==null) { writeErr(s,"missing fields"); return; }
        String r = twoPC.transfer(origen, destino, monto, txId);
        LengthPrefixedCodec.write(s.getOutputStream(), r);
    }

    private void handleCreateCuenta(Socket s, String req) throws IOException {
        Long id = JsonLite.getDataLong(req, "id");
        Double saldo = JsonLite.getDataDouble(req, "saldo");
        if (id==null || saldo==null) { writeErr(s,"missing id/saldo"); return; }
        int part = locator.shardFor(id);
        RoutingTable.Node p = routing.primaryOf(part);
        String txId = UUID.randomUUID().toString();

        // Lo implementamos como APPLY_CREATE al primario + replicación asíncrona
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type","APPLY_CREATE");
        m.put("req_id", UUID.randomUUID().toString());
        m.put("client_id", "CENTRAL");
        m.put("ts", System.currentTimeMillis());
        Map<String,Object> d = new LinkedHashMap<>();
        d.put("tx_id", txId);
        d.put("id", id);
        d.put("saldo", saldo);
        m.put("data", d);
        String apply = JsonLite.obj(m);

        try (Socket n = new Socket(p.host, p.port)) {
            LengthPrefixedCodec.write(n.getOutputStream(), apply);
            String resp = LengthPrefixedCodec.read(n.getInputStream());
            // replicamos a réplicas
            for (RoutingTable.Node rep : routing.replicasOf(part)) {
                try (Socket rS = new Socket(rep.host, rep.port)) {
                    Map<String,Object> m2 = new LinkedHashMap<>(m);
                    m2.put("type", "REPLICATE_CREATE");
                    LengthPrefixedCodec.write(rS.getOutputStream(), JsonLite.obj(m2));
                    LengthPrefixedCodec.read(rS.getInputStream());
                } catch (Exception ignore) {}
            }
            LengthPrefixedCodec.write(s.getOutputStream(), resp);
        }
    }

    private void handleArqueo(Socket s) throws IOException {
        // consulta a todos los primarios para sumar saldos (simple)
        double total = 0.0;
        for (int part=0; part<3; part++) {
            RoutingTable.Node p = routing.primaryOf(part);
            try (Socket n = new Socket(p.host, p.port)) {
                Map<String,Object> m = new LinkedHashMap<>();
                m.put("type","SUM_SALDOS");
                m.put("req_id", UUID.randomUUID().toString());
                m.put("client_id","CENTRAL"); m.put("ts",System.currentTimeMillis());
                m.put("data", Collections.emptyMap());
                LengthPrefixedCodec.write(n.getOutputStream(), JsonLite.obj(m));
                String resp = LengthPrefixedCodec.read(n.getInputStream());
                Double sum = JsonLite.getDataDouble(resp, "sum");
                if (sum != null) total += sum;
            } catch (Exception e) { /* best-effort */ }
        }
        Map<String,Object> r = new LinkedHashMap<>();
        r.put("type","OK");
        Map<String,Object> data = new LinkedHashMap<>();
        data.put("total_saldos", total);
        r.put("data", data);
        LengthPrefixedCodec.write(s.getOutputStream(), JsonLite.obj(r));
    }

    private static void writeErr(Socket s, String msg) throws IOException {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type","ERROR"); m.put("msg", msg);
        LengthPrefixedCodec.write(s.getOutputStream(), JsonLite.obj(m));
    }

    private static String reqReplaceType(String json, String newType) {
        // muy simple: reemplazar "type":"..." por el nuevo
        String t = JsonLite.getString(json, "type");
        return json.replace("\"type\":\""+t+"\"", "\"type\":\""+newType+"\"");
    }
}