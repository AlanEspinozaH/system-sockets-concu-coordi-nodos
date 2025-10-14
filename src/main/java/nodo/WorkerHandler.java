/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package nodo;

import util.LengthPrefixedCodec;
import util.JsonLite;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class WorkerHandler implements Runnable {
    private final Socket s;
    private final ShardStore store;
    private final TxLog txlog;

    public WorkerHandler(Socket s, ShardStore store, TxLog txlog) { 
        this.s = s; 
        this.store = store; 
        this.txlog = txlog; }

    @Override public void run() {
        try (Socket socket = s) {
            while (true) {
                String req = LengthPrefixedCodec.read(socket.getInputStream());
                String type = JsonLite.getString(req, "type");
                if (type == null) { writeErr(socket,"bad_request"); continue; }

                switch (type) {
                    case "GET":                handleGet(socket, req); break;
                    case "GET_LOAN_STATUS":    handleLoanStatus(socket, req); break;
                    case "PREPARE":            handlePrepare(socket, req); break;
                    case "COMMIT":             handleCommit(socket, req); break;
                    case "ABORT":              handleAbort(socket, req); break;
                    case "REPLICATE_APPLY":    handleReplicateApply(socket, req); break;
                    case "APPLY_TRANSFER_LOCAL": handleApplyLocal(socket, req); break;
                    case "APPLY_CREATE":       handleCreate(socket, req, false); break;
                    case "REPLICATE_CREATE":   handleCreate(socket, req, true); break;
                    case "SUM_SALDOS":         handleSum(socket); break;
                    default:                   writeErr(socket,"unknown:"+type);
                }
            }
        } catch (EOFException eof) {
            // peer cerró
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleGet(Socket so, String req) throws IOException {
        Long id = JsonLite.getDataLong(req, "id");
        double saldo = store.getSaldo(id);
        String movs = store.movimientosDelMesCsv(id);
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type","OK");
        Map<String,Object> d = new LinkedHashMap<>();
        d.put("saldo", saldo);
        d.put("movs_csv", movs);
        m.put("data", d);
        LengthPrefixedCodec.write(so.getOutputStream(), JsonLite.obj(m));
    }

    private void handleLoanStatus(Socket so, String req) throws IOException {
        Long id = JsonLite.getDataLong(req, "id");
        String st = store.estadoPrestamo(id);
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type","OK");
        Map<String,Object> d = new LinkedHashMap<>();
        d.put("estado", st);
        m.put("data", d);
        LengthPrefixedCodec.write(so.getOutputStream(), JsonLite.obj(m));
    }

    private void handlePrepare(Socket so, String req) throws IOException {
        String tx = JsonLite.getDataString(req, "tx_id");
        if (txlog.seen(tx)) { voteCommit(so); return; }
        Long id = JsonLite.getDataLong(req, "id");
        Double delta = JsonLite.getDataDouble(req, "delta");
        boolean ok = store.canApply(id, delta);
        if (ok) { voteCommit(so); } else { voteAbort(so, "saldo_insuficiente"); }
    }

    private void handleCommit(Socket so, String req) throws IOException {
        String tx = JsonLite.getDataString(req, "tx_id");
        if (txlog.seen(tx)) { ok(so); return; }
        // El Central luego envía REPLICATE_APPLY con (id,delta) concretos.
        txlog.put(tx, "COMMIT");
        ok(so);
    }

    private void handleAbort(Socket so, String req) throws IOException {
        String tx = JsonLite.getDataString(req, "tx_id");
        txlog.put(tx, "ABORT");
        ok(so);
    }

    private void handleReplicateApply(Socket so, String req) throws IOException {
        String tx = JsonLite.getDataString(req, "tx_id");
        if (txlog.seen(tx)) { ok(so); return; }
        Long id = JsonLite.getDataLong(req, "id");
        Double delta = JsonLite.getDataDouble(req, "delta");
        if (id != null && delta != null) {
            store.applyDelta(id, delta, "Transferencia");
            store.seedPrestamoSiAplica(id);
        }
        txlog.put(tx, "COMMIT");
        ok(so);
    }

    private void handleApplyLocal(Socket so, String req) throws IOException {
        String tx = JsonLite.getDataString(req, "tx_id");
        if (txlog.seen(tx)) { ok(so); return; }
        Long id = JsonLite.getDataLong(req, "id");
        Double delta = JsonLite.getDataDouble(req, "delta");
        if (!store.canApply(id, delta)) { writeErr(so,"saldo_insuficiente"); return; }
        store.applyDelta(id, delta, "Transferencia");
        txlog.put(tx, "COMMIT");
        ok(so);
    }

    private void handleCreate(Socket so, String req, boolean replica) throws IOException {
        String tx = JsonLite.getDataString(req, "tx_id");
        if (txlog.seen(tx)) { ok(so); return; }
        Long id = JsonLite.getDataLong(req, "id");
        Double saldo = JsonLite.getDataDouble(req, "saldo");
        boolean created = store.createCuenta(id, saldo);
        if (created) { store.seedPrestamoSiAplica(id); }
        txlog.put(tx, "COMMIT");
        ok(so);
    }

    private void handleSum(Socket so) throws IOException {
        double sum = store.sumSaldos();
        Map<String,Object> r = new LinkedHashMap<>();
        r.put("type","OK");
        Map<String,Object> d = new LinkedHashMap<>();
        d.put("sum", sum);
        r.put("data", d);
        LengthPrefixedCodec.write(so.getOutputStream(), JsonLite.obj(r));
    }

    // ---- helpers de respuesta ----
    private static void ok(Socket so) throws IOException {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type","OK");
        m.put("data", Collections.emptyMap());
        LengthPrefixedCodec.write(so.getOutputStream(), JsonLite.obj(m));
    }

    private static void voteCommit(Socket so) throws IOException {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type","VOTE_COMMIT");
        m.put("data", Collections.emptyMap());
        LengthPrefixedCodec.write(so.getOutputStream(), JsonLite.obj(m));
    }

    private static void voteAbort(Socket so, String reason) throws IOException {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type","VOTE_ABORT");
        Map<String,Object> d = new LinkedHashMap<>();
        d.put("reason", reason);
        m.put("data", d);
        LengthPrefixedCodec.write(so.getOutputStream(), JsonLite.obj(m));
    }

    private static void writeErr(Socket so, String msg) throws IOException {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type","ERROR");
        Map<String,Object> d = new LinkedHashMap<>();
        d.put("msg", msg);
        m.put("data", d);
        LengthPrefixedCodec.write(so.getOutputStream(), JsonLite.obj(m));
    }
}
