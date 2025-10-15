/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package nodo;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Almacenamiento simple en memoria + persistencia CSV.
 * Directorio por nodo: data-node-{port}/
 *  - cuentas.csv   (idCuenta,saldo)
 *  - prestamos.csv (idCuenta,deudaPendiente,diasParaVencer)
 *  - movimientos.csv (idMov,idCuenta,concepto,monto)
 */
public class ShardStore {
    private final File baseDir;
    private final ConcurrentHashMap<Long, Double> saldos = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Loan> prestamos = new ConcurrentHashMap<>();
    // añadimos mapa de candados para los locks 
    private final ConcurrentHashMap<Long, ReentrantLock> accountLocks = new ConcurrentHashMap<>();
    private volatile long seqMov = 1;

    public static class Loan { public double deuda; public int dias; Loan(double d,int di){deuda=d;dias=di;} }

    public ShardStore(int port) throws IOException {
        this.baseDir = new File("data-node-" + port);
        if (!baseDir.exists()) baseDir.mkdirs();
        load();
    }

    private File fCuentas() { return new File(baseDir,"cuentas.csv"); }
    private File fPrestamos() { return new File(baseDir,"prestamos.csv"); }
    private File fMovs() { return new File(baseDir,"movimientos.csv"); }

    private void load() throws IOException {
        if (fCuentas().exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(fCuentas(), StandardCharsets.UTF_8))) {
                String line; while ((line = br.readLine()) != null) {
                    if (line.startsWith("idCuenta")) continue;
                    String[] t = line.split(",");
                    if (t.length >= 2) saldos.put(Long.parseLong(t[0]), Double.parseDouble(t[1]));
                }
            }
        }
        if (fPrestamos().exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(fPrestamos(), StandardCharsets.UTF_8))) {
                String line; while ((line = br.readLine()) != null) {
                    if (line.startsWith("idCuenta")) continue;
                    String[] t = line.split(",");
                    if (t.length >= 3) prestamos.put(Long.parseLong(t[0]), new Loan(Double.parseDouble(t[1]), Integer.parseInt(t[2])));
                }
            }
        }
        if (!fMovs().exists()) {
            try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(fMovs(), true), StandardCharsets.UTF_8))) {
                pw.println("idMov,idCuenta,concepto,monto");
            }
        } else {
            // aproximar seqMov
            long c = 1;
            try (BufferedReader br = new BufferedReader(new FileReader(fMovs(), StandardCharsets.UTF_8))) {
                String l; while ((l = br.readLine()) != null) if (!l.startsWith("idMov")) c++;
            }
            seqMov = c;
        }
    }

    // Persistencia simple
    private synchronized void persistCuenta(long id, double saldo) throws IOException {
        saldos.put(id, saldo);
        // reescribir todo (simple)
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(fCuentas()), StandardCharsets.UTF_8))) {
            pw.println("idCuenta,saldo");
            for (Map.Entry<Long,Double> e : saldos.entrySet()) {
                pw.println(e.getKey()+","+e.getValue());
            }
            pw.flush();
            try (FileChannel ch = FileChannel.open(fCuentas().toPath())) { ch.force(true); }
        }
    }

    private synchronized void appendMov(long idCuenta, String concepto, double monto) throws IOException {
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(fMovs(), true), StandardCharsets.UTF_8))) {
            pw.println((seqMov++)+","+idCuenta+","+concepto+","+monto);
            pw.flush();
            try (FileChannel ch = FileChannel.open(fMovs().toPath())) { ch.force(true); }
        }
    }

    private synchronized void upsertPrestamo(long idCuenta, double deuda, int dias) throws IOException {
        prestamos.put(idCuenta, new Loan(deuda, dias));
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(fPrestamos()), StandardCharsets.UTF_8))) {
            pw.println("idCuenta,deudaPendiente,diasParaVencer");
            for (Map.Entry<Long,Loan> e : prestamos.entrySet()) {
                pw.println(e.getKey()+","+e.getValue().deuda+","+e.getValue().dias);
            }
            pw.flush();
            try (FileChannel ch = FileChannel.open(fPrestamos().toPath())) { ch.force(true); }
        }
    }

    // --- API para WorkerHandler ---

    // 💻 Reemplaza el método createCuenta
    public boolean createCuenta(long id, double saldo) throws IOException {
        if (saldos.containsKey(id)) return false; // Verificación rápida sin lock

        ReentrantLock lock = getLockForAccount(id);
        lock.lock();
        try {
        // Doble verificación por si otro hilo la creó mientras esperábamos el lock
            if (saldos.containsKey(id)) return false;

            persistCuenta(id, saldo); // Llama a la versión sin synchronized
            appendMov(id, "Apertura", saldo);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** PREPARE: verifica que el delta no deje saldo negativo (si es débito). */
    public boolean canApply(long id, double delta) {
        double s = saldos.getOrDefault(id, 0.0);
        return !(s + delta < 0.0);
    }

    public void applyDelta(long id, double delta, String concepto) throws IOException {
        double s = saldos.getOrDefault(id, 0.0);
        s += delta;
        persistCuenta(id, s);
        appendMov(id, concepto, delta);
    }

    public double getSaldo(long id) {
        return saldos.getOrDefault(id, 0.0);
    }

    public synchronized String movimientosDelMesCsv(long id) {
        // Para demo: devolvemos últimas 10 líneas del archivo del id (simple)
        // (En una versión completa filtraríamos por mes/fecha)
        List<String> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fMovs(), StandardCharsets.UTF_8))) {
            String l; while ((l = br.readLine()) != null) {
                if (l.startsWith("idMov")) continue;
                String[] t = l.split(",");
                if (t.length >= 4 && Long.parseLong(t[1]) == id) rows.add(l);
            }
        } catch (Exception ignore) {}
        int n = Math.max(0, rows.size()-10);
        StringBuilder sb = new StringBuilder();
        for (int i=n;i<rows.size();i++) {
            String[] t = rows.get(i).split(",");
            sb.append(t[0]).append(",").append(t[2]).append(",").append(t[3]);
            if (i<rows.size()-1) sb.append(";");
        }
        return sb.toString(); // "12,Retiro,500;52,Comision,12.6;..."
    }

    public synchronized String estadoPrestamo(long id) {
        Loan L = prestamos.get(id);
        if (L == null) return "SIN_DEUDA";
        if (L.dias <= 2) return "VENCE_EN_"+L.dias+"_DIAS";
        return "TIENE_DEUDA_"+L.deuda;
    }

    public synchronized double sumSaldos() {
        return saldos.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    // semillas simples para demo de préstamos
    public synchronized void seedPrestamoSiAplica(long id) throws IOException {
        if (!prestamos.containsKey(id) && id % 10 == 0) { // 10% de cuentas
            upsertPrestamo(id, 500.0, 2);
        }
    }

    // Metodo para obtener candados 
    private ReentrantLock getLockForAccount(long accountId) {
    // computeIfAbsent garantiza que solo se cree un candado por ID, de forma atómica.
    return accountLocks.computeIfAbsent(accountId, k -> new ReentrantLock());
    }
}