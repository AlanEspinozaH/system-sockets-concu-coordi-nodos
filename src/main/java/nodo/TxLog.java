// TxLog.java
package nodo;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class TxLog implements Closeable {
    private final ConcurrentHashMap<String, String> log = new ConcurrentHashMap<>();
    private final PrintWriter writer;

    public TxLog(int port) throws IOException {
        File baseDir = new File("data-node-" + port);
        if (!baseDir.exists()) baseDir.mkdirs();
        File logFile = new File(baseDir, "tx.log");

        // Cargar transacciones existentes al iniciar
        if (logFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2) {
                        log.put(parts[0], parts[1]);
                    }
                }
            }
        }

        // Abrir el archivo en modo 'append'
        this.writer = new PrintWriter(new BufferedWriter(new FileWriter(logFile, StandardCharsets.UTF_8, true)));
    }

    public boolean seen(String txId) {
        return log.containsKey(txId);
    }

    public void put(String txId, String state) {
        log.put(txId, state);
        writer.println(txId + "," + state);
        writer.flush(); // Asegurar que se escriba a disco inmediatamente
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}