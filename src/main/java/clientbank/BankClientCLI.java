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

public class BankClientCLI {

    private final String host;
    private final int port;

    public BankClientCLI(String host, int port) { this.host = host; this.port = port; }

    private String send(Map<String,Object> msg) throws IOException {
        try (Socket s = new Socket(host, port)) {
            LengthPrefixedCodec.write(s.getOutputStream(), JsonLite.obj(msg));
            return LengthPrefixedCodec.read(s.getInputStream());
        }
    }

    private static Map<String,Object> wrap(String type, Map<String,Object> data) {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("req_id", UUID.randomUUID().toString());
        m.put("client_id", "CLI-BANK");
        m.put("ts", System.currentTimeMillis());
        m.put("data", data == null ? Collections.emptyMap() : data);
        return m;
    }

    private void bootstrap(int n, double saldo) throws Exception {
        System.out.println("Creando " + n + " cuentas iniciales...");
        for (int i = 1; i <= n; i++) {
            Map<String,Object> d = new LinkedHashMap<>();
            d.put("tx_id", UUID.randomUUID().toString());
            d.put("id", (long)i);
            d.put("saldo", saldo);
            String resp = send(wrap("CREATE_CUENTA", d));
            if (i % 1000 == 0) System.out.println("  -> " + i + " (" + resp + ")");
        }
        System.out.println("Bootstrap listo.");
    }
// 💻 COPIA Y PEGA ESTE CÓDIGO COMPLETO REEMPLAZANDO EL MÉTODO repl() ORIGINAL

    private void repl() throws Exception {
        // Mensaje de bienvenida mejorado
        System.out.println("Cliente de Banco CLI. Escriba 'help' o '?' para ver los comandos.");
        System.out.println("Ctrl+D para salir.");
    
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
        
            String[] t = line.split("\\s+");
            String cmd = t[0].toUpperCase(Locale.ROOT);
        
            try {
                switch (cmd) {
                    case "HELP":
                    case "?":
                        System.out.println("Comandos disponibles:");
                        System.out.println("  CONSULTAR_CUENTA <id_cuenta>");
                        System.out.println("  ESTADO_PAGO_PRESTAMO <id_cuenta>");
                        System.out.println("  TRANSFERIR_CUENTA <id_origen> <id_destino> <monto>");
                        System.out.println("  ARQUEO_CUENTAS");
                        System.out.println("  HELP o ? para mostrar esta ayuda.");
                        break;

                    case "CONSULTAR_CUENTA": {
                        // --- VALIDACIÓN DE ARGUMENTOS ---
                        if (t.length < 2) {
                            System.out.println("Error: Falta el ID de la cuenta. Uso: CONSULTAR_CUENTA <id>");
                            break;
                        }
                        long id = Long.parseLong(t[1]);
                        Map<String,Object> d = new LinkedHashMap<>();
                        d.put("id", id);
                        String r = send(wrap("CONSULTAR_CUENTA", d));
                        System.out.println(r);
                        break;
                    }
                    case "ESTADO_PAGO_PRESTAMO": {
                        // --- VALIDACIÓN DE ARGUMENTOS ---
                        if (t.length < 2) {
                            System.out.println("Error: Falta el ID de la cuenta. Uso: ESTADO_PAGO_PRESTAMO <id>");
                            break;
                        }
                        long id = Long.parseLong(t[1]);
                        Map<String,Object> d = new LinkedHashMap<>();
                        d.put("id", id);
                        String r = send(wrap("ESTADO_PAGO_PRESTAMO", d));
                        System.out.println(r);
                        break;
                    }
                    case "TRANSFERIR_CUENTA": {
                        // --- VALIDACIÓN DE ARGUMENTOS ---
                        if (t.length < 4) {
                            System.out.println("Error: Faltan argumentos. Uso: TRANSFERIR_CUENTA <origen> <destino> <monto>");
                            break;
                        }
                        long o = Long.parseLong(t[1]);
                        long de = Long.parseLong(t[2]);
                        double m = Double.parseDouble(t[3]);
                        Map<String,Object> d = new LinkedHashMap<>();
                        d.put("origen", o);
                        d.put("destino", de);
                        d.put("monto", m);
                        d.put("tx_id", UUID.randomUUID().toString());
                        String r = send(wrap("TRANSFERIR_CUENTA", d));
                        System.out.println(r);
                        break;
                    }
                    case "ARQUEO_CUENTAS": {
                        String r = send(wrap("ARQUEO_CUENTAS", Collections.emptyMap()));
                        System.out.println(r);
                        break;
                    }
                    default:
                        System.out.println("Comando no reconocido. Escriba 'help' para ver la lista de comandos.");
                }
            } catch (NumberFormatException e) {
                System.out.println("Error: El ID o el monto deben ser números válidos.");
            } catch (Exception e) {
                System.out.println("Error procesando la solicitud: " + e.getMessage());
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 6002;
        boolean doBootstrap = false;
        int n = 10000;
        double saldo = 1000.0;

        for (int i=0; i<args.length; i++) {
            switch (args[i]) {
                case "--server":
                    String[] hp = args[++i].split(":");
                    host = hp[0]; port = Integer.parseInt(hp[1]); break;
                case "--bootstrap":
                    doBootstrap = true; break;
                case "--n":
                    n = Integer.parseInt(args[++i]); break;
                case "--saldo":
                    saldo = Double.parseDouble(args[++i]); break;
            }
        }

        BankClientCLI cli = new BankClientCLI(host, port);
        if (doBootstrap) cli.bootstrap(n, saldo);
        cli.repl();
    }
}
