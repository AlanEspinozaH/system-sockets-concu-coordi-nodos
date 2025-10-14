/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package clientchat; 

import util.LengthPrefixedCodec;
import util.JsonLite;

import javax.swing.*;
import java.awt.*;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;

public class ChatClient {

    private final String host;
    private final int port;

    public ChatClient(String h,int p){host=h;port=p;}

    private String send(String type, Map<String,Object> data) throws Exception {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("req_id", UUID.randomUUID().toString());
        m.put("client_id", "CHAT");
        m.put("ts", System.currentTimeMillis());
        m.put("data", data);
        try (Socket s = new Socket(host, port)) {
            OutputStream out = s.getOutputStream(); InputStream in = s.getInputStream();
            LengthPrefixedCodec.write(out, JsonLite.obj(m));
            return LengthPrefixedCodec.read(in);
        }
    }

    private void ui() {
        JFrame f = new JFrame("Cliente Chat");
        JTextArea area = new JTextArea(16, 60);
        area.setEditable(false);
        JTextField input = new JTextField();
        JButton btn = new JButton("Enviar");

        JPanel p = new JPanel(new BorderLayout());
        p.add(new JScrollPane(area), BorderLayout.CENTER);
        JPanel south = new JPanel(new BorderLayout());
        south.add(input, BorderLayout.CENTER);
        south.add(btn, BorderLayout.EAST);
        p.add(south, BorderLayout.SOUTH);

        Runnable sendAction = () -> {
            try {
                String line = input.getText().trim();
                input.setText("");
                if (line.isEmpty()) return;
                area.append("> " + line + "\n");

                String[] t = line.split("\\s+");
                String cmd = t[0].toLowerCase(Locale.ROOT);
                String resp;

                if (cmd.equals("mov") || cmd.equals("saldo") || cmd.equals("consulta")) {
                    long id = Long.parseLong(t[1]);
                    Map<String,Object> d = new LinkedHashMap<>(); d.put("id", id);
                    resp = send("CONSULTAR_CUENTA", d);
                } else if (cmd.equals("deuda")) {
                    long id = Long.parseLong(t[1]);
                    Map<String,Object> d = new LinkedHashMap<>(); d.put("id", id);
                    resp = send("ESTADO_PAGO_PRESTAMO", d);
                } else {
                    resp = "Comandos: mov <id> | saldo <id> | consulta <id> | deuda <id>";
                }
                area.append(resp + "\n");
            } catch (Exception ex) {
                area.append("Error: " + ex.getMessage() + "\n");
            }
        };

        btn.addActionListener(e -> sendAction.run());
        input.addActionListener(e -> sendAction.run());

        f.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        f.setContentPane(p);
        f.pack();
        f.setLocationRelativeTo(null);
        f.setVisible(true);
    }

    public static void main(String[] args) {
    String host = "localhost"; 
    int port = 6001;
    //       Antes fallaba la compilación porque la lambda de invokeLater
    //       intentaba capturar variables locales NO "efectivamente finales".
    for (int i = 0; i < args.length; i++) {
        if ("--server".equals(args[i])) {
            String[] hp = args[++i].split(":");
            host = hp[0];
            port = Integer.parseInt(hp[1]);
        }
    }

    // Copias finales para la lambda: las lambdas solo capturan variables locales
    //       'final o efectivamente final'. Como 'host'/'port' se reasignaron,
    //       dejamos fHost/fPort como copias inmutables para la lambda.
    final String fHost = host;
    final int fPort = port;
    //invokeLater garantiza ejecutar 'ui()' en el EDT (buena práctica).
    SwingUtilities.invokeLater(() -> new ChatClient(fHost, fPort).ui());
    }

}
