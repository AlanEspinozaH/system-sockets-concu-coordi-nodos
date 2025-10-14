/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package util;

import java.util.*;

/**
 * JSON sin dependencias:
 * - Soporta objetos con String/Number/Boolean y mapas/arrays simples.
 * - Serialización segura de los tipos anteriores.
 * - Parsing "ligero": para este proyecto, extraemos por clave
 *   o convertimos un objeto/array para nuestras estructuras.
 * No pretende ser RFC-estricto, pero es suficiente y robusto para los mensajes definidos.
 */
public final class JsonLite {

    private JsonLite() {}

    // -------- Serialization --------
    public static String obj(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String,Object> e : map.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append(quote(e.getKey())).append(':').append(val(e.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    public static String arr(List<?> list) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        boolean first = true;
        for (Object o : list) {
            if (!first) sb.append(',');
            first = false;
            sb.append(val(o));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String val(Object v) {
        if (v == null) return "null";
        if (v instanceof String) return quote((String) v);
        if (v instanceof Number || v instanceof Boolean) return String.valueOf(v);
        if (v instanceof Map) return obj((Map<String,Object>) v);
        if (v instanceof List) return arr((List<?>) v);
        return quote(String.valueOf(v));
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\","\\\\").replace("\"","\\\"") + "\"";
    }

    // -------- Very-light parsing (flat paths only) --------

    /** Devuelve valor string de una clave a primer nivel: "type", "client_id", etc. */
    public static String getString(String json, String key) {
        String pat = "\"" + key + "\"";
        int i = json.indexOf(pat);
        if (i < 0) return null;
        int c = json.indexOf(':', i + pat.length());
        if (c < 0) return null;
        int q1 = json.indexOf('"', c + 1);
        if (q1 < 0) return null;
        int q2 = json.indexOf('"', q1 + 1);
        if (q2 < 0) return null;
        return json.substring(q1 + 1, q2).replace("\\\"", "\"").replace("\\\\","\\");
    }

    /** Obtiene double de data.{key} cuando está como "data":{"monto":123.4} */
    public static Double getDataDouble(String json, String key) {
        String sub = getSub(json, "data");
        if (sub == null) return null;
        return getNumber(sub, key);
    }

    /** Obtiene long/int de data.{key}. */
    public static Long getDataLong(String json, String key) {
        String sub = getSub(json, "data");
        if (sub == null) return null;
        Double d = getNumber(sub, key);
        return d == null ? null : d.longValue();
    }

    public static String getDataString(String json, String key) {
        String sub = getSub(json, "data");
        if (sub == null) return null;
        return getString(sub, key);
    }

    // Helpers

    private static String getSub(String json, String key) {
        String pat = "\"" + key + "\"";
        int i = json.indexOf(pat);
        if (i < 0) return null;
        int c = json.indexOf(':', i + pat.length());
        if (c < 0) return null;
        int o = json.indexOf('{', c + 1);
        if (o < 0) return null;
        int depth = 0;
        for (int k = o; k < json.length(); k++) {
            char ch = json.charAt(k);
            if (ch == '{') depth++;
            if (ch == '}') { depth--; if (depth == 0) return json.substring(o, k + 1); }
        }
        return null;
    }

    private static Double getNumber(String json, String key) {
        String pat = "\"" + key + "\"";
        int i = json.indexOf(pat);
        if (i < 0) return null;
        int c = json.indexOf(':', i + pat.length());
        if (c < 0) return null;
        int j = c + 1;
        while (j < json.length() && Character.isWhitespace(json.charAt(j))) j++;
        int k = j;
        while (k < json.length()) {
            char ch = json.charAt(k);
            if ((ch >= '0' && ch <= '9') || ch == '-' || ch == '+' || ch == '.' || ch == 'E' || ch == 'e') {
                k++;
            } else break;
        }
        try {
            return Double.valueOf(json.substring(j, k));
        } catch (Exception e) {
            return null;
        }
    }
}