
package util;

import java.util.*;


public final class JsonLite {

    private JsonLite() {}

    
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

    
    public static Double getDataDouble(String json, String key) {
        String sub = getSub(json, "data");
        if (sub == null) return null;
        return getNumber(sub, key);
    }

   
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
            if (ch == '}') {
                depth--;
                if (depth == 0) return json.substring(o, k + 1);
            }
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

    
    public static Object parse(String json) {
        if (json == null || json.isEmpty()) return null;
        json = json.trim();
        if (json.startsWith("{") && json.endsWith("}")) {
            Map<String,Object> map = new LinkedHashMap<>();
            String inner = json.substring(1, json.length()-1);
            String[] parts = inner.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); 
            for (String part : parts) {
                String[] kv = part.split(":", 2);
                if (kv.length != 2) continue;
                String key = kv[0].trim().replaceAll("^\"|\"$", "");
                String val = kv[1].trim();
                if (val.startsWith("\"")) {
                    map.put(key, val.replaceAll("^\"|\"$", ""));
                } else if (val.equals("true") || val.equals("false")) {
                    map.put(key, Boolean.valueOf(val));
                } else if (val.equals("null")) {
                    map.put(key, null);
                } else {
                    try {
                        map.put(key, Double.valueOf(val));
                    } catch (Exception e) {
                        map.put(key, val);
                    }
                }
            }
            return map;
        } else if (json.startsWith("[") && json.endsWith("]")) {
            return Arrays.asList(json.substring(1, json.length()-1).split(","));
        } else {
            return json;
        }
    }

   
    public static List<Map<String, Object>> getListOfMaps(String json, String key1, String key2) {
        try {
            String dataJson = getSub(json, key1);
            if (dataJson == null) return null;

            String opsPat = "\"" + key2 + "\":[";
            int start = dataJson.indexOf(opsPat);
            if (start < 0) return null;
            start += opsPat.length();

            int end = dataJson.indexOf("]", start);
            if (end < 0) return null;

            String opsContent = dataJson.substring(start, end);
            List<Map<String, Object>> result = new ArrayList<>();
            String[] objects = opsContent.split("\\},\\{");

            for (String objStr : objects) {
                Map<String, Object> map = new LinkedHashMap<>();
                String cleanObj = objStr.replace("{", "").replace("}", "").trim();
                String[] pairs = cleanObj.split(",");
                for (String pair : pairs) {
                    String[] kv = pair.split(":");
                    String key = kv[0].replace("\"", "").trim();
                    String value = kv[1].trim();
                    if (value.contains("\"")) {
                        map.put(key, value.replace("\"", ""));
                    } else {
                        map.put(key, Double.parseDouble(value));
                    }
                }
                result.add(map);
            }
            return result;
        } catch (Exception e) {
            return null; 
        }
    }
}
