/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/** @author alulo */
package util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/** Framing: 4 bytes big-endian (longitud) + payload UTF-8 (NDJSON). */
public final class LengthPrefixedCodec {
    private LengthPrefixedCodec() {}

    public static void write(OutputStream out, String payload) throws IOException {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(data.length);
        out.write(bb.array());
        out.write(data);
        out.flush();
    }

    public static String read(InputStream in) throws IOException {
        byte[] lenBuf = in.readNBytes(4);
        if (lenBuf.length < 4) throw new EOFException("stream closed");
        int len = ByteBuffer.wrap(lenBuf).order(ByteOrder.BIG_ENDIAN).getInt();
        byte[] data = in.readNBytes(len);
        if (data.length < len) throw new EOFException("short read");
        return new String(data, StandardCharsets.UTF_8);
    }
}