package com.qwazr.jdbc.cache;

import java.nio.charset.StandardCharsets;
import java.util.Formatter;

public class DatatypeConverter {
    public static String printHexBinary(byte[] data) {
        Formatter formatter = new Formatter();
        for (byte b : data) {
            formatter.format("%02x", b);
        }
        String hex = formatter.toString();
        formatter.close();
        return hex;
    }

    public static byte[] parseHexBinary(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    public static String printBase64Binary(byte[] data) {
        return java.util.Base64.getEncoder().encodeToString(data);
    }

    public static byte[] parseBase64Binary(String s) {
        return java.util.Base64.getDecoder().decode(s);
    }

    public static String printHexBinary(String s) {
        return printHexBinary(s.getBytes(StandardCharsets.UTF_8));
    }

    public static String parseString(String s) {
        return new String(parseHexBinary(s), StandardCharsets.UTF_8);
    }
}