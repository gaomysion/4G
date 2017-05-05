package com.asiainfo.socket;

public class ConvToByte {
    public static byte[] longToByte(long number) {
        byte[] result = new byte[8];
        result[0] = ((byte) (int) (number >> 56 & 0xFF));
        result[1] = ((byte) (int) (number >> 48 & 0xFF));
        result[2] = ((byte) (int) (number >> 40 & 0xFF));
        result[3] = ((byte) (int) (number >> 32 & 0xFF));
        result[4] = ((byte) (int) (number >> 24 & 0xFF));
        result[5] = ((byte) (int) (number >> 16 & 0xFF));
        result[6] = ((byte) (int) (number >> 8 & 0xFF));
        result[7] = ((byte) (int) (number & 0xFF));

        return result;
    }


    public static double bytes2Double(byte[] b, int off) {
        return Double.longBitsToDouble(byteToLong( b, off));
    }

    public static long byteToLong(byte[] b, int off) {
        long s = 0L;
        long s0 = b[(7 + off)] & 0xFF;
        long s1 = b[(6 + off)] & 0xFF;
        long s2 = b[(5 + off)] & 0xFF;
        long s3 = b[(4 + off)] & 0xFF;
        long s4 = b[(3 + off)] & 0xFF;
        long s5 = b[(2 + off)] & 0xFF;
        long s6 = b[(1 + off)] & 0xFF;
        long s7 = b[off] & 0xFF;

        s1 <<= 8;
        s2 <<= 16;
        s3 <<= 24;
        s4 <<= 32;
        s5 <<= 40;
        s6 <<= 48;
        s7 <<= 56;
        s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
        return s;
    }

    public static byte[] intToByte(int number) {
        byte[] result = new byte[4];
        result[0] = ((byte) (number >> 24 & 0xFF));
        result[1] = ((byte) (number >> 16 & 0xFF));
        result[2] = ((byte) (number >> 8 & 0xFF));
        result[3] = ((byte) (number & 0xFF));

        return result;
    }

    public static int byteToInt(byte[] b, int off) {
        int s = 0;
        int s0 = b[(3 + off)] & 0xFF;
        int s1 = b[(2 + off)] & 0xFF;
        int s2 = b[(1 + off)] & 0xFF;
        int s3 = b[off] & 0xFF;
        s3 <<= 24;
        s2 <<= 16;
        s1 <<= 8;
        s = s0 | s1 | s2 | s3;
        return s;
    }

    public static byte[] shortToByte(short number) {
        byte[] result = new byte[2];
        result[0] = ((byte) (number >> 8 & 0xFF));
        result[1] = ((byte) (number & 0xFF));

        return result;
    }

    public static short byteToShort(byte[] b, int off) {
        short s = 0;
        short s0 = (short) (b[(1 + off)] & 0xFF);
        short s1 = (short) (b[off] & 0xFF);
        s1 = (short) (s1 << 8);
        s = (short) (s0 | s1);
        return s;
    }

    public static int byteToUnsignedShort(byte[] b, int off) {
        short s = 0;
        short s0 = (short) (b[(1 + off)] & 0xFF);
        short s1 = (short) (b[off] & 0xFF);
        s1 = (short) (s1 << 8);
        s = (short) (s0 | s1);
        return s & 0xFFFF;
    }

    public static int byteToUnsignedByte(byte[] b, int off) {
        short s1 = (short) (b[off] & 0xFF);
        return s1 & 0xFFFF;
    }

    public static String decodeTBCD(byte[] c, int start, int end, boolean is_del_f) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            String tmp = String.format("%02x", c[i]);
            sb.append(tmp.charAt(1));
            sb.append(tmp.charAt(0));
        }
        if (is_del_f) {
            int str_end = sb.indexOf("f");
            if (str_end > 0) sb.setLength(str_end);
        }
        return sb.toString();
    }

    public static String getHexString(byte[] c, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(String.format("%02x", c[i]));
        }
        return sb.toString();
    }

    public static long byteToUnsignedInt(byte[] b, int off) {
        int s = 0;
        int s0 = b[(3 + off)] & 0xFF;
        int s1 = b[(2 + off)] & 0xFF;
        int s2 = b[(1 + off)] & 0xFF;
        int s3 = b[off] & 0xFF;
        s3 <<= 24;
        s2 <<= 16;
        s1 <<= 8;
        s = s0 | s1 | s2 | s3;
        return s & 0xFFFFFFFFL;
    }

    public static String getIpv4(byte[] b, int off) {
        String ip = "";
        int s0 = b[(3 + off)] & 0xFF;
        int s1 = b[(2 + off)] & 0xFF;
        int s2 = b[(1 + off)] & 0xFF;
        int s3 = b[off] & 0xFF;

        ip = s0 + "." + s1 + "." + s2 + "." + s3;
        return ip;
    }

    public static String getIpv6(byte[] b, int off) {
        String ip = "";
        for (int i = off; i < off + 16; i++) {
            ip += String.format("%02X", b[i]);
            if (i != off && (i - off) % 2 == 1 && i != off + 15) {
                ip += ":";
            }
        }
        return ip;
    }

    public static String getIp(byte[] b, int off) {

        int start = 0;
        StringBuilder sb = new StringBuilder();
        String tmp = String.format("%02X", b[off]);
        if ("FF".equals(tmp)) {
            start = off + 12;
        } else {
            start = off;
        }
        for (int i = start; i < off + 16; i++) {
            if (start != off) {
                sb.append(b[i] & 0xff);
                if (i != off + 15)
                    sb.append(".");
            }
            if (start == off) {
                sb.append(String.format("%02X", b[i]));
                if (i != start && (i - start) % 2 == 1 && i != off + 15)
                    sb.append(":");
            }
        }

        return sb.toString();
    }

    public static String getHexString2(byte[] c, int start, int end) {
        StringBuilder sb = new StringBuilder();

        for (int i = start; i < end; i++) {
            sb.append(String.format("%02x", c[i]));
        }
        if ("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".equals(sb.toString())) {
            return sb.toString();
        } else {
            return new String(c, start, end - start);
        }
    }
}
