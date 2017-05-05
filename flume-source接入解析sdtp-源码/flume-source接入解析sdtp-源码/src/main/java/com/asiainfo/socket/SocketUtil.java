package com.asiainfo.socket;

/**
 * Created by ai on 2016/4/20.
 */
public class SocketUtil {
    //两位字节数组转int
    //一个byte型整数在内存中占8位，也就是一个字节. 表数范围:-128 --127 . （字符类型char 2个字节）
    //0xFF 0xff = 1111 1111 = 2^8 - 1 = 255
    //<<8是左移，值会放大256倍，左边补8个0 eg.0x1101 处理后就是 0x1200
    public static int bytesToInt(byte[] bytes) {
        int num = bytes[1] & 0xFF;
        num |= ((bytes[0] << 8) & 0xFF00);
        return num;
    }
    //四位字节数组转int
    public static int bytes2Int(byte[] bytes) {
        int number = bytes[0] & 0xFF;
        // "|="按位或赋值。
        number |= ((bytes[1] << 8) & 0xFF00);
        number |= ((bytes[2] << 16) & 0xFF0000);
        number |= ((bytes[3] << 24) & 0xFF000000);
        return number;
    }
    public static byte[] int2Byte(int number) {
        byte[] abyte = new byte[4];
        // "&" 与（AND），对两个整型操作数中对应位执行布尔代数，两个位都为1时输出1，否则0。
        abyte[0] = (byte) (0xff & number);
        // ">>"右移位，若为正数则高位补0，若为负数则高位补1
        abyte[1] = (byte) ((0xff00 & number) >> 8);
        abyte[2] = (byte) ((0xff0000 & number) >> 16);
        abyte[3] = (byte) ((0xff000000 & number) >> 24);
        return abyte;
    }
}
