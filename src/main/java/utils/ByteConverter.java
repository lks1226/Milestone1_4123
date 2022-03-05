package utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

public class ByteConverter {
    public static byte[] toByte(float f) {
        return ByteBuffer.allocate(4).putFloat(f).array();
    }

    public static byte[] toByte(int f) {
        return ByteBuffer.allocate(4).putInt(f).array();
    }

    public static byte[] toByte(String s) {
        return (s + "\0").getBytes();
    }

    public static ArrayList<Float> bytesToFloat(byte[] bytes) {
        ArrayList<Float> floatArr = new ArrayList<Float>();
        for (int i = 0; i < bytes.length; i += 4) {
            float f = ByteBuffer.wrap(Arrays.copyOfRange(bytes, i, i + 4)).getFloat();
            floatArr.add(Math.round(f * 100) / 100.0F);
        }
        return floatArr;
    }

    public static ArrayList<Integer> bytesToInt(byte[] bytes) {
        ArrayList<Integer> intArr = new ArrayList<Integer>();
        for (int j = 0; j < bytes.length; j += 4) {

            Integer i = ByteBuffer.wrap(Arrays.copyOfRange(bytes, j, j + 4)).getInt();
            intArr.add(i);
        }
        return intArr;
    }

    public static ArrayList<String> bytesToString(byte[] bytes) {
        ArrayList<String> strArr = new ArrayList<String>();
        int ptr = 4;
        while (ptr < bytes.length && bytes[ptr] != '\0') {
            int sEnd = findEndOfString(bytes, ptr);
            strArr.add(new String(Arrays.copyOfRange(bytes, ptr, sEnd)));
            ptr = sEnd + 1 + 4;
        }

        return strArr;
    }

    public static int findEndOfString(byte[] bytes, int start) {
        for (int i = start; i < bytes.length; i++) {
            if (bytes[i] == (byte) '\0') return i;
        }
        return -1;
    }

    public static byte[] combineBytes(byte[] b1, byte[] b2) {
        byte[] newBytes = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, newBytes, 0, b1.length);
        System.arraycopy(b2, 0, newBytes, b1.length, b2.length);
        return newBytes;
    }

    public static void main(String[] args) {
        System.out.println(Arrays.toString(combineBytes("abc".getBytes(), "def".getBytes())));
    }
}

