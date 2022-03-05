package entity;

import utils.ByteConverter;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Page {
    private int remainingByte;
    private byte[] bytes;

    public Page(byte[] bytes, int remainingByte) {
        this.bytes = bytes;
        this.remainingByte = remainingByte;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getRemainingByte() {
        return remainingByte;
    }

    public int getPageSize() {
        return this.bytes.length;
    }

    public void setRemainingByte(int remainingByte) {
        this.remainingByte = remainingByte;
    }

    public void setBytes(byte[] bytes, int start) {
        System.arraycopy(bytes, 0, this.bytes, start, bytes.length);
    }

    public void printString() {
        System.out.println(ByteConverter.bytesToString(this.getBytes()));
    }

    public void resetPage(int pageSize){
        this.bytes = new byte[pageSize];
        this.remainingByte = pageSize;
    }


    public void clearSection(int start, int end){
        for (int i = start; i < end; i++){
            this.bytes[i] = '\0';
        }
    }

}
