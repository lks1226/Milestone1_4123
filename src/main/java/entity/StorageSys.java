package entity;

import java.io.*;
import java.util.Arrays;

public class StorageSys {
    private final Page[] storage;
    private final int pageSize;


    public StorageSys(int numPage, int pageSize) {
        this.storage = new Page[numPage];
        this.pageSize = pageSize;
        for (int i = 0; i < numPage; i++) {
            this.storage[i] = new Page(new byte[pageSize], pageSize);
        }
    }

    public static void main(String[] args) {
        StorageSys ss = new StorageSys(5, 10);
        Page[] ps = ss.getStorage();

        byte[] bytes = "abc".getBytes();
        byte[] bytes2 = "def".getBytes();
        Page p = new Page(new byte[10], 10 - bytes.length);
        p.setBytes(bytes, 0);
        ss.writePage(1, p);

        Page p2 = new Page(new byte[10], 10 - bytes.length);
        p2.setBytes(bytes2, 0);
        ss.writePage(3, p2);
        for (int i = 0; i < 5; i++) {
            System.out.println(Arrays.toString(ps[i].getBytes()));
            System.out.println(ps[i].getRemainingByte());
        }
        System.out.println(Arrays.toString(ss.getAllRemainingSize()));
    }

    public Page[] getStorage() {
        return storage;
    }

    public boolean writePage(int addr, Page pageToWrite) {
        int idx = this.checkAddr(addr);
        if (idx != -1) {
            this.storage[idx] = pageToWrite;
            return true;
        }

        return false;
    }

    public Page readPage(int addr) {
        int idx = this.checkAddr(addr);

        if (idx != -1) {
            return this.storage[idx];
//            byte[] newBytes = new byte[this.pageSize];
//            System.arraycopy(tempPage.getBytes(), 0, newBytes, 0, this.pageSize);
//            return new Page(newBytes, tempPage.getRemainingByte());
        }

        return null;
    }

    public int checkAddr(int addr) {
        if (addr >= this.storage.length) {
            return -1;
        }
        return addr;
    }

    public int[] getAllRemainingSize() {
        int[] l = new int[this.storage.length];
        for (int i = 0; i < this.storage.length; i++) {
            l[i] = this.storage[i].getRemainingByte();
        }
        return l;
    }

}
