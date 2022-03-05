package controller;

import entity.Page;
import entity.StorageSys;

import java.util.Arrays;
import java.util.HashMap;

public class MemoryController {
    private final int numPage;
    private final int pageSize;
    private int reservedPage = 0;
    private boolean[] isReserved;
    private StorageSys memory;
    private int nextAddr = 0;
    private HashMap<Integer, Integer> pageTable_M2D = new HashMap<Integer, Integer>();
    private HashMap<Integer, Integer> pageTable_D2M = new HashMap<Integer, Integer>();

    public MemoryController(int numPage, int pageSize) {
        this.numPage = numPage;
        this.pageSize = pageSize;
        this.memory = new StorageSys(numPage, pageSize);
        this.isReserved = new boolean[numPage];
        Arrays.fill(isReserved, false);
    }


    public static void main(String[] args) throws Exception {
        MemoryController mc = new MemoryController(2, 10);

        byte[] bytes = "abc".getBytes();
        byte[] bytes2 = "def".getBytes();
        Page p = new Page(new byte[10], 10 - bytes.length);
        p.setBytes(bytes, 0);

        Page p2 = new Page(new byte[10], 10 - bytes.length);
        p2.setBytes(bytes2, 0);

        mc.writeToMemory(1, p);
        mc.writeToMemory(1, p2);
        mc.writeToMemory(0, p2);

        for (int i = 0; i < mc.getMemory().getStorage().length; i++) {
            Page pTemp = mc.getMemory().readPage(i);
            pTemp.printString();
        }
    }


    public boolean checkPageTable_D2M(int addr) {
        return this.pageTable_D2M.containsKey(addr);
    }

    public int getMemoryAddr(int diskAddr) {
        return this.pageTable_D2M.get(diskAddr);
    }

    //    Using simple FIFO and exclude reserve page out of the cycle
    public int writeToMemory(int diskAddr, Page p) throws Exception {
        this.memory.writePage(this.nextAddr, p);
//        System.out.printf("Wrote to memory location %d for block %d \n", this.nextAddr, diskAddr);
        int curAddr = this.nextAddr;

        this.pageTable_D2M.put(diskAddr, curAddr);
        this.pageTable_M2D.put(curAddr, diskAddr);
        this.nextAddr = findNextAddr(curAddr);

        return curAddr;
    }

    public StorageSys getMemory() {
        return memory;
    }

    public Page getPage(int i) {
        return getMemory().readPage(i);
    }

    // This will make sure the last page of the memory array will not be changed for usual
    // read and write unless specified
    public int reserveMemory(String s) throws Exception {
        this.reservedPage += 1;
        this.isReserved[nextAddr] = true;

        this.pageTable_D2M.remove(pageTable_M2D.get(nextAddr));


        // M2D is to find the key in D2M to remove
        this.pageTable_M2D.remove(nextAddr);

        int curAddr = this.nextAddr;
        this.nextAddr = findNextAddr(curAddr);
//        System.out.printf("Memory reserved at %d by %s \n", curAddr, s);
        return curAddr;
    }

    public void releaseMemory(int addr){
        if (!isReserved[addr]){
            System.out.printf("Page %d is not a reserved page \n", addr);
        }
        else{
//            System.out.printf("Page %d is released \n", addr);
            this.reservedPage -= 1;
            this.isReserved[addr] = false;
        }
    }

    private int findNextAddr(int curAddr) throws Exception {
        int ptr = (curAddr + 1) % this.numPage;
        while (ptr != curAddr){
            if (!isReserved[ptr]){
                this.nextAddr = ptr;
                this.memory.getStorage()[nextAddr].resetPage(this.pageSize);
                this.pageTable_D2M.remove(pageTable_M2D.get(nextAddr));

                // M2D is to find the key in D2M to remove
                this.pageTable_M2D.remove(nextAddr);

                break;
            }

            ptr = (ptr + 1) % this.numPage;
        }

        if (ptr == curAddr){
                System.out.println(Arrays.toString(this.isReserved));
                throw new Exception("All memories are reserved");
        }

        return ptr;
    }

    public void addToPageTable_D2M(int diskAddr, int memoryAddr){
        this.pageTable_D2M.put(diskAddr, memoryAddr);
    }

    public void addToPageTable_M2D(int diskAddr, int memoryAddr){
        this.pageTable_M2D.put(diskAddr, memoryAddr);
    }

    }
