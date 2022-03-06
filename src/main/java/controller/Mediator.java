package controller;

import entity.Page;
import utils.ByteConverter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;

public class Mediator {
    private MemoryController memoryController;
    private DiskController diskController;
    private final int MEMORYSIZE;
    private final int PAGESIZE;


    //Filename will be the location of the input csv file.
    //Filepath is the base path for which the column store files should be store
    public Mediator(int MEMORYSIZE, int PAGESIZE, String FILEPATH, String FILENAME) throws Exception {
        this.diskController = new DiskController(PAGESIZE, FILEPATH);
        diskController.init(FILENAME);
        this.MEMORYSIZE = MEMORYSIZE;
        this.PAGESIZE = PAGESIZE;
        this.memoryController = new MemoryController(MEMORYSIZE, PAGESIZE);
    }

    public int getMEMORYSIZE() {
        return MEMORYSIZE;
    }

    public int getPAGESIZE() {
        return PAGESIZE;
    }

    public int reserveMemory(String s) throws Exception {
        return this.memoryController.reserveMemory(s);
    }

    public int getPageCounter(int column){
        return this.diskController.getPageCounter()[column];
    }

    // This function write page in memory to file
    public void flushToDisk(String columnName,int outputAddr){
        diskController.writeToFile(columnName, diskController.getPageCounter()[5],
                outputAddr, memoryController.getMemory());
        memoryController.addToPageTable_D2M(diskController.getPageCounter()[5] * 6 + 5, outputAddr);
        memoryController.addToPageTable_M2D(outputAddr, diskController.getPageCounter()[5] * 6 + 5);
        memoryController.releaseMemory(outputAddr);
        diskController.setPageCounter(5, diskController.getPageCounter()[5] + 1);
    }

    //This function reset the page to all empty bytes
    public void resetPage(int memoryLoc){
        memoryController.getPage(memoryLoc).resetPage(PAGESIZE);
    }

    // This function give memory location or bring data in disk to memory
    // Size = number of byte per record (position + data)
    public Page getPage(int column, String columnName, int thInDisk) throws Exception {
        Page page;
        int addrInDisk = thInDisk * 6 + column;
        if (memoryController.checkPageTable_D2M(addrInDisk)) {
            page = memoryController.getPage(memoryController.getMemoryAddr(addrInDisk));
        } else {
            int addrInMemory = memoryController.writeToMemory(addrInDisk,
                    diskController.readFromFile(columnName + "/" + thInDisk));
            page = memoryController.getPage(addrInMemory);
        }
        return page;
    }

    // These pages in memory has not been flushed to disk
    public Page getPageInMemory(int idx){
        return memoryController.getPage(idx);
    }

    // Write to file if full and return the new memory addr
    public int outputToMemory(int outputAddr, byte[] objectToWrite, String outputFilename) throws Exception {
        Page outputBuffer = this.memoryController.getPage(outputAddr);
        if (objectToWrite.length > outputBuffer.getRemainingByte()) {
            int nextOutputAddr = this.memoryController.reserveMemory("New output buffer");
            Page newOutputBuffer = this.memoryController.getPage(nextOutputAddr);
            newOutputBuffer.resetPage(this.PAGESIZE);
            newOutputBuffer.setBytes(objectToWrite, 0);
            newOutputBuffer.setRemainingByte(newOutputBuffer.getRemainingByte() - objectToWrite.length);

            diskController.writeToFile(outputFilename, diskController.getPageCounter()[5],
                    outputAddr, memoryController.getMemory());
            memoryController.addToPageTable_D2M(diskController.getPageCounter()[5] * 6 + 5, outputAddr);
            memoryController.addToPageTable_M2D(outputAddr, diskController.getPageCounter()[5] * 6 + 5);

            diskController.setPageCounter(5, diskController.getPageCounter()[5] + 1);
            memoryController.releaseMemory(outputAddr);

            return nextOutputAddr;
        } else {
            outputBuffer.setBytes(objectToWrite, outputBuffer.getPageSize() - outputBuffer.getRemainingByte());
            outputBuffer.setRemainingByte(outputBuffer.getRemainingByte() - objectToWrite.length);
            return outputAddr;
        }
    }

    public void saveOutputAsText(String columnname, ArrayList<Integer> pagesInDisk) throws Exception {
        String header = "Date,Station,Category,Value";
        String outputFileName = "ScanResult.csv";
        FileWriter fw = new FileWriter(this.diskController.getBaseFilePath() + outputFileName, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(header);
        bw.newLine();

        for (Integer idx : pagesInDisk) {
            int ptr = 0;
            byte[] bytes = getPage(5, columnname, idx).getBytes();

            while (ptr < this.PAGESIZE) {
                String date = new String(Arrays.copyOfRange(bytes, ptr, ptr + 10));
                ptr += 10;

                if (date.getBytes()[0] == '\0') break;

                int stationInt = ByteConverter.bytesToInt(Arrays.copyOfRange(bytes,
                        ptr, ptr + 4)).get(0);
                String station = stationInt == 1 ? "Changi" : "Paya Lebar";
                ptr += 4;

                String category = new String(Arrays.copyOfRange(bytes, ptr, ptr + 15));
                ptr += 15;

                float value = ByteConverter.bytesToFloat(Arrays.copyOfRange(bytes,
                        ptr, ptr + 4)).get(0);
                ptr += 4;

                bw.write(date + "," + station + "," + category + "," + value);
                bw.newLine();

            }

        }

        bw.close();
        System.out.println("Output file generated at " + this.diskController.getBaseFilePath() + outputFileName);
    }

}
