package controller;

import entity.Page;
import entity.StorageSys;
import utils.ByteConverter;

import java.io.*;

public class DiskController {
    private final int pageSize;
    private final String[] columnName = {"id", "Timestamp", "Station", "Temperature", "Humidity", "Intermediate"};
    private Integer[] pageCounter = {0, 0, 0, 0, 0, 0};
    private StorageSys inputBuffer;
    private String baseFilePath;

    public DiskController(int pageSize, String storageFileLoc) {
        this.pageSize = pageSize;
        this.inputBuffer = new StorageSys(columnName.length, pageSize);
        this.baseFilePath = storageFileLoc;
    }

    public void setPageCounter(int idx, int count) {
        this.pageCounter[idx] = count;
    }

    public Integer[] getPageCounter() {
        return pageCounter;
    }

    public String getBaseFilePath() {
        return baseFilePath;
    }

    public void init(String fileName) throws Exception {
        //id,Timestamp,Station,Temperature,Humidity
        // Total memory needed = 5 for record
        String line = "";
        String splitBy = ",";
        int row = 1;
        try {
            //parsing a CSV file into BufferedReader class constructor
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String header = br.readLine();
            while ((line = br.readLine()) != null)   //returns a Boolean value
            {
                String[] record = line.split(splitBy);    // use comma as separator

                // reformatting
                if (record[3].equals("M")) record[3] = "NaN";
                if (record[4].equals("M")) record[4] = "NaN";
                if (record[2].equals("Changi")) record[2] = "1";
                else record[2] = "2";

                for (int i = 0; i < 5; i++) {
                    byte[] byteBuffer;
                    byte[] rowByte = ByteConverter.toByte(row);
                    if (i == 0 || i == 2) {
                        byteBuffer = ByteConverter.combineBytes(rowByte, ByteConverter.toByte(Integer.parseInt(record[i])));
                    } else if (i >= 3) {
                        byteBuffer = ByteConverter.combineBytes(rowByte, ByteConverter.toByte(Float.parseFloat(record[i])));
                    } else {
                        byteBuffer = ByteConverter.combineBytes(rowByte, ByteConverter.toByte(record[i]));
                    }


                    Page newPage = writeToInputBuffer(i, byteBuffer, inputBuffer.readPage(i));
                    inputBuffer.writePage(i, newPage);
                }
                row++;

            }

            // Write the remaining input buffer to disk
            for (int i = 0; i < 5; i++) {
                if (inputBuffer.readPage(i).getRemainingByte() != pageSize) {
                    String filepath = writeToFile(columnName[i], pageCounter[i], i, inputBuffer);
//                    System.out.printf("Writing to disk location %d \n", pageCounter[i] * 6 + i);
                    pageCounter[i] += 1;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Page writeToInputBuffer(int idxOfInputBuffer, byte[] bytesToWrite, Page p) throws Exception {
        int rem = p.getRemainingByte();
        if (p.getRemainingByte() < bytesToWrite.length) {
            String filepath = writeToFile(columnName[idxOfInputBuffer],
                    pageCounter[idxOfInputBuffer],
                    idxOfInputBuffer,
                    inputBuffer);
//            System.out.printf("Writing to disk location %d", this.pageCounter[idxOfInputBuffer] * 6 + idxOfInputBuffer);
            this.pageCounter[idxOfInputBuffer] += 1;

//          Reset the input buffer
            p = new Page(new byte[this.pageSize], pageSize);
            rem = p.getRemainingByte();
        }

        int idxWrite = p.getPageSize() - rem;
//        System.out.printf("Writing to input buffer %d at position %d \n", idxOfInputBuffer, idxWrite);
        p.setBytes(bytesToWrite, idxWrite);
        p.setRemainingByte(rem - bytesToWrite.length);


        return p;
    }

    public String writeToFile(String columnName, int th, int memoryAddr, StorageSys ss) {
        try {
            // Initialize a pointer
            // in file using OutputStream
            String baseAddr = this.baseFilePath;
            String columnFolder = baseAddr + columnName;
            File columnFile = new File(columnFolder);
            if (columnFile.mkdir()) {
//                System.out.println("Folder created: " + columnFile.getName());
            }

            String dataAddr = columnFolder + "/" + th;
            File file = new File(dataAddr);

            if (file.createNewFile()) {
//                System.out.println("File created: " + file.getName());
            }

            OutputStream os = new FileOutputStream(file, false);

            // Starts writing the bytes in it
            os.write(ss.getStorage()[memoryAddr].getBytes());

            // Close the file
            os.close();
            return dataAddr;
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
        return "";

    }

    public Page readFromFile(String postfix) throws IOException {
        Page p = new Page(new byte[this.pageSize], this.pageSize);
        String filePath = baseFilePath + postfix;
        File file = new File(filePath);
        FileInputStream fileInputStream;
        byte[] bFile = new byte[(int) file.length()];
        try {
            //convert file into array of bytes
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bFile);
            fileInputStream.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        p.setBytes(bFile, 0);
        p.setRemainingByte(this.pageSize - bFile.length);
        return p;
    }


}
