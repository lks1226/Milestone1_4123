import controller.DiskController;
import controller.MemoryController;
import entity.Page;
import utils.ByteConverter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class Query {
    private final int MEMORYSIZE;
    private final int PAGESIZE;
    private final String[] columnName = {"id", "Timestamp", "Station", "Temperature", "Humidity", "Intermediate"};
    private MemoryController memoryController;
    private DiskController diskController;
    private HashMap<String, Integer> stationInt = new HashMap<String, Integer>();
    private ArrayList<Integer> filteredDate;
    private int largerYear;

    public Query(int MEMORYSIZE, int PAGESIZE, DiskController diskController) {
        this.MEMORYSIZE = MEMORYSIZE;
        this.PAGESIZE = PAGESIZE;
        this.memoryController = new MemoryController(MEMORYSIZE, PAGESIZE);
        this.diskController = diskController;

        this.stationInt.put("Changi", 1);
        this.stationInt.put("Paya Lebar", 2);
    }

    public static void main(String[] args) throws Exception {

        final int PAGESIZE = 1024;
        final int MEMORYSIZE = 1000; // In number of pages
        final String FILENAME = "/Users/kaishern/Desktop/NTU/4123-Big Data Management/Lab/Miles" +
                "tone1_4123/src/main/resources/SingaporeWeather.csv";
//        final String FILENAME = "/Users/kaishern/Desktop/NTU/4123-Big Data Management/Lab/Miles" +
//                "tone1_4123/src/main/resources/year02_1_31.csv";
        final String FILEPATH = "/Users/kaishern/Desktop/NTU/4123-Big Data Management/Lab/Miles" +
                "tone1_4123/src/main/resources/";

        // Initialise the Disk storage
        System.out.println("Reading SingaporeWeather.csv to column stored files....");
        DiskController diskController = new DiskController(PAGESIZE, FILEPATH);
        diskController.init(FILENAME);
        System.out.println("Running the query......");
        Query q = new Query(MEMORYSIZE, PAGESIZE, diskController);
        ArrayList<Integer> p = q.matchYear(2003, 2013);
        ArrayList<Integer> p1 = q.matchStation("Paya Lebar", p);
        ArrayList<Integer> p2 = q.findMinMax(q.filteredDate);
        ArrayList<Integer> p3 = q.generateFinalOutput(p2, q.filteredDate, p1);

//        System.out.println("==========Filtered Location============");
//        for (Integer i : p1) {
//            System.out.println(Arrays.toString(diskController.readFromFile("Intermediate/" + i).getBytes()));
//        }
//        System.out.println("==========Filtered Date============");
//        for (Integer i : q.filteredDate) {
//            System.out.println(Arrays.toString(diskController.readFromFile("Intermediate/" + i).getBytes()));
//        }
//        System.out.println("==========Min max Value============");
//        for (Integer i : p2) {
//            System.out.println(Arrays.toString(diskController.readFromFile("Intermediate/" + i).getBytes()));
//        }
//        System.out.println("==========Final Result============");
//        for (Integer i : p3) {
//            System.out.println(Arrays.toString(diskController.readFromFile("Intermediate/" + i).getBytes()));
//        }

        q.saveOutputAsText(p3);

    }

    public ArrayList<Integer> matchYear(int year1, int year2) throws Exception {
        this.largerYear = Math.max(year1, year2);
        int outputAddr = this.memoryController.reserveMemory("Year output buffer");

        ArrayList<Integer> outputDiskAddr = new ArrayList<Integer>();

        for (int thInDisk = 0; thInDisk < diskController.getPageCounter()[1]; thInDisk++) {
            Page p = getPage(1, thInDisk);
            byte[] targetByte = (Integer.toString(year1)).getBytes();
            byte[] targetByte2 = (Integer.toString(year2)).getBytes();
            int ptr = 4;
            byte[] bytes = p.getBytes();

            while (ptr < bytes.length && bytes[ptr] != '\0') {
                byte[] position = Arrays.copyOfRange(bytes, ptr - 4, ptr);
                byte[] yearBytes = Arrays.copyOfRange(bytes, ptr, ptr + 4);

                if (Arrays.equals(yearBytes, targetByte) || (Arrays.equals(yearBytes, targetByte2))) {
                    byte[] result = Arrays.copyOfRange(bytes, ptr - 4, ptr + 17);
                    int nextOutputBufferLoc = outputToMemory(outputAddr, result);
                    // new output buffer is being used
                    if (nextOutputBufferLoc != outputAddr) {
                        outputAddr = nextOutputBufferLoc;
                        outputDiskAddr.add(diskController.getPageCounter()[5] - 1);
                    }
                }

                ptr = ptr + 17 + 4;

            }
        }

        // flush
        flushToDisk(outputAddr);
        outputDiskAddr.add(diskController.getPageCounter()[5] - 1);

        return outputDiskAddr;
    }

    public ArrayList<Integer> matchStation(String station, ArrayList<Integer> pagesInDisk) throws Exception {
        ArrayList<Integer> outputBufferAddrs = new ArrayList<Integer>();
        int outputAddr = this.memoryController.reserveMemory("Station output buffer");

        ArrayList<Integer> filteredInputAddrs = new ArrayList<Integer>();
        int filteredInputAddr = this.memoryController.reserveMemory("intermediate output buffer");

        byte[] targetByte = ByteConverter.toByte(this.stationInt.get(station));
        for (Integer idx : pagesInDisk) {
            byte[] inputBytes = getPage(5, idx).getBytes();
            int ptr = 4;
            // The loop for previous result
            while (ptr < inputBytes.length && inputBytes[ptr] != '\0') {
                byte[] position = Arrays.copyOfRange(inputBytes, ptr - 4, ptr);
                byte[] date = Arrays.copyOfRange(inputBytes, ptr, ptr + 17);
                int positionInt = ByteBuffer.wrap(position).getInt();

                Page stationPage = getPage(2, positionInt / (this.PAGESIZE / 8));

                //loop for station page
                int ptr1 = 4;
                byte[] stationsBytes = stationPage.getBytes();
                while (ptr1 < stationsBytes.length) {
                    byte[] stationPositionBytes = Arrays.copyOfRange(stationsBytes, ptr1 - 4, ptr1);
                    byte[] stationIntByte = Arrays.copyOfRange(stationsBytes, ptr1, ptr1 + 4);

                    // if match both position and station

                    if (Arrays.equals(position, stationPositionBytes) && Arrays.equals(stationIntByte, targetByte)) {

                        byte[] result = Arrays.copyOfRange(stationsBytes, ptr1 - 4, ptr1 + 4);
                        byte[] resultPrev = ByteConverter.combineBytes(position, date);

                        int nextOutputBufferLoc = outputToMemory(outputAddr, result);
                        // new output buffer is being used
                        if (nextOutputBufferLoc != outputAddr) {
                            outputAddr = nextOutputBufferLoc;
                            // output to memory will update page counter
                            outputBufferAddrs.add(diskController.getPageCounter()[5] - 1);
                        }

                        int nextFilteredInputAddr = outputToMemory(filteredInputAddr, resultPrev);
                        if (nextFilteredInputAddr != filteredInputAddr) {
                            filteredInputAddr = nextFilteredInputAddr;
                            // output to memory will update page counter
                            filteredInputAddrs.add(diskController.getPageCounter()[5] - 1);
                        }

                    }
                    ptr1 = ptr1 + 4 + 4;
                }

                ptr = ptr + 17 + 4;
            }
        }

        // flush
        flushToDisk(outputAddr);
        outputBufferAddrs.add(diskController.getPageCounter()[5] - 1);

        // flush
        flushToDisk(filteredInputAddr);
        filteredInputAddrs.add(diskController.getPageCounter()[5] - 1);

        this.filteredDate = new ArrayList<Integer>(filteredInputAddrs);
        return outputBufferAddrs;
    }

    // This function write page in memory to file
    public void flushToDisk(int outputAddr) {
        diskController.writeToFile(this.columnName[5], diskController.getPageCounter()[5],
                outputAddr, memoryController.getMemory());
        memoryController.addToPageTable_D2M(diskController.getPageCounter()[5] * 6 + 5, outputAddr);
        memoryController.addToPageTable_M2D(outputAddr, diskController.getPageCounter()[5] * 6 + 5);
        memoryController.releaseMemory(outputAddr);
        diskController.setPageCounter(5, diskController.getPageCounter()[5] + 1);
    }

    // Find the min and max value of temperature and humidity
    public ArrayList<Integer> findMinMax(ArrayList<Integer> pagesInDisk) throws Exception {
        int numMonthPerPage = (int) Math.floor((float) this.PAGESIZE / (31 * 8));
        int numPagePerType = (int) Math.ceil((float) 12 / numMonthPerPage);
        int numPageRequired = numPagePerType * 4 * 2; // 4 types 2 yrs
        ArrayList<Integer> outputMemoryLoc = new ArrayList<Integer>();
        ArrayList<Integer> outputDiskLoc = new ArrayList<Integer>();

        // Reserved the first numPageRequired memory
        // Need to release all after each stage

        for (int i = 0; i < numPageRequired; i++) {
            int memoryLoc = memoryController.reserveMemory("");
            outputMemoryLoc.add(memoryLoc);
            memoryController.getPage(memoryLoc).resetPage(this.PAGESIZE);
        }

        int ctr = 1;

        for (Integer idx : pagesInDisk) {
            // prev stage filtered date output
            byte[] inputBytes = getPage(5, idx).getBytes();
            int ptr = 4;
            while (ptr < inputBytes.length) {
                byte[] position = Arrays.copyOfRange(inputBytes, ptr - 4, ptr);
                int idInt = ByteBuffer.wrap(position).getInt() - 1;

                if (idInt == -1) break;
//                System.out.printf("\nPosition : %s \n", Arrays.toString(position));
                int thPage = idInt / (this.PAGESIZE / 8);
                int sectionIdx = idInt % (this.PAGESIZE / 8) * 8;
                Page tempPage = getPage(3, thPage);
                Page humPage = getPage(4, thPage);
                System.out.println();

                int year = Integer.parseInt(new String(Arrays.copyOfRange(inputBytes, ptr, ptr + 4)));
                int month = Integer.parseInt(new String(Arrays.copyOfRange(inputBytes, ptr + 5, ptr + 7)));
                int day = Integer.parseInt(new String(Arrays.copyOfRange(inputBytes, ptr + 8, ptr + 10)));

                float temp = ByteConverter.bytesToFloat(Arrays.copyOfRange(tempPage.getBytes(), sectionIdx + 4, sectionIdx + 8)).get(0);
                float hum = ByteConverter.bytesToFloat(Arrays.copyOfRange(humPage.getBytes(), sectionIdx + 4, sectionIdx + 8)).get(0);
//                System.out.printf("Year: %d Month: %d \n", year, month);
//                System.out.printf("Temp: %f Humidity: %f \n", temp, hum);

                // Temperature //
                int base = 0;
                if (year == largerYear)
                    base = base + (numPageRequired / 4);

                int idxMonth = ((month - 1) / numMonthPerPage);
                int pageSection = ((month - 1) % numMonthPerPage);
                // For min Temp
//                System.out.println("==Updating Min temp==");
                updateMin(base, idxMonth, day, pageSection, ctr, temp, outputMemoryLoc);

                //For max Temp
//                System.out.println("==Updating Max temp==");
                base += numPageRequired / 8;
                updateMax(base, idxMonth, day, pageSection, ctr, temp, outputMemoryLoc);

                // Humidity //
//                System.out.println("==Updating Min hum==");
                base = numPageRequired / 2;
                if (year == largerYear)
                    base = base + (numPageRequired / 4);
                // For min hum
                updateMin(base, idxMonth, day, pageSection, ctr, hum, outputMemoryLoc);


                //For max hum
//                System.out.println("==Updating Max hum==");
                base += numPageRequired / 8;
                updateMax(base, idxMonth, day, pageSection, ctr, hum, outputMemoryLoc);

                ptr += 17 + 4;
                ctr += 1;
            }
        }

        for (int i : outputMemoryLoc) {
            flushToDisk(i);
            outputDiskLoc.add(diskController.getPageCounter()[5] - 1);
        }
        return outputDiskLoc;

    }

    public void updateMin(int base, int idxMonth, int day, int pageSection, int ctr,
                          float value, ArrayList<Integer> outputMemoryLoc) throws Exception {
        int outputPageIdx = base + idxMonth;
        int startIndexInPage = pageSection * 31 * 8;
        Page outputPage = this.memoryController.getMemory().getStorage()[outputMemoryLoc.get(outputPageIdx)];
        float curVal = findValueInSection(startIndexInPage, startIndexInPage + (31 * 8), outputPage);
        byte[] ctrByte = ByteBuffer.allocate(4).putInt(ctr).array();
        byte[] valueByte = ByteBuffer.allocate(4).putFloat(value).array();
        byte[] resultByte = ByteConverter.combineBytes(ctrByte, valueByte);

        if (Float.isNaN(curVal) || curVal == value) {
            outputPage.setBytes(resultByte, startIndexInPage + ((day - 1) * 8));
        } else if (curVal > value) {
            outputPage.clearSection(startIndexInPage, startIndexInPage + (31 * 8));
            outputPage.setBytes(resultByte, startIndexInPage + ((day - 1) * 8));
        }
    }

    public void updateMax(int base, int idxMonth, int day, int pageSection, int ctr,
                          float value, ArrayList<Integer> outputMemoryLoc) throws Exception {
        int outputPageIdx = base + idxMonth;
        int startIndexInPage = pageSection * 31 * 8;
        Page outputPage = this.memoryController.getMemory().getStorage()[outputMemoryLoc.get(outputPageIdx)];
        float curVal = findValueInSection(startIndexInPage, startIndexInPage + (31 * 8), outputPage);
        byte[] ctrByte = ByteBuffer.allocate(4).putInt(ctr).array();
        byte[] valueByte = ByteBuffer.allocate(4).putFloat(value).array();
        byte[] resultByte = ByteConverter.combineBytes(ctrByte, valueByte);

        if (Float.isNaN(curVal) || curVal == value) {
            outputPage.setBytes(resultByte, startIndexInPage + ((day - 1) * 8));
        } else if (curVal < value) {
            outputPage.clearSection(startIndexInPage, startIndexInPage + (31 * 8));
            outputPage.setBytes(resultByte, startIndexInPage + ((day - 1) * 8));
        }
    }

    public float findValueInSection(int startIndexInPage, int endIndexInPage, Page outputPage) {
        for (int i = startIndexInPage; i < endIndexInPage; i += 8) {
            if (ByteConverter.bytesToInt(
                    Arrays.copyOfRange(outputPage.getBytes(), i, i + 4)).get(0) != 0) {
                return ByteConverter.bytesToFloat(
                        Arrays.copyOfRange(outputPage.getBytes(), i + 4, i + 8)).get(0);
            }
        }
        return Float.parseFloat("NaN");
    }

    // Write to file if full and return the new memory addr
    private int outputToMemory(int outputAddr, byte[] objectToWrite) throws Exception {
        Page outputBuffer = this.memoryController.getPage(outputAddr);
        if (objectToWrite.length > outputBuffer.getRemainingByte()) {
            int nextOutputAddr = this.memoryController.reserveMemory("New output buffer");
            Page newOutputBuffer = this.memoryController.getPage(nextOutputAddr);
            newOutputBuffer.resetPage(this.PAGESIZE);
            newOutputBuffer.setBytes(objectToWrite, 0);
            newOutputBuffer.setRemainingByte(newOutputBuffer.getRemainingByte() - objectToWrite.length);

            diskController.writeToFile(this.columnName[5], diskController.getPageCounter()[5],
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

    // This function give memory location or bring data in disk to memory
    // Size = number of byte per record (position + data)
    private Page getPage(int column, int thInDisk) throws Exception {
        Page page;
        int addrInDisk = thInDisk * 6 + column;
        if (memoryController.checkPageTable_D2M(addrInDisk)) {
            page = memoryController.getPage(memoryController.getMemoryAddr(addrInDisk));
        } else {
            int addrInMemory = memoryController.writeToMemory(addrInDisk,
                    diskController.readFromFile(columnName[column] + "/" + thInDisk));
            page = memoryController.getPage(addrInMemory);
        }
        return page;
    }

    public ArrayList<Integer> generateFinalOutput(ArrayList<Integer> pagesInDisk,
                                                  ArrayList<Integer> filteredDate, ArrayList<Integer> filteredStation) throws Exception {
        ArrayList<Integer> outputBufferAddrs = new ArrayList<Integer>();
        int outputAddr = this.memoryController.reserveMemory("Station output buffer");

        for (int i = 0; i < pagesInDisk.size(); i++) {
            int idx = pagesInDisk.get(i);
            String category = "";
            if (i / (pagesInDisk.size() / 8) % 2 == 0) {
                category += "Min ";
            } else category += "Max ";

            if (i >= pagesInDisk.size() / 2) {
                category += "Humidity   ";
            } else category += "Temperature";

            byte[] categoryByte = category.getBytes();
            byte[] inputBytes = getPage(5, idx).getBytes();
            for (int ptr = 4; ptr < inputBytes.length; ptr += 8) {
                byte[] position = Arrays.copyOfRange(inputBytes, ptr - 4, ptr);
                int positionInt = ByteBuffer.wrap(position).getInt() - 1;

                if (positionInt == -1) {
                    continue;
                }
                byte[] valueByte = Arrays.copyOfRange(inputBytes, ptr, ptr + 4);

                int thPage_date = positionInt / (this.PAGESIZE / 21);
                int idxInPage_date = (positionInt % (this.PAGESIZE / 21)) * 21;
                int thPage_station = positionInt / (this.PAGESIZE / 8);
                int idxInPage_station = (positionInt % (this.PAGESIZE / 8)) * 8;

                Page page_date = getPage(5, filteredDate.get(thPage_date));
                Page page_station = getPage(5, filteredStation.get(thPage_station));

                byte[] dateByte = Arrays.copyOfRange(page_date.getBytes(), idxInPage_date + 4,
                        idxInPage_date + 14);
                int stationInt = ByteConverter.bytesToInt(Arrays.copyOfRange(page_station.getBytes(),
                        idxInPage_station + 4, idxInPage_station + 8)).get(0);
                byte[] stationByte = ByteBuffer.allocate(4).putInt(stationInt).array();

                byte[] record = ByteConverter.combineBytes(dateByte, stationByte);
                record = ByteConverter.combineBytes(record, categoryByte);
                record = ByteConverter.combineBytes(record, valueByte);

                int nextOutputBufferLoc = outputToMemory(outputAddr, record);
                // new output buffer is being used
                if (nextOutputBufferLoc != outputAddr) {
                    outputAddr = nextOutputBufferLoc;
                    // output to memory will update page counter
                    outputBufferAddrs.add(diskController.getPageCounter()[5] - 1);
                }
            }
        }

        flushToDisk(outputAddr);
        outputBufferAddrs.add(diskController.getPageCounter()[5] - 1);
        return outputBufferAddrs;
    }

    public void saveOutputAsText(ArrayList<Integer> pagesInDisk) throws Exception {
        String header = "Date,Station,Category,Value";
        FileWriter fw = new FileWriter(this.diskController.getBaseFilePath() + "ScanResult.csv", true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(header);
        bw.newLine();

        for (Integer idx : pagesInDisk) {
            int ptr = 0;
            byte[] bytes = getPage(5, idx).getBytes();

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
        System.out.println("Output file generated at " + this.diskController.getBaseFilePath() + "ScanResult.csv");
    }
}

