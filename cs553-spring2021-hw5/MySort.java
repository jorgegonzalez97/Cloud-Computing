import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class MySort {

    private static String inputF;
    private static String outputF;
    private static String outputLog;
    
    private static List < String > fileList = new ArrayList < String > ();
    private static long fileSize;
    private static int threadCount;
    private static int noOfChunks;
    
    private static BufferedWriter fLog;
    private static volatile String[][] fileChunks;
    
    //Implement Internal MergeSort
    static class MergeSort {

    	private static void merge(String[] a, int from, int mid, int to) {

            int n = to - from + 1; 
            String[] b = new String[n]; 
            int i1 = from; 
            int i2 = mid + 1; 
            int j = 0; 

            while (i1 <= mid && i2 <= to) {
                if (a[i1].compareTo(a[i2]) < 0) {
                    b[j] = a[i1];
                    i1++;
                } else {
                    b[j] = a[i2];
                    i2++;
                }
                j++;
            }

            while (i1 <= mid) {
                b[j] = a[i1];
                i1++;
                j++;
            }


            while (i2 <= to) {
                b[j] = a[i2];
                i2++;
                j++;
            }

            for (j = 0; j < n; j++) {
                a[from + j] = b[j];
            }
        } 

        private static void mergesort(String[] arr, int low, int high) {

            if (low < high) {

            	int middle = (low + high) / 2;

                mergesort(arr, low, middle);
                mergesort(arr, middle + 1, high);

                merge(arr, low, middle, high);

            }
        }
        
        public static void mergesort(String[] arr) {

            mergesort(arr, 0, arr.length - 1);


        }
    }
    
    static class Splitting {

        private int fileIndex;

        Splitting(String inName, String outName, int fileIndex) {

            this.fileIndex = fileIndex;

            try {
                BufferedWriter writer = Files.newBufferedWriter(Paths.get(inName + "_" + fileIndex));
                int linesToRead = (int)(fileSize / noOfChunks) / 100;

                Stream < String > chunks = Files.lines(Paths.get(inName))
                    .skip(linesToRead * fileIndex)
                    .limit(linesToRead);

                chunks.forEach(line -> {
                    writeToFile(writer, line);
                });

                writer.close();

            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }

        public void writeToFile(BufferedWriter writer, String line) {
            try {
                writer.write(line + "\r\n");
            } catch (Exception e) {
            	System.out.println(e.toString());
            }
        }
    }
    
    static List < String > Sort(List < String > dataToSort, int beginIndex, int endIndex) throws IOException {

        int i = beginIndex;
        int j = endIndex;

        int m = beginIndex + (endIndex - beginIndex) / 2;

        String mm = dataToSort.get(m).substring(0, 10);

        while (i <= j) {
            String temp = null;

            while (dataToSort.get(i).substring(0, 10).compareTo(mm) < 0)
                i++;

            while (dataToSort.get(j).substring(0, 10).compareTo(mm) > 0)
                j--;

            if (i <= j) {
                temp = dataToSort.get(i);
                dataToSort.set(i, dataToSort.get(j));
                dataToSort.set(j, temp);

                i++;
                j--;
            }
        }
        if (beginIndex < j)
            Sort(dataToSort, beginIndex, j);
        if (i < endIndex)
            Sort(dataToSort, i, endIndex);

        return dataToSort;
    }

    static void writeSortedFile(List < String > dataToSort, int fileIndex) throws IOException, InterruptedException {

        FileWriter filewrite = new FileWriter(new File(fileList.get(fileIndex)));
        BufferedWriter bufw = new BufferedWriter(filewrite);
        
        int k = 0;

        while (k != dataToSort.size()) {
            bufw.write(dataToSort.get(k) + " " + "\n");
            k++;
        }
        bufw.close();
    }
        

    public static void main(String[] args) throws Exception {
        
    	if (args.length != 4) {
            System.out.println("Wrong number of arguments: see README.txt");
            System.exit(-1);
        }

        inputF = args[0];
        outputF = args[1];
        outputLog = args[2];
        
        threadCount = Integer.parseInt(args[3]);
        
        long availMem = 8; // RAM Chamaleon limited to 8 GB
        noOfChunks = 128; //It can be changed
        
        try {
                fLog = new BufferedWriter(new FileWriter(outputLog, true));
            } catch (Exception e) {
                System.out.println(e.toString());
            }

        double tStart = System.currentTimeMillis();

        final List < Integer > fileIndexes = new ArrayList < Integer > ();
        
        for (int i = 0; i < noOfChunks; i++)
            fileIndexes.add(i);
        
        createFileList();


        File file = new File(inputF);
        fileSize = file.length();

        // In - memory sort if file size is smaller than available memory (< 8 GB)
        if (fileSize <= (availMem * 1000000000)) {

            System.out.println("IN-MEMORY SORT");

            // gensort generates lines with 100 bytes -> divide by number of threads to balance the workload
            int nLines = (int)(fileSize / 100) / threadCount; 
            fileChunks = new String[threadCount][nLines];

            Thread[] thread = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                thread[i] = new Thread(new Runnable() {
                	
                	@Override
                	 public void run() {

                         int tName = Integer.parseInt(Thread.currentThread().getName());
                         BufferedReader reader;
                         try {
                             reader = new BufferedReader(new FileReader(inputF));
                             String s;
                             int index = 0;
                             int lRead = 0;
                             while ((s = reader.readLine()) != null) {
                                 if (index++ >= nLines * tName)
                                     fileChunks[tName][lRead++] = s;
                                 if (lRead == nLines)
                                     break;
                             }
                             reader.close();
                         } catch (Exception e) {
                             e.printStackTrace();
                         }
                     }
                });
                thread[i].setName(Integer.toString(i));
                thread[i].start();
            }
            
            for (int i = 0; i < threadCount; i++) {
                thread[i].join();
            }

            int index = 0;
            String[] fLines = new String[(int)(fileSize / 100)];
            for (int i = 0; i < threadCount; i++) {
                for (int j = 0; j < nLines; j++) {
                    fLines[index++] = fileChunks[i][j];
                }
                fileChunks[i] = null;
            }
            
            fileChunks = null;
            
            MergeSort.mergesort(fLines);

            FileWriter filewrite = new FileWriter(new File(outputF));
            BufferedWriter bufw = new BufferedWriter(filewrite);
            int k = 0;
            while (k != fLines.length) bufw.write(fLines[k++] + " " + "\n");
            bufw.close();
            filewrite.close();

            long tEnd = System.currentTimeMillis();
            
             try {
                fLog.write("Time taken (ms): " + (tEnd - tStart) + "\r\n");
                fLog.close();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
            

        // If file bigger than memory --> external sorting
        } else {

            System.out.println("EXTERNAL SORT");

            Thread[] thread = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                thread[i] = new Thread(new Runnable() {
                	
                	@Override
                    public void run() {
                        try {
                            for (int i = 0; i < noOfChunks / threadCount; i++) {
                                int fileIndex = ((Integer.parseInt(Thread.currentThread().getName()) * noOfChunks) / (threadCount)) + i;
                                new Splitting(inputF, outputF, fileIndex);
                            }
                        } catch (Exception e) {
                            System.out.println(e.toString());
                        }
                    }
                });
                thread[i].setName(Integer.toString(i));
                thread[i].start();
            }
            
            for (int i = 0; i < threadCount; i++) {
                thread[i].join();
            }
            
            for (int i = 0; i < threadCount; i++) {
                thread[i] = new Thread(new Runnable() {
                    
                	@Override
                	public void run() {
                        try {
                            for (int i = 0; i < noOfChunks / threadCount; i++) {
                                int fileIndex = ((Integer.parseInt(Thread.currentThread().getName()) * noOfChunks) / (threadCount)) + i;
                                implementSorting(fileIndexes.get(fileIndex));
                            }
                        } catch (Exception e) {
                            System.out.println(e.toString());
                        }
                    }
                
                });
                thread[i].setName(Integer.toString(i));
                thread[i].start();
            }

            for (int i = 0; i < threadCount; i++) {
                thread[i].join();
            }

            System.out.println("All chunks created and merged individually");

            mergeSortedChunks();

            long tEnd = System.currentTimeMillis();
            System.out.println(" Output file created. See log file to check the time taken");
            
             try {
                fLog.write("Time taken (ms): " + (tEnd - tStart) + "\r\n");
                fLog.close();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
            
            
        }
    }

    private static void implementSorting(int index) throws Exception {

        List < String > retList = null;

        retList = readDataFile(fileList.get(index));
        retList = Sort(retList, 0, retList.size() - 1);
        writeSortedFile(retList, index);
        retList.clear();

    }

    private static void mergeSortedChunks() throws IOException {
       
        BufferedReader[] bufReaderArray = new BufferedReader[noOfChunks];
        List < String > mergeList = new ArrayList < String > ();
        List < String > keyValueList = new ArrayList < String > ();

        //Check first line from each chunk -> only way to merge all the files without loading them completely in memory
        for (int i = 0; i < noOfChunks; i++) {
            bufReaderArray[i] = new BufferedReader(new FileReader(fileList.get(i)));

            String fileLine = bufReaderArray[i].readLine();

            if (fileLine != null) {
                mergeList.add(fileLine.substring(0, 10));
                keyValueList.add(fileLine);
            }
        }

        BufferedWriter bufw = new BufferedWriter(new FileWriter(outputF));

        for (long j = 0; j < fileSize; j++) {
            String minString = mergeList.get(0);
            int minFile = 0;

            for (int k = 0; k < noOfChunks; k++) {
                if (minString.compareTo(mergeList.get(k)) > 0) {
                    minString = mergeList.get(k);
                    minFile = k;
                }
            }

            String s = keyValueList.get(minFile);
            bufw.write(s + "\n");

            mergeList.set(minFile, "-1");
            keyValueList.set(minFile, "-1");

            String temp = bufReaderArray[minFile].readLine();

            if (temp != null) {
                mergeList.set(minFile, temp.substring(0, 10));
                keyValueList.set(minFile, temp);
            } else {

                j = fileSize;

                List < String > tempList = new ArrayList < String > ();

                for (int i = 0; i < mergeList.size(); i++) {
                    if (keyValueList.get(i) != "-1")
                        tempList.add(keyValueList.get(i));

                    while ((minString = bufReaderArray[i].readLine()) != null) {
                        tempList.add(minString);
                    }
                }

                tempList = Sort(tempList, 0, tempList.size() - 1);
                int i = 0;
                while (i < tempList.size()) {
                    s = tempList.get(i);
                    bufw.write(s + "\n");
                    i++;
                }

            }
        }
        bufw.close();

        for (int i = 0; i < noOfChunks; i++)
            bufReaderArray[i].close();
    }

    private static void createFileList() {
        for (int i = 0; i < noOfChunks; i++)
            fileList.add(inputF + "_" + i);
    }

    static List < String > readDataFile(String filePath) throws Exception {
        List < String > dataToSort = new ArrayList < String > ();

        FileReader file = new FileReader(new File(filePath));
        BufferedReader bufRead = new BufferedReader(file);
        dataToSort.clear();

        String readline;

        while (true) {
            readline = null;

            if ((readline = bufRead.readLine()) == null)
                break;

            dataToSort.add(readline);
        }


        bufRead.close();

        return dataToSort;
    }

}

    
