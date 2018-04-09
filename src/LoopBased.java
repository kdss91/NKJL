import java.io.*;
import java.util.*;

public class LoopBased {
	static int readIOCount = 0 ;
	static int writeIOCount= 0 ;
    public static void main(String[] args) {
        File tempDir = new File("loop_temp");
        tempDir.mkdir();

        Helper.relativeStart = Helper.start = Calendar.getInstance().getTimeInMillis();
        System.out.println("Started at: " + Calendar.getInstance().getTime());
        System.out.println("calculating join...");
        calculateBagJoin();
        System.out.println("Total readIOCount: "+readIOCount);
        System.out.println("Total writeIOCount: "+writeIOCount);
        System.out.println("Total IOCount: "+readIOCount+ writeIOCount);
        Helper.printTime("Total time taken", false);
    }

    private static void calculateBagJoin() {
        try {
            String file1 = "data/" + Helper.RELATION1;
            String file2 = "data/" + Helper.RELATION2;

            List<String> output = new ArrayList<>();

            BufferedReader reader1 = new BufferedReader(new FileReader(file1));
            
            int tupleCount = 0;
            
            while(true) {
            	Helper.calculateBufferSize(Helper.RELATION1);
                List<String> list1 = getBlocksFromFile(reader1, Helper.RELATION1, Helper.bufferSize - 100);
                if(list1.isEmpty()) break;
        		Iterator<String> iterator1 = list1.iterator();
        		System.out.println("new block rel1");
        		
            	BufferedReader reader2 = new BufferedReader(new FileReader(file2));
            	List<String> list2 = getBlocksFromFile(reader2, Helper.RELATION2, 1);
                Iterator<String> iterator2 = list2.iterator();
                
                boolean flagA = true;
                boolean flagB = true;
                boolean nextB = false;
                
                String a = null, b = null;
                String lastB = list2.get(list2.size() - 1);
                String lastA = list1.get(list1.size() - 1);
                
            	while(iterator2.hasNext() || !flagB) {
            		if (flagB) {
                        if (iterator2.hasNext()) {
                        	b = iterator2.next();
                        	if(lastA.substring(0, 8).compareTo(b.substring(0,8)) < 0) {
                        		nextB = true;
                        	}
                        } else b = null;
                    }
                    if (flagA) {
                        if (iterator1.hasNext()) a = iterator1.next();
                        else a = null;
                    }
                    
                    if (a != null && b != null) {
                        if (a.substring(0, 8).compareTo(b.substring(0, 8)) < 0) {
                            flagA = true;
                            flagB = false;
                        } else if (a.substring(0, 8).compareTo(b.substring(0, 8)) > 0) {
                            flagA = false;
                            flagB = true;
                            if(a.substring(0, 8).compareTo(lastB.substring(0, 8)) > 0) {
                            	nextB = true;
                            }
                        } else {
                            flagB = true;
                            flagA = false;
                            tupleCount++;
                            output.add(a + b.substring(8, b.length()));
                            if (output.size() == Helper.numOfTuplesPerOutput) {
                                writeToFile(output, "loop_temp/" + Helper.OUTPUT);
                            }
                        }
                    } else if (b != null) {
                        flagB = true;
                    } else if (a != null) {
                        flagA = true;
                    }
                    
                    if ((flagB && !iterator2.hasNext()) || (flagA && !iterator1.hasNext()) || nextB) {
                    	list2.clear();
                    	list2 = getBlocksFromFile(reader2, Helper.RELATION2, 1);
                        iterator2 = list2.iterator();
                        if(list2.isEmpty()) lastB = null; 
                        else lastB = list2.get(list2.size() - 1);
                        
                        iterator1 = list1.iterator();
                        flagA = true;
                        flagB = true;
                        nextB = false;
                    }
            	}
            	
            	reader2.close();
            	reader2 = null;
            }
            if (!output.isEmpty()) {
                writeToFile(output, "loop_temp/" + Helper.OUTPUT);
            }

            System.out.println("total NumberOf tuples: " + tupleCount);	
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<String> getBlocksFromFile(BufferedReader reader, String relation, int numOfBlocks) throws IOException {
        List<String> block = new ArrayList<>();
        String line;
        for (int i = 0; i < (numOfBlocks * Helper.getTuplesPerBlock(relation)); i++) {
            line = reader.readLine();
            if (line == null) break;
            block.add(line);
        }
        Collections.sort(block);
        readIOCount++;
        return block;
    }

    private static void writeToFile(List<String> output, String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(
                new FileWriter(filePath, true));
        for (String string : output) {
            writer.write(string + System.getProperty("line.separator"));
        }
        writeIOCount++;
        writer.close();
        output.clear();
    }
}