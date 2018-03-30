import java.io.*;
import java.util.*;

public class LoopBased {

    public static void main(String[] args) {
        File tempDir = new File("loop_temp");
        tempDir.mkdir();

        Helper.relativeStart = Helper.start = Calendar.getInstance().getTimeInMillis();
        System.out.println("Started at: " + Calendar.getInstance().getTime());
        System.out.println("calculating join...");
        calculateBagJoin();
        Helper.printTime("Total time taken", false);
    }

    private static void calculateBagJoin() {
        try {
            String file1 = "data/" + Helper.RELATION1;
            String file2 = "data/" + Helper.RELATION2;

            List<String> output = new ArrayList<>();

            BufferedReader reader1 = new BufferedReader(new FileReader(file1));
            Helper.calculateBufferSize(Helper.RELATION1);
            Iterator<String> iterator1 = getBlocksFromFile(reader1, Helper.RELATION1, Helper.bufferSize - 100).iterator();
            String prevA = null;
            int countA = 0;
            while (iterator1.hasNext()) {
                String a = iterator1.next();
                if (prevA == null) prevA = a;
                if (a.equals(prevA)) {
                    countA++;
                } else {
                    System.out.println(prevA.substring(0, 8) + " : " + countA);
                    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
                    Iterator<String> iterator2 = getBlocksFromFile(reader2, Helper.RELATION2, 1).iterator();

                    while (iterator2.hasNext()) {
                        boolean flag = false;
                        String b = iterator2.next();
                        if (prevA.substring(0, 8).equals(b.substring(0, 8))) {
                            for (int i = 0; i < countA; i++) {
                                output.add(prevA + b);
                                if (output.size() == Helper.numOfTuplesPerOutput) {
                                    writeToFile(output, "loop_temp/" + Helper.OUTPUT);
                                }
                            }
                        } else if (prevA.substring(0, 8).compareTo(b.substring(0, 8)) < 0) {
                            flag = true;
                        }

                        if (!iterator2.hasNext() || flag) {
                            iterator2 = getBlocksFromFile(reader2, Helper.RELATION2, 1).iterator();
                        }
                    }

                    reader2.close();
                    prevA = a;
                    countA = 1;
                }

                if (!iterator1.hasNext()) {
                    Helper.calculateBufferSize(Helper.RELATION1);
                    iterator1 = getBlocksFromFile(reader1, Helper.RELATION1, Helper.bufferSize - 100).iterator();
                    System.out.println("read next portion of relation 1");
                }
            }
            if (!output.isEmpty()) {
                writeToFile(output, "loop_temp/" + Helper.OUTPUT);
            }

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
        return block;
    }

    private static void writeToFile(List<String> output, String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(
                new FileWriter(filePath, true));
        for (String string : output) {
            writer.write(string + System.getProperty("line.separator"));
        }
        writer.close();
        output.clear();
    }
}