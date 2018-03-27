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
            Iterator<String> iterator1 = getMaxBlocksFromFile(reader1, Helper.RELATION1).iterator();
            String prevA = null;
            int countA = 0;
            while (iterator1.hasNext()) {
                String a = iterator1.next();
                if(prevA == null) prevA = a;
                if (a.equals(prevA)) {
                    countA++;
                }else {
                    System.out.println(prevA.substring(0,8) + " : " + countA);
                    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
                    Iterator<String> iterator2 = getBlockFromFile(reader2, Helper.RELATION2).iterator();

                    while (iterator2.hasNext()) {
                        String b = iterator2.next();
                        if (prevA.substring(0, 8).equals(b.substring(0, 8))) {
                            for (int i = 0; i < countA; i++) {
                                output.add(prevA + b);
                                if (output.size() == Helper.numOfTuplesPerOutput) {
                                    writeToFile(output, "loop_temp/" + Helper.OUTPUT);
                                }
                            }
                        }

                        if (!iterator2.hasNext()) {
                            iterator2 = getBlockFromFile(reader2, Helper.RELATION2).iterator();
                        }
                    }

                    reader2.close();
                    prevA = a;
                    countA = 1;
                }

                if (!iterator1.hasNext()) {
                    iterator1 = getMaxBlocksFromFile(reader1, Helper.RELATION1).iterator();
                }
            }
            if (!output.isEmpty()) {
                writeToFile(output, "loop_temp/" + Helper.OUTPUT);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<String> getMaxBlocksFromFile(BufferedReader reader, String relation) throws IOException {
        Helper.calculateBufferSize(relation);
        List<String> blocks = new ArrayList<>();
        for (int i = 0; i < Helper.bufferSize - 3; i++) {
            List<String> temp = getBlockFromFile(reader, relation);
            if (temp.isEmpty()) break;
            else blocks.addAll(temp);
        }
        Collections.sort(blocks);
        return blocks;
    }

    private static List<String> getBlockFromFile(BufferedReader reader, String relation) throws IOException {
        List<String> block = new ArrayList<>();
        String line;
        for (int i = 0; i < Helper.getTuplesPerBlock(relation); i++) {
            line = reader.readLine();
            if (line == null) break;
            block.add(line);
        }
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