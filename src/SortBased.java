import java.io.*;
import java.util.*;

public class SortBased {

    static Map<String, Double> gradeMap = new HashMap<>();

    static int blockReads1 = 0, blockWrites1 = 0;
    static int blockReads2 = 0, blockWrites2 = 0;
    static int blockWritesGpa = 0, blockWritesJoin = 0;

    public static void main(String[] args) {

        gradeMap.put("A+", 4.3);
        gradeMap.put("A", 4.0);
        gradeMap.put("A-", 3.7);
        gradeMap.put("B+", 3.3);
        gradeMap.put("B", 3.0);
        gradeMap.put("B-", 2.7);
        gradeMap.put("C+", 2.3);
        gradeMap.put("C", 2.0);
        gradeMap.put("C-", 1.7);
        gradeMap.put("D+", 1.3);
        gradeMap.put("D", 1.0);
        gradeMap.put("D-", 0.7);

        File tempDir = new File("sort_temp");
        tempDir.mkdir();

        Helper.relativeStart = Helper.start = Calendar.getInstance().getTimeInMillis();
        System.out.println("Started at: " + Calendar.getInstance().getTime());

        String temp = performSort();
        System.out.println("calculating join...");
        calculateBagJoin(temp.split(":")[0], temp.split(":")[1]);
        Helper.printTime("Time for join", true);

        Helper.printTime("Total time taken", false);

        System.out.println("Total block reads for relation1: " + blockReads1);
        System.out.println("Total block writes for relation1: " + blockWrites1);
        System.out.println("Total block reads for relation2: " + blockReads2);
        System.out.println("Total block writes for relation2: " + blockWrites2);
        System.out.println("Total block writes for join: " + blockWritesJoin);
        System.out.println("Total block writes for gpa: " + blockWritesGpa);

        System.out.println("Total disk IOs: " + (blockReads1 + blockWrites1 + blockReads2 + blockWrites2));
        System.out.println("Total disk IOs with join and GPA: " + (blockReads1 + blockWrites1 + blockReads2 + blockWrites2 + blockWritesGpa + blockWritesJoin));
    }

    private static String performSort() {
        System.out.println("Sorting relation 1...");
        int p1 = (performSortFor(Helper.RELATION1));

        System.out.println("Sorting relation 2...");
        int p2 = (performSortFor(Helper.RELATION2));

        return p1 + ":" + p2;
    }

    private static int performSortFor(String relation) {
        // phase 1
        int numOfRuns = createSortedSubList(relation);
        Helper.printTime("Time for phase1 (" + relation + ")", true);
        // phase 2
        int pass = 1;
        while (numOfRuns > 1) {
            numOfRuns = mergeSubLists(relation, numOfRuns, pass);
            pass++;
        }
        Helper.printTime("Time for phase2 (" + relation + ")", true);
        return pass - 1;
    }

    private static int createSortedSubList(String relation) {
        Helper.calculateBufferSize(relation);
        try {
            BufferedReader reader = new BufferedReader(new FileReader("data/" + relation));
            List<String> tuples = new ArrayList<>();
            String line;
            int numOfRuns = 0;
            for (int i = 0; (line = reader.readLine()) != null; i++) {
                if(i % Helper.getTuplesPerBlock(relation) == 0) {
                    if(relation.equals(Helper.RELATION1)) blockReads1++;
                    else blockReads2++;
                }

                tuples.add(line + "~>1");
                if (i == (Helper.getTuplesPerBlock(relation) * Helper.bufferSize) - 1) {
                    quickSort(tuples, 0, tuples.size() - 1);
                    i = -1;

                    writeToFile(tuples, relation, 0, numOfRuns);

                    numOfRuns++;
                }
            }

            if (!tuples.isEmpty()) {
                // Collections.sort(tuples);
                quickSort(tuples, 0, tuples.size() - 1);
                writeToFile(tuples, relation, 0, numOfRuns);
                numOfRuns++;
            }

            reader.close();
            reader = null;
            tuples = null;
            return numOfRuns;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static int mergeSubLists(String relation, int numOfRuns, int pass) {
        int temp = 0;
        int run = 0;
        try {
            Helper.calculateBufferSize(relation);
            Helper.bufferSize = 50;
            for (;; ++run) {
                int size = Math.min(Helper.bufferSize, numOfRuns - (run * Helper.bufferSize));
                List<List<String>> chunks = new ArrayList<>();
                List<Integer> indices = new ArrayList<>();
                List<Integer> blockNum = new ArrayList<>();
                List<String> output = new ArrayList<>();

                List<BufferedReader> readers = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    File file = new File(
                            "sort_temp/" + relation + "-sublist-" + (pass - 1) + "-" + ((run * Helper.bufferSize) + i));
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    getNextBlockFromReader(relation, reader, chunks, indices, blockNum, i);
                    readers.add(reader);
                }

                String selection = null;
                String prevSelection = null;
                int selectionCount = 0;

                while (true) {
                    boolean flag = false;
                    prevSelection = selection;
                    selection = null;
                    for (int i = 0; i < size; i++) {
                        if (indices.get(i) < chunks.get(i).size()) {
                            if (selection == null
                                    || selection.compareTo(chunks.get(i).get(indices.get(i)).split("~>")[0]) > 0) {
                                selection = chunks.get(i).get(indices.get(i)).split("~>")[0];
                            }
                            flag = true;
                        } else {
                            if (getNextBlockFromReader(relation, readers.get(i), chunks, indices, blockNum, i)) {
                                if (selection == null
                                        || selection.compareTo(chunks.get(i).get(indices.get(i)).split("~>")[0]) > 0) {
                                    selection = chunks.get(i).get(indices.get(i)).split("~>")[0];
                                }
                                flag = true;
                            }
                        }
                    }

                    if (prevSelection != null && !prevSelection.equals(selection)) {
                        output.add(prevSelection + "~>" + selectionCount);
                        selectionCount = 0;
                        if (output.size() == Helper.numOfTuplesPerOutput) {
                            writeToFile(output, relation, pass, run);
                        }
                    }

                    for (int i = 0; i < size; i++) {
                        if (indices.get(i) < chunks.get(i).size()) {
                            if (selection.equals(chunks.get(i).get(indices.get(i)).split("~>")[0])) {
                                selectionCount += Integer.valueOf(chunks.get(i).get(indices.get(i)).split("~>")[1]);
                                indices.set(i, indices.get(i) + 1);
                            }
                        }
                    }

                    if (!flag) {
                        if (!output.isEmpty()) {
                            writeToFile(output, relation, pass, run);
                        }
                        break;
                    }
                }

                temp += size;
                if (temp >= numOfRuns)
                    break;

                for (int i = 0; i < readers.size(); i++) {
                    if (readers.get(i) != null)
                        readers.get(i).close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return run + 1;
    }

    private static void calculateBagJoin(String p1, String p2) {
        int rowCount = 0;
        try {
            String file1 = "sort_temp/" + Helper.RELATION1 + "-sublist-" + p1 + "-0";
            String file2 = "sort_temp/" + Helper.RELATION2 + "-sublist-" + p2 + "-0";

            BufferedReader reader1 = new BufferedReader(new FileReader(file1));
            Iterator<String> iterator1 = getBlockFromFile(reader1, Helper.RELATION1).iterator();

            BufferedReader reader2 = new BufferedReader(new FileReader(file2));
            Iterator<String> iterator2 = getBlockFromFile(reader2, Helper.RELATION2).iterator();

            List<String> output = new ArrayList<>();
            List<String> gpa = new ArrayList<>();
            int credits = 0;
            double grades = 0.0;
            String prevB = null;

            String[] a = null, b = null;
            boolean flagA = true;
            boolean flagB = true;

            while (iterator1.hasNext() || iterator2.hasNext()) {
                if (flagB) {
                    if (iterator2.hasNext()) {
                        b = iterator2.next().split("~>");

                        int c = Integer.valueOf(b[0].substring(21, 23).trim());
                        double g = 0;

                        if (gradeMap.containsKey(b[0].substring(23, 27).trim()))
                            g = gradeMap.get(b[0].substring(23, 27).trim());

                        if (prevB == null || !b[0].substring(0, 8).equals(prevB)) {
                            if (prevB != null) {
                                double temp = grades / credits;
                                gpa.add(b[0].substring(0, 8) + " " + String.format("%.2f", temp));
                                if (gpa.size() == Helper.numOfTuplesPerGpa) {
                                    writeToFile(gpa, "sort_temp/" + Helper.GPA);
                                    blockWritesGpa++;
                                }
                            }

                            prevB = b[0].substring(0, 8);
                            credits = c;
                            grades = c * g;
                        } else {
                            credits += c;
                            grades += (c * g);
                        }
                    } else {
                        b = null;
                    }
                }
                if (flagA) {
                    if (iterator1.hasNext())
                        a = iterator1.next().split("~>");
                    else
                        a = null;
                }

                if (a != null && b != null) {
                    if (a[0].substring(0, 8).compareTo(b[0].substring(0, 8)) < 0) {
                        flagA = true;
                        flagB = false;
                    } else if (a[0].substring(0, 8).compareTo(b[0].substring(0, 8)) > 0) {
                        flagA = false;
                        flagB = true;
                    } else {
                        flagB = true;
                        flagA = false;
                        int totalCount = Integer.valueOf(a[1]) * Integer.valueOf(b[1]);
                        rowCount += totalCount;
                        output.add(a[0] + b[0].substring(8, b[0].length()) + "~>" + totalCount);
                        if (output.size() == Helper.numOfTuplesPerOutput) {
                            writeToFile(output, "sort_temp/" + Helper.OUTPUT);
                            blockWritesJoin++;
                        }
                    }
                } else if (b != null) {
                    flagB = true;
                } else if (a != null) {
                    flagA = true;
                }

                if (!iterator2.hasNext()) {
                    iterator2 = getBlockFromFile(reader2, Helper.RELATION2).iterator();
                }

                if (!iterator1.hasNext()) {
                    iterator1 = getBlockFromFile(reader1, Helper.RELATION1).iterator();
                }
            }

            if (!output.isEmpty()) {
                blockWritesJoin++;
                writeToFile(output, "sort_temp/" + Helper.OUTPUT);
            }
            if (!gpa.isEmpty()) {
                blockWritesGpa++;
                writeToFile(gpa, "sort_temp/" + Helper.GPA);
            }
        } catch (IOException e) {
            System.out.println("Error while calculating join.");
            e.printStackTrace();
        }
        System.out.println("Total number of tuples in join result: " + rowCount);

    }

    private static List<String> getBlockFromFile(BufferedReader reader, String relation) throws IOException {
        if(relation.equals(Helper.RELATION1)) blockReads1++;
        else blockReads2++;
        List<String> output = new ArrayList<>();
        String line;
        for (int i = 0; i < Helper.getTuplesPerBlock(relation); i++) {
            line = reader.readLine();
            if (line == null)
                break;
            output.add(line);
        }
        return output;
    }

    private static boolean getNextBlockFromReader(String relation, BufferedReader reader, List<List<String>> chunks,
                                                  List<Integer> indices, List<Integer> blockNum, int index) throws IOException {
        boolean temp = false;
        if(relation.equals(Helper.RELATION1)) blockReads1++;
        else blockReads2++;

        if (blockNum.size() == index) {
            indices.add(0);
            blockNum.add(0);
            chunks.add(new ArrayList<>());
        } else {
            chunks.get(index).clear();
        }

        for (int i = 0; i < Helper.getTuplesPerBlock(relation); i++) {
            String line = reader.readLine();
            if (line == null)
                break;
            chunks.get(index).add(line);
        }

        if (!chunks.get(index).isEmpty()) {
            temp = true;
            indices.set(index, 0);
        }

        blockNum.set(index, blockNum.get(index) + 1);

        return temp;
    }

    private static void writeToFile(List<String> output, String relation, int pass, int run) throws IOException {
        int count = output.size() / Helper.getTuplesPerBlock(relation);
        if(count == 0) count = 1;
        if(relation.equals(Helper.RELATION1)) blockWrites1 += count;
        else blockWrites2 += count;

        writeToFile(output, "sort_temp/" + relation + "-sublist-" + pass + "-" + run);
    }

    private static void writeToFile(List<String> output, String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
        for (String string : output) {
            writer.write(string + System.getProperty("line.separator"));
        }
        writer.close();
        output.clear();
    }

    private static void quickSort(List<String> a, int left, int right) {
        int index = partition(a, left, right);
        if (left < index - 1)
            quickSort(a, left, index - 1);
        if (index < right)
            quickSort(a, index, right);
    }

    private static int partition(List<String> a, int left, int right) {
        int i = left, j = right;
        String pivot = a.get((left + right) / 2);
        while (i <= j) {
            while (a.get(i).compareTo(pivot) < 0)
                i++;
            while (a.get(j).compareTo(pivot) > 0)
                j--;
            if (i <= j) {
                Collections.swap(a, i, j);
                i++;
                j--;
            }
        }
        return i;
    }

}
