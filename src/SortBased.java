import java.io.*;
import java.util.*;

public class SortBased {

    public static void main(String[] args) {
        File tempDir = new File("temp");
        tempDir.mkdir();

        Helper.relativeStart = Helper.start = Calendar.getInstance().getTimeInMillis();
        System.out.println("Started at: " + Calendar.getInstance().getTime());

        if (args.length == 0 || args[0].equals("tpmms")) {
            String temp = performSort();
            System.out.println("calculating join...");
            calculateBagJoin(temp.split(":")[0], temp.split(":")[1]);
            Helper.printTime("Time for join", true);
        } else {
            System.out.println("Nested Loop technique not implemented yet");
        }
        Helper.printTime("Total time taken", false);
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
                tuples.add(line + "~>1");
                if (i == (Helper.getTuplesPerBlock(relation) * Helper.bufferSize) - 1) {
                    quickSort(tuples, 0, tuples.size() - 1);
                    i = -1;

                    writeToFile(tuples, relation, 0, numOfRuns);

                    numOfRuns++;
                }
            }

            if (!tuples.isEmpty()) {
                //Collections.sort(tuples);
                quickSort(tuples, 0, tuples.size() - 1);
                writeToFile(tuples, relation, 0, numOfRuns);
                numOfRuns++;
            }

            reader.close();
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
            for (; ; ++run) {
                int size = Math.min(Helper.bufferSize, numOfRuns - (run * Helper.bufferSize));
                List<List<String>> chunks = new ArrayList<>();
                List<Integer> indices = new ArrayList<>();
                List<String> output = new ArrayList<>();

                List<BufferedReader> readers = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    File file = new File("temp/" + relation + "-sublist-" + (pass - 1) + "-" + ((run * Helper.bufferSize) + i));
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    getNextBlockFromReader(relation, reader, chunks, indices, i);
                    readers.add(reader);
                }

                String selection = null;
                String prevSelection;
                int selectionCount = 0;

                while (true) {
                    boolean flag = false;
                    prevSelection = selection;
                    selection = null;
                    for (int i = 0; i < size; i++) {
                        if (indices.get(i) < chunks.get(i).size()) {
                            if (selection == null ||
                                    selection.compareTo(chunks.get(i).get(indices.get(i)).split("~>")[0]) > 0) {
                                selection = chunks.get(i).get(indices.get(i)).split("~>")[0];
                            }
                            flag = true;
                        } else {
                            if (getNextBlockFromReader(relation, readers.get(i), chunks, indices, i)) {
                                if (selection == null ||
                                        selection.compareTo(chunks.get(i).get(indices.get(i)).split("~>")[0]) > 0) {
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
                if (temp >= numOfRuns) break;

                for (int i = 0; i < readers.size(); i++) {
                    if (readers.get(i) != null) readers.get(i).close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return run + 1;
    }

    private static void calculateBagJoin(String p1, String p2) {
        try {
            String file1 = "temp/" + Helper.RELATION1 + "-sublist-" + p1 + "-0";
            String file2 = "temp/" + Helper.RELATION2 + "-sublist-" + p2 + "-0";

            BufferedReader reader1 = new BufferedReader(new FileReader(file1));
            Iterator<String> iterator1 = getBlockFromFile(reader1, Helper.RELATION1).iterator();

            BufferedReader reader2 = new BufferedReader(new FileReader(file2));
            Iterator<String> iterator2 = getBlockFromFile(reader2, Helper.RELATION2).iterator();

            List<String> output = new ArrayList<>();
            List<String> gpas = new ArrayList<>();

            String[] a = null, b = null;
            boolean flagA = true;
            boolean flagB = true;

            String sid = null;
            int credits = 0;
            double grades = 0.0;

            while (iterator1.hasNext() && iterator2.hasNext()) {
                if (flagB) {
                    b = iterator2.next().split("~>");
                    if (a != null && a[0].substring(0, 8).compareTo(b[0].substring(0, 8)) < 0) {
                        flagA = true;
                    }

                    //calculate GPA
                    String bid = b[0].substring(0, 8);
                    int c = Integer.valueOf(b[0].substring(21, 23).trim());
                    double g = Double.valueOf(b[0].substring(23, 27).trim());
                    if (sid == null) sid = bid;

                    if (!sid.equals(bid)) {
                        gpas.add(sid + String.format("%.2f", (double) (grades / credits)));
                        if (gpas.size() == Helper.numOfTuplesPerOutput) {
                            writeToFile(gpas, "temp/" + Helper.GPA);
                        }
                        sid = bid;
                        credits = c;
                        grades = (c * g);
                    } else {
                        credits += c;
                        grades += (c * g);
                    }
                }
                if (flagA) a = iterator1.next().split("~>");

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
                    for (int i = 0; i < totalCount; i++) {
                        output.add(a[0] + b[0]);
                        if (output.size() == Helper.numOfTuplesPerOutput) {
                            writeToFile(output, "temp/" + Helper.OUTPUT);
                        }
                    }
                }

                if (!iterator2.hasNext()) {
                    iterator2 = getBlockFromFile(reader2, Helper.RELATION2).iterator();
                }

                if (!iterator1.hasNext()) {
                    iterator1 = getBlockFromFile(reader1, Helper.RELATION1).iterator();
                }
            }

            if (!gpas.isEmpty()) {
                writeToFile(gpas, "temp/" + Helper.GPA);
            }

            if (!output.isEmpty()) {
                writeToFile(output, "temp/" + Helper.OUTPUT);
            }
        } catch (IOException e) {
            System.out.println("Error while calculating join.");
            e.printStackTrace();
        }

    }



    private static List<String> getBlockFromFile(BufferedReader reader, String relation) throws IOException {
        List<String> output = new ArrayList<>();
        String line;
        for (int i = 0; i < Helper.getTuplesPerBlock(relation); i++) {
            line = reader.readLine();
            if (line == null) break;
            output.add(line);
        }
        return output;
    }

    private static boolean getNextBlockFromReader(String relation, BufferedReader reader, List<List<String>> chunks,
                                                  List<Integer> indices, int index) throws IOException {
        boolean temp = false;

        if (chunks.size() == index) {
            indices.add(0);
            chunks.add(new ArrayList<>());
        } else {
            chunks.get(index).clear();
        }

        for (int i = 0; i < Helper.getTuplesPerBlock(relation); i++) {
            String line = reader.readLine();
            if (line == null) break;
            chunks.get(index).add(line);
        }

        if (!chunks.get(index).isEmpty()) {
            temp = true;
            indices.set(index, 0);
        }

        return temp;
    }

    private static void writeToFile(List<String> output, String relation, int pass, int run) throws IOException {
        writeToFile(output, "temp/" + relation + "-sublist-" + pass + "-" + run);
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