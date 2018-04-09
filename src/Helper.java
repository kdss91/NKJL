import java.util.Calendar;

public class Helper {

    public static final String RELATION1 = "rel1";
    public static final String RELATION2 = "rel2";
    public static final String OUTPUT = "join";
    public static final String GPA = "gpa";

    public static final int BLOCK_SIZE = 4 * 1024;

    private static final int numOfTuplesPerBlockA = 40;
    private static final int numOfTuplesPerBlockB = 130;
    public static final int numOfTuplesPerOutput = 30;
    public static final int numOfTuplesPerGpa = 130;

    private static final int numOfBytesPerTupleA = 320;
    private static final int numOfBytesPerTupleB = 130;

    public static int bufferSize = 0;
    public static long start = 0, relativeStart = 0;

    static long getAvailableMemory() {
        System.gc();
        System.runFinalization();
        return Runtime.getRuntime().freeMemory();
    }

    static long getMaxMemory() {
        System.gc();
        System.runFinalization();
        return Runtime.getRuntime().totalMemory();
    }

    static int getBlockSize(String relation){
        if(relation.equals(RELATION1)){
            return numOfTuplesPerBlockA * numOfBytesPerTupleA;
        } else {
            return numOfTuplesPerBlockB * numOfBytesPerTupleB;
        }
    }

    static int getTuplesPerBlock(String relation) {
        if(relation.equals(RELATION1)){
            return numOfTuplesPerBlockA;
        }else {
            return numOfTuplesPerBlockB;
        }
    }

    static void calculateBufferSize(String relation) {
        long availableMemory = Helper.getAvailableMemory();
        System.out.println("Available memory: " + availableMemory + "Bytes, " + availableMemory / (1024 * 1024) + "MB");
        int numOfBlocksForMemory = (int) Math.floor(availableMemory / getBlockSize(relation));
        bufferSize = numOfBlocksForMemory - 2;
        System.out.println("BufferSize: " + bufferSize);
    }

    static String getFormattedTime(long diff) {
        return ((double) diff / (1000)) + " seconds, " + ((double) diff / (1000 * 60)) + " minutes";
    }

    static void printTime(String label, boolean isRelative) {
        long diff;
        if (isRelative) {
            diff = Calendar.getInstance().getTimeInMillis() - relativeStart;
            relativeStart = Calendar.getInstance().getTimeInMillis();
        } else {
            diff = Calendar.getInstance().getTimeInMillis() - start;
        }

        System.out.println(label + ": " + Helper.getFormattedTime(diff));
    }
}
