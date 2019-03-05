import java.io.File;
import java.lang.instrument.Instrumentation;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

class Dictonary {

    public static void main(String[] args) {
        long beforeUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        File file = new File("./words.txt");
        Set<String> dictonary = new HashSet<String>();
        try {
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                dictonary.add(sc.nextLine());
            }
            sc.close();
            long afterUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long actualMemUsed = afterUsedMem - beforeUsedMem;
            System.out.println("Memory " + actualMemUsed);
            System.out.println(dictonary.size());
            Scanner sc1 = new Scanner(System.in);
            int n = sc1.nextInt();
            System.out.println(n);
            sc1.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}