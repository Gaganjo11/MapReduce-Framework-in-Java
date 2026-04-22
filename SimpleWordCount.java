import java.io.*;
import java.util.*;

public class SimpleWordCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SimpleWordCount <input file> <output file>");
            System.exit(1);
        }
        
        Map<String, Integer> counts = new HashMap<>();
        
        // Read file line by line
        try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.toLowerCase().split("\\W+");
                for (String word : words) {
                    if (!word.isEmpty()) {
                        counts.put(word, counts.getOrDefault(word, 0) + 1);
                    }
                }
            }
        }
        
        // Write output
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(args[1]))) {
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                writer.write(entry.getKey() + "\t" + entry.getValue());
                writer.newLine();
            }
        }
        
        System.out.println("Word count completed. Output written to " + args[1]);
    }
}
