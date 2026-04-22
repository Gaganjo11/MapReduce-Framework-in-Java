import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Job {
    private final Configuration conf;
    private String inputPath;
    private String outputDir;
    private Class<? extends Mapper> mapperClass;
    private Class<? extends Reducer> reducerClass;
    private Partitioner partitioner = new DefaultPartitioner<>();
    private int numReduceTasks = 2;
    private int maxRetries = 3;

    public Job(Configuration conf) {
        this.conf = conf;
    }

    public static Job getInstance(Configuration conf) {
        return new Job(conf);
    }

    public void setInputPath(String path) { this.inputPath = path; }
    public void setOutputDir(String dir) { this.outputDir = dir; }
    public void setMapperClass(Class<? extends Mapper> cls) { this.mapperClass = cls; }
    public void setReducerClass(Class<? extends Reducer> cls) { this.reducerClass = cls; }
    public void setPartitioner(Partitioner partitioner) { this.partitioner = partitioner; }
    public void setNumReduceTasks(int tasks) { this.numReduceTasks = tasks; }
    public void setMaxRetries(int retries) { this.maxRetries = retries; }

    public boolean waitForCompletion(boolean verbose) throws Exception {
        // Create input splits
        List<InputSplit> splits = createSplits(new File(inputPath));
        if (verbose) System.out.println("Created " + splits.size() + " splits");

        // Run map phase and collect intermediate data
        List<Pair<Object, Object>> allIntermediate = new ArrayList<>();
        for (int i = 0; i < splits.size(); i++) {
            if (verbose) System.out.println("Processing split " + (i+1) + " of " + splits.size());
            Context mapContext = runMapTask(splits.get(i), verbose);
            if (mapContext == null) return false;
            allIntermediate.addAll(mapContext.getOutputs());
            mapContext.clear(); // Free memory
        }

        if (verbose) System.out.println("Collected " + allIntermediate.size() + " intermediate pairs");

        // Shuffle and sort
        Map<Object, List<Object>> grouped = shuffleAndSort(allIntermediate);
        allIntermediate.clear(); // Free memory
        if (verbose) System.out.println("Shuffled into " + grouped.size() + " unique keys");

        // Run reduce phase
        boolean success = runReducePhase(grouped, verbose);
        if (!success) return false;

        System.out.println("Job completed successfully. Output written to " + outputDir);
        return true;
    }

    private List<InputSplit> createSplits(File file) throws IOException {
        long fileSize = file.length();
        long splitSize = Math.max(1024, fileSize / 2); // Smaller splits for memory
        if (splitSize < 1024) splitSize = fileSize;
        
        List<InputSplit> splits = new ArrayList<>();
        long start = 0;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            while (start < fileSize) {
                long end = Math.min(start + splitSize, fileSize);
                if (end < fileSize) {
                    raf.seek(end);
                    raf.readLine();
                    end = raf.getFilePointer();
                }
                splits.add(new InputSplit(file.getAbsolutePath(), start, end - start));
                start = end;
            }
        }
        return splits;
    }

    private Context runMapTask(InputSplit split, boolean verbose) {
        try {
            Context ctx = new Context(conf);
            Mapper mapper = mapperClass.getDeclaredConstructor().newInstance();
            try (RecordReader reader = split.createRecordReader()) {
                while (reader.nextKeyValue()) {
                    mapper.map(reader.getCurrentKey(), reader.getCurrentValue(), ctx);
                }
            }
            if (verbose) System.out.println("  Map task produced " + ctx.getOutputs().size() + " pairs");
            return ctx;
        } catch (Exception e) {
            System.err.println("Map task failed: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private Map<Object, List<Object>> shuffleAndSort(List<Pair<Object, Object>> intermediates) {
        Map<Object, List<Object>> grouped = new HashMap<>();
        for (Pair<Object, Object> pair : intermediates) {
            Object key = pair.getKey();
            Object value = pair.getValue();
            if (!grouped.containsKey(key)) {
                grouped.put(key, new ArrayList<>());
            }
            grouped.get(key).add(value);
        }
        return new TreeMap<>(grouped);
    }

    private boolean runReducePhase(Map<Object, List<Object>> grouped, boolean verbose) {
        // Create output directory
        new File(outputDir).mkdirs();
        
        // Process each key sequentially
        int reducerId = 0;
        for (Map.Entry<Object, List<Object>> entry : grouped.entrySet()) {
            try {
                Context reduceCtx = new Context(conf);
                Reducer reducer = reducerClass.getDeclaredConstructor().newInstance();
                reducer.reduce(entry.getKey(), entry.getValue(), reduceCtx);
                
                // Write output to a single file (since we're processing sequentially)
                String partFile = outputDir + "/part-r-" + String.format("%05d", reducerId++);
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(partFile))) {
                    for (Pair<Object, Object> pair : reduceCtx.getOutputs()) {
                        writer.write(pair.getKey() + "\t" + pair.getValue());
                        writer.newLine();
                    }
                }
                if (verbose) System.out.println("  Reduced key: " + entry.getKey());
            } catch (Exception e) {
                System.err.println("Reduce task failed for key " + entry.getKey() + ": " + e.getMessage());
                return false;
            }
        }
        return true;
    }
}