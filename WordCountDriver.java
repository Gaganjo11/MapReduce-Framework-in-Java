import java.io.File;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountDriver <input file> <output dir>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setInputPath(args[0]);
        job.setOutputDir(args[1]);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setNumReduceTasks(3);
        job.setMaxRetries(2);

        new File(args[1]).mkdirs();
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
