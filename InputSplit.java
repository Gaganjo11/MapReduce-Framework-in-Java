import java.io.*;

public class InputSplit {
    private final String filePath;
    private final long start;
    private final long length;

    public InputSplit(String filePath, long start, long length) {
        this.filePath = filePath;
        this.start = start;
        this.length = length;
    }

    public RecordReader createRecordReader() throws IOException {
        return new RecordReader(filePath, start, length);
    }

    public String getFilePath() { return filePath; }
    public long getStart() { return start; }
    public long getLength() { return length; }
}
