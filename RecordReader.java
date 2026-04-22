import java.io.*;

public class RecordReader implements AutoCloseable {
    private final RandomAccessFile file;
    private final long end;
    private long currentPos;
    private String nextLine;

    public RecordReader(String filePath, long start, long length) throws IOException {
        this.file = new RandomAccessFile(filePath, "r");
        this.file.seek(start);
        this.end = start + length;
        this.currentPos = start;
        if (start != 0) {
            file.readLine();
            currentPos = file.getFilePointer();
        }
        advance();
    }

    private void advance() throws IOException {
        if (currentPos >= end) {
            nextLine = null;
            return;
        }
        nextLine = file.readLine();
        if (nextLine == null) {
            currentPos = end;
        } else {
            currentPos = file.getFilePointer();
        }
    }

    public boolean nextKeyValue() throws IOException {
        if (nextLine == null) return false;
        return true;
    }

    public Long getCurrentKey() {
        return currentPos - (nextLine != null ? nextLine.length() + 1 : 0);
    }

    public String getCurrentValue() {
        return nextLine;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
