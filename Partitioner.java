public interface Partitioner<KEY> {
    int getPartition(KEY key, int numReducers);
}
