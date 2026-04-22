public class DefaultPartitioner<KEY> implements Partitioner<KEY> {
    @Override
    public int getPartition(KEY key, int numReducers) {
        return Math.abs(key.hashCode() % numReducers);
    }
}
