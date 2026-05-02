# 1. Navigate to project directory
cd "/home/gagan/Documents/java map/MapReduce-Framework-in-Java"

# 2. (Optional) Recompile if source changed
javac *.java

# 3. Run simple version
java SimpleWordCount input.txt output_simple.txt

# 4. View results
cat output_simple.txt

# 5. Run MapReduce version
java WordCountDriver input.txt mapreduce_output

# 6. View MapReduce results
cat mapreduce_output/result-0   # Results from reducer 0
cat mapreduce_output/result-1   # Results from reducer 1
cat mapreduce_output/result-2   # Results from reducer 2
