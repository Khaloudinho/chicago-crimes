package question4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CrimesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Iterate through the crimes in order to not have the same location twice
        // Otherwise the dataset would be really bigger

        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(key, new IntWritable(sum));

    }
}