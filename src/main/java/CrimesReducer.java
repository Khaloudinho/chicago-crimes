import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CrimesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }

    public static class IntComparator extends WritableComparator {
        public IntComparator() {
            super(IntWritable.class);
        }

        private Integer int1;
        private Integer int2;

        @Override
        public int compare(byte[] raw1, int offset1, int length1, byte[] raw2, int offset2, int length2) {
            int1 = ByteBuffer.wrap(raw1, offset1, length1).getInt();
            int2 = ByteBuffer.wrap(raw2, offset2, length2).getInt();

            return int2.compareTo(int1);
        }
    }
}