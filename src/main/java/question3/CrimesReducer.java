package question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CrimesReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        Double mx = 0d;
        Double my = 0d;
        int counter = 0;

        for (Text value : values) {
            System.out.println("KEY : " + key + " - VALUE : " + value);

            String[] temp = value.toString().split(", ");

            mx += Double.parseDouble(temp[0]);
            my += Double.parseDouble(temp[1]);
            counter++;
        }

        mx = mx / counter;
        my = my / counter;

        String centroid = "";
        if (mx != 0d && my != 0d) centroid = mx + " " + my;

        if (!centroid.isEmpty()) context.write(new Text(centroid), key);
    }

}
