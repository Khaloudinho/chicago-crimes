package question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 03/05/14
 * Time: 0.19
 */
public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //Double mx = 0d;
        //Double my = 0d;
        //int counter = 0;
        int sum = 0;

        for (Text value : values) {
            //String[] temp = value.toString().split(" ");
            //mx += Double.parseDouble(temp[0]);
            //my += Double.parseDouble(temp[1]);
            //counter++;
            context.write(key, value);
        }


        //mx = mx / counter;
        //my = my / counter;

        //String centroid = "";
        //if (mx != 0d && my != 0d) centroid = mx + " " + my;

        //if (!centroid.isEmpty()) context.write(new Text(centroid), key);
    }

}
