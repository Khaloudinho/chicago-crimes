package question4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CrimesMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        String regexLocation = "^(\\-?\\d+(\\.\\d+)?)", regexBoolean = "true|false";

        String latitude = tokens[tokens.length - 4].trim(), longitude = tokens[tokens.length - 3].trim();

        String arrest = tokens[tokens.length - 15].toLowerCase().trim();

        if (latitude.matches(regexLocation) && longitude.matches(regexLocation) && arrest.matches(regexBoolean)) {
            context.write(new Text(latitude.concat(", " + longitude)), new Text(arrest));
        }
    }
}