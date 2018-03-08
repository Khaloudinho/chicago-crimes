package question4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CrimesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        String regexLocation = "^-?\\d+\\.\\d+$", regexBoolean = "true|false";

        // We get latitude and longitude of each crime

        String latitude = tokens[tokens.length - 4].trim(), longitude = tokens[tokens.length - 3].trim();

        // We get the boolean which means if the crime has been elucidated or not

        String arrest = tokens[tokens.length - 15].toLowerCase().trim();

        int number = 1;

        if (latitude.matches(regexLocation) && longitude.matches(regexLocation) && arrest.matches(regexBoolean)) {
            context.write(new Text(latitude.concat("," + longitude + "_" + arrest)), new IntWritable(number));
        }
    }
}