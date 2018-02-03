package question5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CrimesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");

        String[] months = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "December"};

        int index = 0, number = 1;

        if (tokens[2].substring(0,2).matches("[0-9]{2}")) {
            index = Integer.parseInt(tokens[2].substring(0, 2).trim()) - 1;
        }

        context.write(new Text(months[index].toUpperCase()), new IntWritable(number));
    }
}