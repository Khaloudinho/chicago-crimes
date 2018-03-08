package question5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CrimesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        // We get the month number of each crime

        String monthNumber = tokens[2].substring(0, 2);

        // We store all the months in a table

        String[] months = {"JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER",
                "OCTOBER", "NOVEMBER", "DECEMBER"};

        int index = 0, number = 1;

        // We create the index for each month number in order to match it witch the table created before
        // Ex : for the month number 3 it corresponds to March, so we subtract 1 then we have the index we need to get the month name

        if (monthNumber.matches("[0-9]{2}")) index = Integer.parseInt(monthNumber.trim()) - 1;

        context.write(new Text(months[index]), new IntWritable(number));
    }
}