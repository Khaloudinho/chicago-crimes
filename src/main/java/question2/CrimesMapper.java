package question2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class CrimesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        // We get the date of each crime

        String date = tokens[2].trim();

        // We create a table with all the crimes slots we want to have in output

        String[] slots = {"0-4", "4-8", "8-12", "12-16", "16-20", "20-24"};

        int number = 1, index = 0;

        // We get the 24 hour format of the date we got before in order to use it later

        DateFormat inFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa"), outFormat = new SimpleDateFormat("HH");

        try {
            // We divide the hour by for in order to have the index which contains the corresponding slot
            // Ex : 22/4 = 5 because we only have the whole part of the result, so the slot we will have is slots[5] whether 20-24

            if (date.length() == 22) index = Integer.parseInt(outFormat.format(inFormat.parse(date))) / 4;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        context.write(new Text(slots[index]), new IntWritable(number));
    }
}
