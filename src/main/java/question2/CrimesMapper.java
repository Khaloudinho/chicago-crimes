package question2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Date;
import java.util.Locale;

public class CrimesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] tokens = value.toString().split(",");
        String date = tokens[2].trim();

        String[] slots = {"0-4", "4-8", "8-12", "12-16", "16-20", "20-24"};
        int number = 1, index = 0;

        DateFormat inFormat = new SimpleDateFormat( "MM/dd/yyyy hh:mm:ss aa"), outFormat = new SimpleDateFormat( "HH");

        try {
            if (date.length() == 22) index = Integer.parseInt(outFormat.format(inFormat.parse(date))) / 4;
        } catch (ParseException e) {
            e.printStackTrace();
        }

        context.write(new Text(slots[index]), new IntWritable(number));
    }
}
