package question1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Crimes {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: question1.Crimes <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        Job job = Job.getInstance(conf, "The decreasing ranking of categories of crimes");
        job.setJarByClass(Crimes.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(CrimesMapper.class);
        job.setReducerClass(CrimesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}