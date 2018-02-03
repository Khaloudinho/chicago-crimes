package question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class CrimesMapper extends Mapper<Object, Text, IntWritable, Text> {

    private static List<Double[]> centroids;

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        centroids = Utils.readCentroids(cacheFiles[0].toString());
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        String regexDigits = "^\\d{7}$";
        String xCoord = tokens[tokens.length - 8].trim(), yCoord = tokens[tokens.length - 7].trim();
        double x = 0d, y = 0d;

        if (xCoord.matches(regexDigits)) x = Double.parseDouble(xCoord);
        if (yCoord.matches(regexDigits)) y = Double.parseDouble(yCoord);

        int index = 0;
        double minDistance = Double.MAX_VALUE;

        for (int j = 0; j < centroids.size(); j++) {
            if (x != 0d && y != 0d) {
                double distance = Utils.euclideanDistance(centroids.get(j)[0], centroids.get(j)[1], x, y);

                if (distance < minDistance) {
                    index = j;
                    minDistance = distance;
                }
            }
        }

        context.write(new IntWritable(index), new Text(String.valueOf(x) + ", " + String.valueOf(y)));
    }
}
