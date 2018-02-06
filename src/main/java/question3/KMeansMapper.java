package question3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 03/05/14
 * Time: 0.18
 */
public class KMeansMapper extends Mapper<Object, Text, Text, Text> {

    private static List<Double[]> centroids;

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        centroids = Utils.readCentroids(cacheFiles[0].toString());
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        String xCoord = tokens[tokens.length-4], yCoord = tokens[tokens.length-3], regex = "-?\\d{2}[.]\\d*";

        double x = 0.0d, y = 0.0d;

        if (xCoord.matches(regex) && yCoord.matches(regex)) {
            x = Double.parseDouble(xCoord);
            y = Double.parseDouble(yCoord);
        }

        int index = 0;
        double minDistance = Double.MAX_VALUE;

        for (int j = 0; j < centroids.size(); j++) {
            if (x != 0.0d && y != 0.0d) {
                double distance = Utils.euclideanDistance(centroids.get(j)[0], centroids.get(j)[1], x, y);

                if (distance < minDistance) {
                    index = j;
                    minDistance = distance;
                }
            }
        }

        context.write(new Text(String.valueOf(String.valueOf(x) + "," + String.valueOf(y))),
                new Text(String.valueOf(index)));
    }
}
