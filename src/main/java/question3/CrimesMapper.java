package question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CrimesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private List<Double[]> locations = new ArrayList<>(), centroids = new ArrayList<>();

    public void map(LongWritable key, Text value, Context context) {

        String[] tokens = value.toString().split(",");

        // We get latitude and longitude as well of each crime

        String latitude = tokens[tokens.length - 4].trim(), longitude = tokens[tokens.length - 3].trim();

        String regexLocation = "^-?\\d+\\.\\d+$";

        // We add all the locations in a list

        if (latitude.matches(regexLocation) && longitude.matches(regexLocation)) {
            Double[] coordinates = new Double[2];
            coordinates[0] = Double.parseDouble(latitude);
            coordinates[1] = Double.parseDouble(longitude);

            if (!locations.contains(coordinates)) locations.add(coordinates);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // We convert the clusters number passed through the args in a number

        int clustersNumber = Integer.parseInt(context.getConfiguration().get(Constants.CENTROID_NUMBER_ARG));

        // Initialize the centroids

        if (centroids.isEmpty()) {
            for (int j = 0; j < clustersNumber; j++) {
                centroids.add(locations.get(j));
            }
        }

        int index = 0, number = 1;

        // Iterate through locations

        for (Double[] coordinates : locations) {

            double x = coordinates[0], y = coordinates[1];

            double minDistance = Double.MAX_VALUE;

            // Making euclidian distance between centroid and current point

            for (int k = 0; k < centroids.size(); k++) {
                double distance = Math.sqrt(Math.pow(centroids.get(k)[0] - x, 2) + Math.pow(centroids.get(k)[1] - y, 2));

                // Here thanks to index var we get the nearest cluster

                if (distance < minDistance) {
                    index = k;
                    minDistance = distance;
                }
            }

            // We add a crime to the nearest cluster we found for the current crime in the loop before

            context.write(new Text(index + " [" + centroids.get(index)[0] + ", " + centroids.get(index)[1] + "]"), new IntWritable(number));
        }
    }
}
