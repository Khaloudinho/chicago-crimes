package question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// 1 - Tirer k centroïdes aléatoires
// 2 - Faire la distance euclidienne avec chaque centroïde
// 3 - Garder ceux dont la distance avec le centroïde est <= 2 kms
// 4 - Faire la somme des points de chaque centroïdes
// 5 - Prendre les 3 plus criminogènes
// 6 - Prendre les 3 moins criminogènes
// 7 - Tracer sur la carte avec deux couleurs différentes chacunes des six zones

public class CrimesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private List<Double[]> locations = new ArrayList<>(), centroids = new ArrayList<>();
    private Random rand = new Random();

    public void map(LongWritable key, Text value, Context context) {

        String[] tokens = value.toString().split(",");

        String latitude = tokens[tokens.length - 4].trim(), longitude = tokens[tokens.length - 3].trim();

        String regexLocation = "^-?\\d+\\.\\d+$";

        if (latitude.matches(regexLocation) && longitude.matches(regexLocation)) {
            Double[] coordinates = new Double[2];
            coordinates[0] = Double.parseDouble(latitude);
            coordinates[1] = Double.parseDouble(longitude);
            if (!locations.contains(coordinates)) {
                locations.add(coordinates);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < Integer.parseInt(context.getConfiguration().get(Constants.CENTROID_NUMBER_ARG)); i++) {
            centroids.add(locations.get(rand.nextInt(locations.size())));
        }

        int index = 0, number = 1;

        for (Double[] coordinates : locations) {
            double x = coordinates[0], y = coordinates[1];

            double minDistance = Double.MAX_VALUE;

            for (int j = 0; j < centroids.size(); j++) {
                double distance = Math.sqrt(Math.pow(centroids.get(j)[0] - x, 2) + Math.pow(centroids.get(j)[1] - y, 2));
                if (distance < minDistance) {
                    index = j;
                    minDistance = distance;
                }
            }

            context.write(new Text(index + " [" + centroids.get(index)[0] + ", " + centroids.get(index)[1] + "]"),
                          new IntWritable(number));
        }
    }
}
