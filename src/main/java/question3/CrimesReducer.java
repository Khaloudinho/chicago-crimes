package question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CrimesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String, Integer> map;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int sum = 0, counter = 0;

        String centroid = "";

        Double mx = 0d, my = 0d;

        for (IntWritable value : values) {
            sum += value.get();

            // 1st step for centering the clusters

            String[] temp = key.toString().split(", ");
            mx += Double.parseDouble(temp[0].substring(temp[0].indexOf("[") + 1, temp[0].length()));
            my += Double.parseDouble(temp[1].substring(0, temp[1].indexOf("]") - 1));
            counter++;
        }

        // 2nd step for centring the clusters

        mx = mx / counter;
        my = my / counter;

        // We count all the crimes present in one cluster and we write it into a map

        if (mx != 0d && my != 0d) centroid = "[" + mx + ", " + my + "]";
        if (!centroid.isEmpty()) getMap().put(centroid, sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        map = sortByValueInReverseOrder(getMap());

        int counter = 0;

        // We take the 3 most dangerous clusters

        for (Map.Entry<String, Integer> entry : getMap().entrySet()) {
            if (counter++ == 3) break;
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }

        map = sortByValueInNormalOrder(getMap());

        counter = 0;

        // We take the 3 least dangerous clusters

        for (Map.Entry<String, Integer> entry : getMap().entrySet()) {
            if (counter++ == 3) break;
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }

    private Map<String, Integer> getMap() {
        if (null == map) map = new HashMap<>();
        return map;
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValueInReverseOrder(Map<K, V> map) {
        return map.entrySet().stream().sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValueInNormalOrder(Map<K, V> map) {
        return map.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }
}