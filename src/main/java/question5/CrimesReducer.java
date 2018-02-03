package question5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class CrimesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String, Integer> map;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        getMap().put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        map = sortByValue(getMap());

        TreeMap<String, Integer> myNewMap = getMap().entrySet().stream().limit(3).
                collect(TreeMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

        for (Map.Entry<String, Integer> entry : myNewMap.entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }

    public Map<String, Integer> getMap() {
        if (null == map) map = new HashMap<>();
        return map;
    }

    private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        return map.entrySet().stream().sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }
}