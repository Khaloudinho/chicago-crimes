package question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

// 1 - Tirer k centroïdes aléatoires
// 2 - Faire la distance euclidienne avec chaque centroïde
// 3 - Garder ceux dont la distance avec le centroïde est <= 2 kms
// 4 - Faire la somme des points de chaque centroïdes
// 5 - Prendre les 3 plus criminogènes
// 6 - Prendre les 3 moins criminogènes
// 7 - Tracer sur la carte avec deux couleurs différentes chacunes des six zones

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
        map = sortByValueInReverseOrder(getMap());

        int counter = 0;

        for (Map.Entry<String, Integer> entry : getMap().entrySet()) {
            if (counter++ == 3) break;
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }

        map = sortByValueInNormalOrder(getMap());

        counter = 0;

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