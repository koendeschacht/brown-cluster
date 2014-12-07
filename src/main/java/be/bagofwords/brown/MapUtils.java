package be.bagofwords.brown;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 04/12/14.
 */
public class MapUtils {

    public static int getTotalOfMap(Int2IntOpenHashMap count) {
        if (count == null) {
            return 0;
        } else {
            return count.values().stream().collect(Collectors.summingInt(v -> v));
        }
    }

    public static Int2IntOpenHashMap createNewInt2IntMap(int initialSize) {
        Int2IntOpenHashMap result = new Int2IntOpenHashMap(initialSize);
        result.defaultReturnValue(0);
        return result;
    }

    public static Int2IntOpenHashMap createNewInt2IntMap() {
        return createNewInt2IntMap(0);
    }

    public static Int2IntOpenHashMap computeMapTotals(Map<Integer, Int2IntOpenHashMap> countMaps) {
        Int2IntOpenHashMap totals = createNewInt2IntMap(countMaps.size());
        for (Map.Entry<Integer, Int2IntOpenHashMap> entry : countMaps.entrySet()) {
            int total = entry.getValue().values().stream().collect(Collectors.summingInt(i -> i));
            totals.put(entry.getKey().intValue(), total);
        }
        return totals;
    }

    public static int getTotal(Map<Integer, Int2IntOpenHashMap> counts) {
        return counts.values().stream().flatMap(map -> map.values().stream()).collect(Collectors.summingInt(i -> i));
    }
}
