package be.bagofwords.brown;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 05/12/14.
 */
public class TestUtils {

    public static void checkCounts(ContextCountsImpl clusterContextCounts, Int2IntOpenHashMap phraseToClusterMap, ContextCountsImpl phraseContextCounts) {
        ContextCounts computedContextCounts = MapUtils.computeContextCounts(phraseContextCounts, phraseToClusterMap);
        for (Integer cluster : clusterContextCounts.getAllClusters()) {
            compare(clusterContextCounts.getPrevCounts(cluster), computedContextCounts.getPrevCounts(cluster));
            compare(clusterContextCounts.getNextCounts(cluster), computedContextCounts.getNextCounts(cluster));
        }
    }

    private static void compare(Int2IntOpenHashMap counts1, Int2IntOpenHashMap counts2) {
        for (Int2IntMap.Entry entry : counts1.int2IntEntrySet()) {
            if (counts2.get(entry.getIntKey()) != entry.getIntValue()) {
                throw new RuntimeException("Incorrect counts!");
            }
        }
        for (Int2IntMap.Entry entry : counts2.int2IntEntrySet()) {
            if (counts1.get(entry.getIntKey()) != entry.getIntValue()) {
                throw new RuntimeException("Incorrect counts!");
            }
        }
    }
}
