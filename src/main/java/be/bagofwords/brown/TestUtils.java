package be.bagofwords.brown;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 05/12/14.
 */
public class TestUtils {

    public static void checkCounts(ContextCountsImpl clusterContextCounts, Int2IntOpenHashMap phraseToClusterMap, ContextCountsImpl phraseContextCounts) {
        Map<Integer, Int2IntOpenHashMap> computedPrevClusterCounts = new HashMap<>();
        Map<Integer, Int2IntOpenHashMap> computedNextClusterCounts = new HashMap<>();
        for (Integer phrase : phraseContextCounts.getAllPhrases()) {
            Integer cluster = phraseToClusterMap.get(phrase);
            Int2IntOpenHashMap prevCounts = phraseContextCounts.getPrevCounts(phrase);
            addCounts(phraseToClusterMap, computedPrevClusterCounts, cluster, prevCounts);
            Int2IntOpenHashMap nextCounts = phraseContextCounts.getNextCounts(phrase);
            addCounts(phraseToClusterMap, computedNextClusterCounts, cluster, nextCounts);
        }
        for (Integer cluster : clusterContextCounts.getAllClusters()) {
            compare(clusterContextCounts.getPrevCounts(cluster), computedPrevClusterCounts.get(cluster));
            compare(clusterContextCounts.getNextCounts(cluster), computedNextClusterCounts.get(cluster));
        }
    }

    private static void addCounts(Int2IntOpenHashMap phraseToClusterMap, Map<Integer, Int2IntOpenHashMap> clusterCounts, Integer cluster, Int2IntOpenHashMap phraseCounts) {
        Int2IntOpenHashMap clusterCountsForCluster = clusterCounts.get(cluster);
        if (clusterCountsForCluster == null) {
            clusterCountsForCluster = MapUtils.createNewInt2IntMap();
            clusterCounts.put(cluster, clusterCountsForCluster);
        }
        for (Map.Entry<Integer, Integer> entry : phraseCounts.entrySet()) {
            Integer cluster2 = phraseToClusterMap.get(entry.getKey());
            clusterCountsForCluster.addTo(cluster2, entry.getValue());
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
