package be.bagofwords.brown;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Set;

/**
* Created by Koen Deschacht (koendeschacht@gmail.com) on 03/12/14.
*/
public class SwapWordContextCounts implements ContextCounts {

    public static final int DUMMY_CLUSTER = -1;

    private final ContextCounts origContextCounts;
    private final ContextCounts contextCountsWithWord;
    private final int currCluster;

    public SwapWordContextCounts(ContextCounts origContextCounts, ContextCounts contextCountsWithWord, int currCluster) {
        this.origContextCounts = origContextCounts;
        this.contextCountsWithWord = contextCountsWithWord;
        this.currCluster = currCluster;
    }

    @Override
    public int getPrevTotal(int cluster) {
        if (cluster == DUMMY_CLUSTER) {
            return contextCountsWithWord.getPrevTotal(DUMMY_CLUSTER);
        } else if (cluster == currCluster) {
            return origContextCounts.getPrevTotal(currCluster) - contextCountsWithWord.getPrevTotal(DUMMY_CLUSTER);
        } else {
            return origContextCounts.getPrevTotal(cluster);
        }
    }

    @Override
    public int getNextTotal(int cluster) {
        if (cluster == DUMMY_CLUSTER) {
            return contextCountsWithWord.getNextTotal(DUMMY_CLUSTER);
        } else if (cluster == currCluster) {
            return origContextCounts.getNextTotal(currCluster) - contextCountsWithWord.getNextTotal(DUMMY_CLUSTER);
        } else {
            return origContextCounts.getNextTotal(cluster);
        }
    }

    @Override
    public int getGrandTotal() {
        return origContextCounts.getGrandTotal();
    }

    @Override
    public Set<Integer> getAllClusters() {
        return origContextCounts.getAllClusters();
    }

    @Override
    public Int2IntOpenHashMap getPrevCounts(int cluster) {
        if (cluster == DUMMY_CLUSTER) {
            return contextCountsWithWord.getPrevCounts(cluster);
        } else if (cluster == currCluster) {
            return swapCounts(reduceCounts(origContextCounts.getPrevCounts(cluster), contextCountsWithWord.getPrevCounts(DUMMY_CLUSTER)), contextCountsWithWord.getPrevCounts(cluster));
        } else {
            return swapCounts(origContextCounts.getPrevCounts(cluster), contextCountsWithWord.getPrevCounts(cluster));
        }
    }

    @Override
    public Int2IntOpenHashMap getNextCounts(int cluster) {
        if (cluster == DUMMY_CLUSTER) {
            return contextCountsWithWord.getNextCounts(cluster);
        } else if (cluster == currCluster) {
            return swapCounts(reduceCounts(origContextCounts.getNextCounts(cluster), contextCountsWithWord.getNextCounts(DUMMY_CLUSTER)), contextCountsWithWord.getNextCounts(cluster));
        } else {
            return swapCounts(origContextCounts.getNextCounts(cluster), contextCountsWithWord.getNextCounts(cluster));
        }
    }

    private Int2IntOpenHashMap reduceCounts(Int2IntOpenHashMap origCounts, Int2IntOpenHashMap countsToReduce) {
        if (countsToReduce != null) {
            origCounts = origCounts.clone();
            for (Int2IntMap.Entry entry : countsToReduce.int2IntEntrySet()) {
                int cluster = entry.getIntKey();
                if (cluster == DUMMY_CLUSTER) {
                    reduceValue(origCounts, currCluster, entry.getIntValue());
                } else {
                    reduceValue(origCounts, cluster, entry.getIntValue());
                }
            }
        }
        return origCounts;
    }

    private Int2IntOpenHashMap swapCounts(Int2IntOpenHashMap origCounts, Int2IntOpenHashMap countsToSwap) {
        if (countsToSwap != null) {
            if (countsToSwap.size() > 1) {
                throw new RuntimeException("Unexpected counts!");
            }
            int countToSwap = countsToSwap.get(DUMMY_CLUSTER);
            if (countToSwap > 0) {
                origCounts = origCounts.clone();
                origCounts.addTo(DUMMY_CLUSTER, countToSwap);
                reduceValue(origCounts, currCluster, countToSwap);
            }
        }
        return origCounts;
    }

    private void reduceValue(Int2IntOpenHashMap origCounts, int key, int countToReduce) {
        int oldValue = origCounts.addTo(key, -countToReduce);
        if (oldValue < countToReduce) {
            throw new RuntimeException("Unexpected count " + oldValue);
        }
    }

}
