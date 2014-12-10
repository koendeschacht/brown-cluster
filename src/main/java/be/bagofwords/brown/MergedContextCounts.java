package be.bagofwords.brown;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Set;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 03/12/14.
 */
public class MergedContextCounts implements ContextCounts {

    private final int smallCluster;
    private final int largeCluster;
    private final ContextCounts contextCounts;

    public MergedContextCounts(int smallCluster, int largeCluster, ContextCounts contextCounts) {
        this.smallCluster = smallCluster;
        this.largeCluster = largeCluster;
        this.contextCounts = contextCounts;
    }

    @Override
    public int getPrevTotal(int cluster) {
        if (cluster == smallCluster) {
            return 0;
        } else if (cluster == largeCluster) {
            return contextCounts.getPrevTotal(smallCluster) + contextCounts.getPrevTotal(largeCluster);
        } else {
            return contextCounts.getPrevTotal(cluster);
        }
    }

    @Override
    public int getNextTotal(int cluster) {
        if (cluster == smallCluster) {
            return 0;
        } else if (cluster == largeCluster) {
            return contextCounts.getNextTotal(smallCluster) + contextCounts.getNextTotal(largeCluster);
        } else {
            return contextCounts.getNextTotal(cluster);
        }
    }

    @Override
    public int getGrandTotal() {
        return contextCounts.getGrandTotal();
    }

    @Override
    public Set<Integer> getAllClusters() {
        return contextCounts.getAllClusters();
    }

    @Override
    public Int2IntOpenHashMap getPrevCounts(int cluster) {
        Int2IntOpenHashMap result;
        if (cluster == smallCluster) {
            return MapUtils.createNewInt2IntMap();
        } else if (cluster == largeCluster) {
            result = merge(contextCounts.getPrevCounts(smallCluster), contextCounts.getPrevCounts(largeCluster));
        } else {
            result = contextCounts.getPrevCounts(cluster);
        }
        result = replace(result, smallCluster, largeCluster);
        return result;
    }

    @Override
    public Int2IntOpenHashMap getNextCounts(int cluster) {
        Int2IntOpenHashMap result;
        if (cluster == smallCluster) {
            return MapUtils.createNewInt2IntMap();
        } else if (cluster == largeCluster) {
            result = merge(contextCounts.getNextCounts(smallCluster), contextCounts.getNextCounts(largeCluster));
        } else {
            result = contextCounts.getNextCounts(cluster);
        }
        result = replace(result, smallCluster, largeCluster);
        return result;
    }

    private Int2IntOpenHashMap replace(Int2IntOpenHashMap result, int smallCluster, int largeCluster) {
        int countsSmallCluster = result.get(smallCluster);
        if (countsSmallCluster > 0) {
            result = result.clone();
            result.remove(smallCluster);
            result.addTo(largeCluster, countsSmallCluster);
        }
        return result;
    }

    private Int2IntOpenHashMap merge(Int2IntOpenHashMap counts1, Int2IntOpenHashMap counts2) {
        Int2IntOpenHashMap large = counts1.size() > counts2.size() ? counts1 : counts2;
        Int2IntOpenHashMap small = counts1.size() > counts2.size() ? counts2 : counts1;
        Int2IntOpenHashMap result = large.clone();
        for (Int2IntOpenHashMap.Entry entry : small.int2IntEntrySet()) {
            result.addTo(entry.getIntKey(), entry.getIntValue());
        }
        return result;
    }

}
