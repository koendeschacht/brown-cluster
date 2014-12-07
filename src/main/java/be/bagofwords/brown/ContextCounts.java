package be.bagofwords.brown;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Set;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 03/12/14.
 */
public interface ContextCounts {

    int getPrevTotal(int cluster);

    int getNextTotal(int cluster);

    int getGrandTotal();

    Set<Integer> getAllClusters();

    Int2IntOpenHashMap getPrevCounts(int cluster);

    Int2IntOpenHashMap getNextCounts(int cluster);
}
