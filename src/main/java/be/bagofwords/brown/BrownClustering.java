package be.bagofwords.brown;

import be.bagofwords.application.status.perf.ThreadSampleMonitor;
import be.bagofwords.ui.UI;
import be.bagofwords.util.NumUtils;
import be.bagofwords.util.Pair;
import be.bagofwords.util.Utils;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 02/12/14.
 * <p/>
 * Clustering algorithm of words and phrases described in:
 * Class Based n-gram Models of Natural Language, P.F. Brown, P.V. deSouza, R.L. Mercer, V.J.D. Pietra, J.C. Lai
 * http://people.csail.mit.edu/imcgraw/links/research/pubs/ClassBasedNGrams.pdf
 */

public class BrownClustering extends BaseWordClustering {

    public static void main(String[] args) throws IOException {
        String inputFile = "/home/koen/input_news_small.txt";
        String outputFile = "/home/koen/brown_output6.txt";
        int minFrequencyOfPhrase = 10;
        int maxNumberOfClusters = 1000;
        boolean onlySwapMostFrequentWords = true;
        long start = System.currentTimeMillis();
        new BrownClustering(inputFile, outputFile, minFrequencyOfPhrase, maxNumberOfClusters, onlySwapMostFrequentWords).run();
        long end = System.currentTimeMillis();
        UI.write("Took " + (end - start) + " ms.");
    }

    public static final boolean DO_TESTS = false; //you probably want to enable this during development

    private final int maxNumberOfClusters;
    private boolean onlySwapMostFrequentWords;
    private ExecutorService executorService = Executors.newFixedThreadPool(8);

    public BrownClustering(String inputFile, String outputFile, int minFrequencyOfPhrase, int maxNumberOfClusters, boolean onlySwapMostFrequentWords) {
        super(inputFile, outputFile, minFrequencyOfPhrase);
        this.maxNumberOfClusters = maxNumberOfClusters;
        this.onlySwapMostFrequentWords = onlySwapMostFrequentWords;
    }

    private void run() throws IOException {
        ThreadSampleMonitor threadSampleMonitor = new ThreadSampleMonitor(true, "threadSamples.txt", "brown-cluster");
        Pair<Map<Integer, String>, Int2IntOpenHashMap> readPhrases = readPhrases();
        Map<Integer, String> phraseMap = readPhrases.getFirst();
        Int2IntOpenHashMap phraseFrequencies = readPhrases.getSecond();
        UI.write("Read " + phraseMap.size() + " phrases.");
        ContextCountsImpl contextCounts = extractContextCounts(phraseMap);
        doClustering(phraseMap, contextCounts, phraseFrequencies);
        threadSampleMonitor.terminate();
        executorService.shutdown();
    }

    private void doClustering(Map<Integer, String> phraseMap, ContextCountsImpl phraseContextCounts, Int2IntOpenHashMap phraseFrequencies) throws IOException {
        Int2IntOpenHashMap phraseToClusterMap = initializeClusters(phraseMap.size());
        /**
         * STEP 1: create for every unique phrase a unique cluster
         */
        ContextCountsImpl clusterContextCounts = phraseContextCounts.clone(); //initially these counts are identical
        if (DO_TESTS) {
            TestUtils.checkCounts(clusterContextCounts, phraseToClusterMap, phraseContextCounts);
        }
        /**
         * STEP 2: for all phrases that are not among the maxNumberOfClusters frequent phrases, merge their corresponding cluster with a cluster corresponding to a frequent phrase
         */
        if (onlySwapMostFrequentWords) {
            int numOfFrequentPhrases = Math.min(phraseMap.size(), maxNumberOfClusters * 10);
            mergeInfrequentPhrasesWithFrequentPhraseClusters(maxNumberOfClusters, numOfFrequentPhrases, phraseToClusterMap, clusterContextCounts);
            swapPhrases(0, numOfFrequentPhrases, phraseToClusterMap, clusterContextCounts, phraseContextCounts);
            mergeInfrequentPhrasesWithFrequentPhraseClusters(numOfFrequentPhrases, phraseMap.size(), phraseToClusterMap, clusterContextCounts);
        } else {
            mergeInfrequentPhrasesWithFrequentPhraseClusters(maxNumberOfClusters, phraseMap.size(), phraseToClusterMap, clusterContextCounts);
            swapPhrases(0, phraseMap.size(), phraseToClusterMap, clusterContextCounts, phraseContextCounts);
        }
        if (DO_TESTS) {
            TestUtils.checkCounts(clusterContextCounts, phraseToClusterMap, phraseContextCounts);
        }
        /**
         * STEP 4: merge remaining clusters hierarchically
         */
        Map<Integer, ClusterHistoryNode> historyNodes = initializeHistoryNodes(phraseToClusterMap);
        iterativelyMergeClusters(historyNodes, clusterContextCounts);
        writeOutput(phraseMap, phraseToClusterMap, historyNodes, phraseFrequencies);
    }

    private void swapPhrases(int phraseStart, int phraseEnd, Int2IntOpenHashMap phraseToClusterMap, ContextCountsImpl clusterContextCounts, ContextCountsImpl phraseContextCounts) {
        boolean finished = false;
        while (!finished) {
            finished = true;
            for (int phrase = phraseStart; phrase < phraseEnd; phrase++) {
                int currCluster = phraseToClusterMap.get(phrase);
                ContextCountsImpl contextCountsForPhrase = mapPhraseCountsToClusterCounts(phrase, phraseToClusterMap, phraseContextCounts, SwapWordContextCounts.DUMMY_CLUSTER);
                SwapWordContextCounts swapWordContextCounts = new SwapWordContextCounts(clusterContextCounts, contextCountsForPhrase, currCluster);
                Pair<Integer, Double> bestClusterScore = findBestClusterToMerge(SwapWordContextCounts.DUMMY_CLUSTER, 0, maxNumberOfClusters, swapWordContextCounts);
                double oldScore = computeMergeScore(SwapWordContextCounts.DUMMY_CLUSTER, 0.0, currCluster, swapWordContextCounts);
                if (bestClusterScore.getFirst() != currCluster && bestClusterScore.getSecond() > oldScore + 1e-10) {
                    //if the best cluster is not the current one, we merge our counts
                    int newCluster = bestClusterScore.getFirst();
                    UI.write("Assigning phrase " + phrase + " to cluster " + newCluster + " (was cluster " + currCluster + ")");
                    phraseToClusterMap.put(phrase, newCluster);
                    clusterContextCounts.removeCounts(contextCountsForPhrase.mapCluster(SwapWordContextCounts.DUMMY_CLUSTER, currCluster));
                    clusterContextCounts.addCounts(contextCountsForPhrase.mapCluster(SwapWordContextCounts.DUMMY_CLUSTER, newCluster));
                    if (DO_TESTS) {
                        TestUtils.checkCounts(clusterContextCounts, phraseToClusterMap, phraseContextCounts);
                        checkSwapScores(phraseToClusterMap, clusterContextCounts, phraseContextCounts, phrase, currCluster, bestClusterScore, oldScore, newCluster);
                    }
                    finished = false;
                }
            }
        }
    }

    private void checkSwapScores(Int2IntOpenHashMap phraseToClusterMap, ContextCountsImpl clusterContextCounts, ContextCountsImpl phraseContextCounts, int phrase, int currCluster, Pair<Integer, Double> bestClusterScore, double oldScore, int newCluster) {
        ContextCountsImpl debugContextCountsForPhrase = mapPhraseCountsToClusterCounts(phrase, phraseToClusterMap, phraseContextCounts, SwapWordContextCounts.DUMMY_CLUSTER);
        SwapWordContextCounts debugSwapWordContextCounts = new SwapWordContextCounts(clusterContextCounts, debugContextCountsForPhrase, newCluster);
        double debugOldScore = computeMergeScore(SwapWordContextCounts.DUMMY_CLUSTER, 0.0, currCluster, debugSwapWordContextCounts);
        double debugNewScore = computeMergeScore(SwapWordContextCounts.DUMMY_CLUSTER, 0.0, newCluster, debugSwapWordContextCounts);
        if (!NumUtils.equal(debugOldScore, oldScore)) {
            throw new RuntimeException("Inconsistent score! " + oldScore + " " + debugOldScore);
        }
        if (!NumUtils.equal(debugNewScore, bestClusterScore.getSecond())) {
            throw new RuntimeException("Inconsistent score! " + bestClusterScore.getSecond() + " " + debugNewScore);
        }
    }

    private ContextCountsImpl mapPhraseCountsToClusterCounts(int phrase, Int2IntOpenHashMap phraseToClusterMap, ContextCounts phraseContextCounts, int newCluster) {
        Map<Integer, Int2IntOpenHashMap> prevClusterCounts = new HashMap<>();
        Map<Integer, Int2IntOpenHashMap> nextClusterCounts = new HashMap<>();
        addCounts(phraseToClusterMap, phraseContextCounts.getPrevCounts(phrase), prevClusterCounts, nextClusterCounts, phrase, true, newCluster);
        addCounts(phraseToClusterMap, phraseContextCounts.getNextCounts(phrase), nextClusterCounts, prevClusterCounts, phrase, false, newCluster);
        return new ContextCountsImpl(prevClusterCounts, nextClusterCounts);
    }

    private void addCounts(Int2IntOpenHashMap phraseToClusterMap, Int2IntOpenHashMap phraseContextCounts, Map<Integer, Int2IntOpenHashMap> prevClusterCounts, Map<Integer, Int2IntOpenHashMap> nextClusterCounts, int phrase, boolean includeIdentityCounts, int newCluster) {
        Int2IntOpenHashMap phrasePrevClusterCounts = prevClusterCounts.get(newCluster);
        if (phrasePrevClusterCounts == null) {
            phrasePrevClusterCounts = MapUtils.createNewInt2IntMap();
            prevClusterCounts.put(newCluster, phrasePrevClusterCounts);
        }
        for (Int2IntOpenHashMap.Entry otherPhraseEntry : phraseContextCounts.int2IntEntrySet()) {
            int otherPhrase = otherPhraseEntry.getIntKey();
            if (phrase != otherPhrase || includeIdentityCounts) {
                int clusterOtherPhrase = otherPhrase == phrase ? newCluster : phraseToClusterMap.get(otherPhrase);
                phrasePrevClusterCounts.addTo(clusterOtherPhrase, otherPhraseEntry.getIntValue());
                Int2IntOpenHashMap otherPhraseNextCounts = nextClusterCounts.get(clusterOtherPhrase);
                if (otherPhraseNextCounts == null) {
                    otherPhraseNextCounts = MapUtils.createNewInt2IntMap();
                    nextClusterCounts.put(clusterOtherPhrase, otherPhraseNextCounts);
                }
                otherPhraseNextCounts.addTo(newCluster, otherPhraseEntry.getValue());
            }
        }
    }

    private void iterativelyMergeClusters(Map<Integer, ClusterHistoryNode> nodes, ContextCountsImpl contextCounts) {
        nodes = new HashMap<>(nodes);
        List<MergeCandidate> mergeCandidates = computeAllScores(contextCounts);
        while (!mergeCandidates.isEmpty()) {
            MergeCandidate next = mergeCandidates.remove(mergeCandidates.size() - 1);
            int cluster1 = next.getCluster1();
            int cluster2 = next.getCluster2();
            UI.write("Will merge " + cluster1 + " with " + cluster2);
            contextCounts.mergeClusters(cluster1, cluster2);
            updateClusterNodes(nodes, cluster1, cluster2);
            removeMergeCandidates(mergeCandidates, cluster1);
            updateMergeCandidateScores(cluster2, mergeCandidates, contextCounts);
        }
    }

    private void removeMergeCandidates(List<MergeCandidate> mergeCandidates, int smallCluster) {
        mergeCandidates.removeIf(next -> next.getCluster1() == smallCluster || next.getCluster2() == smallCluster);
    }

    private List<MergeCandidate> computeAllScores(ContextCounts contextCounts) {
        List<MergeCandidate> mergeCandidates = Collections.synchronizedList(new ArrayList<>());
        Set<Integer> allClusters = contextCounts.getAllClusters();
        allClusters.parallelStream().forEach(cluster1 -> {
            double ski = computeSK(cluster1, contextCounts);
            for (Integer cluster2 : allClusters) {
                if (cluster1 < cluster2) {
                    double score = computeMergeScore(cluster1, ski, cluster2, contextCounts);
                    mergeCandidates.add(new MergeCandidate(cluster1, cluster2, score));
                }
            }
        });
        Collections.sort(mergeCandidates);
        return mergeCandidates;
    }

    private void updateMergeCandidateScores(int cluster2, List<MergeCandidate> mergeCandidates, ContextCounts contextCounts) {
        double skj = computeSK(cluster2, contextCounts);
        MutableInt numberOfCandidatesRemaining = new MutableInt(0);
        for (MergeCandidate mergeCandidate : mergeCandidates) {
            if (mergeCandidate.getCluster2() == cluster2) {
                synchronized (numberOfCandidatesRemaining) {
                    numberOfCandidatesRemaining.increment();
                }
                executorService.submit(() -> {
                    double ski = computeSK(mergeCandidate.getCluster1(), contextCounts);
                    mergeCandidate.setScore(computeMergeScore(mergeCandidate.getCluster1(), ski, mergeCandidate.getCluster2(), skj, contextCounts));
                    synchronized (numberOfCandidatesRemaining) {
                        numberOfCandidatesRemaining.decrement();
                    }
                });
            }
        }
        while (numberOfCandidatesRemaining.getValue() > 0) {
            Utils.threadSleep(1);
        }
        Collections.sort(mergeCandidates);
    }

    private void mergeInfrequentPhrasesWithFrequentPhraseClusters(int startPhrase, int endPhrase, Int2IntOpenHashMap phraseToClusterMap, ContextCountsImpl clusterContextCounts) {
        for (int phrase = startPhrase; phrase < endPhrase; phrase++) {
            int newCluster = findBestClusterToMerge(phrase, 0, maxNumberOfClusters, clusterContextCounts).getFirst();
            UI.write("Will merge cluster " + phrase + " with " + newCluster);
            clusterContextCounts.mergeClusters(phrase, newCluster);
            phraseToClusterMap.put(phrase, newCluster);
        }
    }

    private Pair<Integer, Double> findBestClusterToMerge(int origCluster, int minCluster, int maxCluster, ContextCounts clusterContextCounts) {
        Object syncLock = new Object();
        MutableDouble bestScore = new MutableDouble(-Double.MAX_VALUE);
        MutableInt bestCluster = new MutableInt(-1);
        MutableInt numOfClustersRemaining = new MutableInt(0);
        for (Integer cluster : clusterContextCounts.getAllClusters()) {
            if (cluster >= minCluster && cluster < maxCluster && cluster != origCluster) {
                synchronized (numOfClustersRemaining) {
                    numOfClustersRemaining.increment();
                }
                executorService.submit(() -> {
                    double score = computeMergeScore(origCluster, 0.0, cluster, clusterContextCounts);
                    if (score > bestScore.doubleValue()) {
                        synchronized (syncLock) {
                            if (score > bestScore.doubleValue()) { //bestScore might have changed while we acquiring the lock
                                bestScore.setValue(score);
                                bestCluster.setValue(cluster);
                            }
                        }
                    }
                    synchronized (numOfClustersRemaining) {
                        numOfClustersRemaining.decrement();
                    }
                });
            }
        }
        while (numOfClustersRemaining.getValue() > 0) {
            Utils.threadSleep(1);
        }
        return new Pair<>(bestCluster.intValue(), bestScore.doubleValue());
    }

    /**
     * see top of page 7 of [Brown et al.].
     */

    private double computeMergeScore(int cki, double ski, int ckj, ContextCounts contextCounts) {
        return computeMergeScore(cki, ski, ckj, computeSK(ckj, contextCounts), contextCounts);
    }

    private double computeMergeScore(int cki, double ski, int ckj, double skj, ContextCounts originalCounts) {
        MergedContextCounts mergedCounts = new MergedContextCounts(cki, ckj, originalCounts);
        double result = -ski - skj;
        result += computeSK(ckj, mergedCounts);
        return result;
    }

    private double computeSK(int cluster, ContextCounts contextCounts) {
        double sk = 0;
        double grandTotal = contextCounts.getGrandTotal();
        int prevTotal = contextCounts.getPrevTotal(cluster);
        for (Int2IntOpenHashMap.Entry entry : contextCounts.getPrevCounts(cluster).int2IntEntrySet()) {
            sk += computeQK(entry.getIntValue(), contextCounts.getNextTotal(entry.getIntKey()), prevTotal, grandTotal);
        }
        Int2IntOpenHashMap nextCounts = contextCounts.getNextCounts(cluster);
        int nextTotal = contextCounts.getNextTotal(cluster) - nextCounts.get(cluster);
        for (Int2IntOpenHashMap.Entry entry : nextCounts.int2IntEntrySet()) {
            if (entry.getIntKey() != cluster) {
                sk += computeQK(entry.getIntValue(), nextTotal, contextCounts.getPrevTotal(entry.getIntKey()), grandTotal);
            }
        }
        return sk;
    }

    private double computeQK(int jointCounts, int totalCki, int totalCkj, double grandTotal) {
        if (jointCounts > 0) {
            double pklm = jointCounts / grandTotal;
            double plkl = totalCki / grandTotal;
            double prkm = totalCkj / grandTotal;
            if (DO_TESTS) {
                checkProbability(pklm);
                checkProbability(plkl);
                checkProbability(prkm);
                if (plkl == 0 || prkm == 0) {
                    throw new RuntimeException("Illegal probabilities!");
                }
            }
            return pklm * Math.log(pklm / (plkl * prkm));
        } else {
            return 0.0;
        }
    }


    private void checkProbability(double probability) {
        if (probability < 0 || probability > 1 || Double.isNaN(probability)) {
            throw new RuntimeException("Illegal probability " + probability);
        }
    }

    private Int2IntOpenHashMap initializeClusters(int numberOfPhrases) {
        Int2IntOpenHashMap phraseToCluster = MapUtils.createNewInt2IntMap(numberOfPhrases);
        for (int i = 0; i < numberOfPhrases; i++) {
            phraseToCluster.put(i, i); //assign every word to its own cluster
        }
        return phraseToCluster;
    }

}

