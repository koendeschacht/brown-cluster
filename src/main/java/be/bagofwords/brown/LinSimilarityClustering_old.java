package be.bagofwords.brown;

import be.bagofwords.application.status.perf.ThreadSampleMonitor;
import be.bagofwords.ui.UI;
import be.bagofwords.util.MappedLists;
import be.bagofwords.util.Pair;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 02/12/14.
 * <p/>
 */

public class LinSimilarityClustering_old extends BaseWordClustering {

    public static void main(String[] args) throws IOException {
        String inputFile = "/home/koen/input_news_small.txt";
        String outputFile = "/home/koen/brown_output5.txt";
        int minFrequencyOfPhrase = 10;
        int numberOfClusters = 1000;
        long start = System.currentTimeMillis();
        new LinSimilarityClustering_old(inputFile, outputFile, numberOfClusters, minFrequencyOfPhrase).run();
        long end = System.currentTimeMillis();
        UI.write("Took " + (end - start) + " ms.");
    }

    public static final boolean DO_TESTS = true; //you probably want to enable this during development

    private final int numberOfClusters;

    public LinSimilarityClustering_old(String textInputFile, String outputFile, int numberOfClusters, int minFrequencyOfPhrase) {
        super(textInputFile, outputFile, minFrequencyOfPhrase);
        this.numberOfClusters = numberOfClusters;
    }

    private void run() throws IOException {
        ThreadSampleMonitor threadSampleMonitor = new ThreadSampleMonitor(true, "threadSamples.txt", "brown-cluster");
        Pair<Map<Integer, String>, Int2IntOpenHashMap> readPhrases = readPhrases();
        Map<Integer, String> phraseMap = readPhrases.getFirst();
        Int2IntOpenHashMap phraseFrequencies = readPhrases.getSecond();
        UI.write("Read " + phraseMap.size() + " phrases. Computing similarities...");
        Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities = computePhraseSimilarities(phraseMap);
        printSims(phraseMap, phraseSimilarities, "euro");
        printSims(phraseMap, phraseSimilarities, "woensdagochtend");
        printSims(phraseMap, phraseSimilarities, "avond");
        UI.write("Computing clusters");
        doClustering(phraseMap, phraseSimilarities, phraseFrequencies);
        threadSampleMonitor.terminate();
    }

    private void printSims(Map<Integer, String> phraseMap, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, String word) {
        Integer ind = null;
        for (Map.Entry<Integer, String> entry : phraseMap.entrySet()) {
            if (entry.getValue().equals(word)) {
                ind = entry.getKey();
            }
        }
        if (ind == null) {
            UI.write("Did not find word " + word);
        } else {
            UI.write("Printing similarities for " + word + " (" + ind + ")");
            Int2DoubleOpenHashMap simsForWord = phraseSimilarities.get(ind);
            List<Pair<Double, Integer>> sims = simsForWord.entrySet().stream().map(entry -> new Pair<>(entry.getValue(), entry.getKey())).sorted(Collections.reverseOrder()).collect(Collectors.toList());
            for (int i = 0; i < sims.size() && i < 10; i++) {
                UI.write("\t" + phraseMap.get(sims.get(i).getSecond()) + " " + sims.get(i).getFirst());
            }
        }
    }

    private void doClustering(Map<Integer, String> phraseMap, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies) throws IOException {
        Int2IntOpenHashMap phraseToClusterMap = initializeRandomClusters(phraseMap.size());
        MappedLists<Integer, Integer> clusterToPhrasesMap = computeClusterToPhraseMap(phraseToClusterMap);
        swapPhrases(phraseToClusterMap, clusterToPhrasesMap, phraseSimilarities, phraseFrequencies);
        Map<Integer, ClusterHistoryNode> historyNodes = initializeHistoryNodes(phraseToClusterMap);
        iterativelyMergeClusters(historyNodes, clusterToPhrasesMap, phraseSimilarities, phraseFrequencies, phraseMap);
        writeOutput(phraseMap, phraseToClusterMap, historyNodes, phraseFrequencies);
    }

    private void swapPhrases(Int2IntOpenHashMap phraseToClusterMap, MappedLists<Integer, Integer> clusterToPhrasesMap, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies) {
        boolean finished = false;
        while (!finished) {
            double clusterScore = getClusterScore(clusterToPhrasesMap, phraseSimilarities, phraseFrequencies);
            UI.write("cluster score " + clusterScore);
            for (Map.Entry<Integer, List<Integer>> entry : clusterToPhrasesMap.entrySet()) {
                int cluster = entry.getKey();
                List<Integer> oldPhrasesInClusters = new ArrayList<>(entry.getValue());
                for (Integer phrase : oldPhrasesInClusters) {
                    entry.getValue().remove(phrase);
                    MutableDouble bestScore = new MutableDouble(-Double.MAX_VALUE);
                    MutableInt bestCluster = new MutableInt(cluster);
                    findBestCluster(clusterToPhrasesMap, phraseSimilarities, phraseFrequencies, phrase, bestScore, bestCluster);
                    Integer newCluster = bestCluster.getValue();
                    if (newCluster != cluster) {
//                        UI.write("swapping phrase " + phrase + " from " + cluster + " to " + newCluster);
                        clusterToPhrasesMap.get(newCluster).add(phrase);
                        phraseToClusterMap.put(phrase, newCluster);
//                        double newScore = getClusterScore(clusterToPhrasesMap, phraseSimilarities, phraseFrequencies);
//                        if (newScore < clusterScore) {
//                            UI.write("score lowered from " + clusterScore + " to" + newScore);
//                            findBestCluster(clusterToPhrasesMap, phraseSimilarities, phraseFrequencies, phrase, bestScore, bestCluster);
//                        }
//                        clusterScore = newScore;
                    } else {
                        //add back to cluster
                        entry.getValue().add(phrase);
                    }
                }
            }
            double newScore = getClusterScore(clusterToPhrasesMap, phraseSimilarities, phraseFrequencies);
            UI.write("New score " + newScore);
            finished = newScore < clusterScore * 1.001;
        }
    }

    private void findBestCluster(MappedLists<Integer, Integer> clusterToPhrasesMap, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies, Integer phrase, MutableDouble bestScore, MutableInt bestCluster) {
        clusterToPhrasesMap.entrySet().parallelStream().forEach(otherCluster -> {
            double averageSim = getAverageSimiliarityWithPhrasesInCluster(phraseSimilarities, phraseFrequencies, otherCluster.getValue());
            List<Integer> newCluster = new ArrayList<>(otherCluster.getValue());
            newCluster.add(phrase);
            double newAverageSim = getAverageSimiliarityWithPhrasesInCluster(phraseSimilarities, phraseFrequencies, newCluster);
            double score = newAverageSim - averageSim;



            if (score > bestScore.getValue()) {
                synchronized (bestScore) {
                    if (score > bestScore.getValue()) {
                        bestScore.setValue(score);
                        bestCluster.setValue(otherCluster.getKey());
                    }
                }
            }
        });
    }


    private double getAverageSimiliarityWithPhrasesInCluster(Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies, List<Integer> phrases) {
        double result = 0;
        for (Integer phrase1 : phrases) {
            double averageSimilarityToItemsInCluster = 0;
            double sumOfSims = 0;
            for (Integer phrase2 : phrases) {
                int frequency = phraseFrequencies.get(phrase2);
                averageSimilarityToItemsInCluster += getPhraseSimilarity(phrase1, phrase2, phraseSimilarities) * frequency;
                sumOfSims += frequency;
            }
            averageSimilarityToItemsInCluster /= sumOfSims;
            result += averageSimilarityToItemsInCluster;
        }
        return result;
    }

    private double getClusterScore(MappedLists<Integer, Integer> clusterToPhrasesMap, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies) {
        double score = 0;
        for (List<Integer> phrases : clusterToPhrasesMap.values()) {
            score += getAverageSimiliarityWithPhrasesInCluster(phraseSimilarities, phraseFrequencies, phrases);
        }
        return score / phraseFrequencies.size();
    }

    private void iterativelyMergeClusters(Map<Integer, ClusterHistoryNode> nodes, MappedLists<Integer, Integer> clusterToPhrasesMap, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies, Map<Integer, String> phraseMap) {
        nodes = new HashMap<>(nodes);
        List<MergeCandidate> mergeCandidates = computeAllScores(clusterToPhrasesMap, phraseSimilarities, phraseFrequencies);
        while (!mergeCandidates.isEmpty()) {
            MergeCandidate next = mergeCandidates.remove(mergeCandidates.size() - 1);
            int cluster1 = next.getCluster1();
            int cluster2 = next.getCluster2();
//            UI.write("Will merge " + cluster1 + " with " + cluster2 + " " + mergeCandidates.size() + " remaining");
//            printCluster(cluster1, clusterToPhrasesMap, phraseMap);
//            printCluster(cluster2, clusterToPhrasesMap, phraseMap);
            clusterToPhrasesMap.get(cluster2).addAll(clusterToPhrasesMap.remove(cluster1));
            updateClusterNodes(nodes, cluster1, cluster2);
            removeMergeCandidates(mergeCandidates, cluster1);
            updateMergeCandidateScores(cluster2, mergeCandidates, clusterToPhrasesMap, phraseSimilarities, phraseFrequencies);
        }
    }

    private void printCluster(int cluster, MappedLists<Integer, Integer> clusterToPhrasesMap, Map<Integer, String> phraseMap) {
        UI.write("--- cluster " + cluster + " -----");
        List<Integer> phrasesInCluster = new ArrayList<>(clusterToPhrasesMap.get(cluster));
        Collections.sort(phrasesInCluster);
        for (int i = 0; i < phrasesInCluster.size() && i < 10; i++) {
            Integer phrase = phrasesInCluster.get(i);
            UI.write("\t" + phraseMap.get(phrase) + " " + phrase);
        }
    }

    private void removeMergeCandidates(List<MergeCandidate> mergeCandidates, int smallCluster) {
        mergeCandidates.removeIf(next -> next.getCluster1() == smallCluster || next.getCluster2() == smallCluster);
    }

    private void updateMergeCandidateScores(int cluster2, List<MergeCandidate> mergeCandidates, MappedLists<Integer, Integer> clusterToPhrasesMap, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies) {
        mergeCandidates.parallelStream().forEach(candidate -> {
                    if (candidate.getCluster2() == cluster2) {
                        double sim = computeClusterSimilarity(clusterToPhrasesMap.get(candidate.getCluster1()), clusterToPhrasesMap.get(candidate.getCluster2()), phraseSimilarities, phraseFrequencies);
                        candidate.setScore(sim);
                    }
                }
        );
        Collections.sort(mergeCandidates);
    }

    private List<MergeCandidate> computeAllScores(MappedLists<Integer, Integer> clusterToPhrasesMap, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies) {
        List<MergeCandidate> result = new ArrayList<>();
        for (Integer cluster1 : clusterToPhrasesMap.keySet()) {
            for (Integer cluster2 : clusterToPhrasesMap.keySet()) {
                if (cluster1 < cluster2) {
                    double sim = computeClusterSimilarity(clusterToPhrasesMap.get(cluster1), clusterToPhrasesMap.get(cluster2), phraseSimilarities, phraseFrequencies);
                    result.add(new MergeCandidate(cluster1, cluster2, sim));
                }
            }
        }
        Collections.sort(result);
        return result;
    }

    private double computeClusterSimilarity(List<Integer> phrasesCluster1, List<Integer> phrasesCluster2, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies) {
        if (phrasesCluster1.isEmpty() || phrasesCluster2.isEmpty()) {
            return 1.0;
        } else {
            List<Integer> mergedCluster = new ArrayList<>(phrasesCluster1);
            mergedCluster.addAll(phrasesCluster2);
            double result = computeSumOfSimilarities(mergedCluster, phraseSimilarities, phraseFrequencies);
            result -= computeSumOfSimilarities(phrasesCluster1, phraseSimilarities, phraseFrequencies);
            result -= computeSumOfSimilarities(phrasesCluster2, phraseSimilarities, phraseFrequencies);
            return result;
        }
    }

    private double computeSumOfSimilarities(List<Integer> cluster, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities, Int2IntOpenHashMap phraseFrequencies) {
        double sumOfSimilarities = 0;
        for (Integer phrase1 : cluster) {
            double averageSim = 0;
            double sum = 0;
            for (Integer phrase2 : cluster) {
                double frequency = phraseFrequencies.get(phrase2.intValue());
                averageSim += getPhraseSimilarity(phrase1, phrase2, phraseSimilarities) * frequency;
                sum += frequency;
            }
            averageSim /= sum;
            sumOfSimilarities += averageSim;
        }
        return sumOfSimilarities;
    }

    private double getPhraseSimilarity(Integer phrase1, Integer phrase2, Map<Integer, Int2DoubleOpenHashMap> phraseSimilarities) {
        if (phrase1 < phrase2) {
            return phraseSimilarities.get(phrase1).get(phrase2.intValue());
        } else if (phrase1 > phrase2) {
            return phraseSimilarities.get(phrase2).get(phrase1.intValue());
        } else {
            return 1.0;
        }
    }

    private MappedLists<Integer, Integer> computeClusterToPhraseMap(Int2IntOpenHashMap phraseToClusterMap) {
        MappedLists<Integer, Integer> result = new MappedLists<>();
        for (Map.Entry<Integer, Integer> entry : phraseToClusterMap.entrySet()) {
            result.get(entry.getValue()).add(entry.getKey());
        }
        return result;
    }


    private Int2IntOpenHashMap initializeRandomClusters(int numberOfPhrases) {
        Int2IntOpenHashMap phraseToCluster = MapUtils.createNewInt2IntMap(numberOfPhrases);
        for (int i = 0; i < numberOfPhrases; i++) {
            phraseToCluster.put(i, i % numberOfClusters); //assign every word to its own cluster
        }
        return phraseToCluster;
    }

    private Map<Integer, Int2DoubleOpenHashMap> computePhraseSimilarities(Map<Integer, String> phraseMap) throws IOException {
        ContextCountsImpl contextCounts = extractContextCounts(phraseMap);
        Map<Integer, Int2DoubleOpenHashMap> result = new HashMap<>();
        for (Integer phrase : phraseMap.keySet()) {
            result.put(phrase, new Int2DoubleOpenHashMap());
        }
        int numTodo = phraseMap.size();
        MutableInt numDone = new MutableInt(0);
        phraseMap.keySet().parallelStream().forEach(phrase1 -> {
            /**
             * prev context
             */
            {
                for (Int2IntMap.Entry entry : contextCounts.getPrevCounts(phrase1).int2IntEntrySet()) {
                    int prevContext = entry.getIntKey();
                    double info = -Math.log(1.0 / contextCounts.getNextTotal(prevContext));
                    double probPhrase1 = entry.getIntValue() / (double) contextCounts.getPrevTotal(phrase1);
                    for (Int2IntMap.Entry innerEntry : contextCounts.getNextCounts(prevContext).int2IntEntrySet()) {
                        int phrase2 = innerEntry.getIntKey();
                        if (phrase1 <= phrase2) {
                            double probPhrase2 = innerEntry.getIntValue() / (double) contextCounts.getPrevTotal(phrase2);
                            double shared = Math.min(probPhrase1, probPhrase2) * info;
                            addSimilarity(phrase1, phrase2, shared, result);
                        }
                    }
                }
            }
            /**
             * next context
             */
            {
                for (Int2IntMap.Entry entry : contextCounts.getNextCounts(phrase1).int2IntEntrySet()) {
                    int nextContext = entry.getIntKey();
                    double info = -Math.log(1.0 / contextCounts.getPrevTotal(nextContext));
                    double probPhrase1 = entry.getIntValue() / (double) contextCounts.getNextTotal(phrase1);
                    for (Int2IntMap.Entry innerEntry : contextCounts.getPrevCounts(nextContext).int2IntEntrySet()) {
                        int phrase2 = innerEntry.getIntKey();
                        if (phrase1 <= phrase2) {
                            double probPhrase2 = innerEntry.getIntValue() / (double) contextCounts.getNextTotal(phrase2);
                            double shared = Math.min(probPhrase1, probPhrase2) * info;
                            addSimilarity(phrase1, phrase2, shared, result);
                        }
                    }
                }
            }
            numDone.increment();
            if (numDone.intValue() % 1000 == 0) {
                UI.write("Did " + (Math.round(numDone.intValue() * 1000.0 / numTodo) / 10.0) + "%");
            }
        });
        Int2DoubleOpenHashMap totals = new Int2DoubleOpenHashMap(phraseMap.size());
        for (Map.Entry<Integer, Int2DoubleOpenHashMap> entry : result.entrySet()) {
            totals.addTo(entry.getKey(), entry.getValue().get(entry.getKey()));
        }
        result.entrySet().parallelStream().forEach(entry -> {
            for (Int2DoubleMap.Entry innerEntry : entry.getValue().int2DoubleEntrySet()) {
                innerEntry.setValue(2 * innerEntry.getValue() / (totals.get(entry.getKey()) + totals.get(innerEntry.getIntKey())));
            }
            entry.getValue().trim();
        });
        return result;
    }

    private void addSimilarity(Integer phrase1, Integer phrase2, double similarity, Map<Integer, Int2DoubleOpenHashMap> result) {
        Int2DoubleOpenHashMap simForPhrase1 = result.get(phrase1);
        synchronized (simForPhrase1) {
            simForPhrase1.addTo(phrase2, similarity);
        }
    }

    private double computePhraseSimilarity(Integer phrase1, Integer phrase2, ContextCountsImpl contextCounts) {
        double totalScorePhrase1 = 0;
        double totalScorePhrase2 = 0;
        double totalSharedScore = 0;
        /**
         * Previous contexts:
         */
        {
            double totalPrevPhrase1 = contextCounts.getPrevTotal(phrase1);
            double totalPrevPhrase2 = contextCounts.getPrevTotal(phrase2);
            for (Int2IntMap.Entry entry : contextCounts.getPrevCounts(phrase1).int2IntEntrySet()) {
                double info = -Math.log(1.0 / contextCounts.getNextTotal(entry.getIntKey()));
                double probPhrase1 = entry.getIntValue() / totalPrevPhrase1;
                double probPhrase2 = totalPrevPhrase2 == 0 ? 0 : contextCounts.getPrevCounts(phrase2).get(entry.getIntKey()) / totalPrevPhrase2;
                double sharedProb = Math.min(probPhrase1, probPhrase2);
                totalScorePhrase1 += probPhrase1 * info;
                totalSharedScore += sharedProb * info;
            }
            for (Int2IntMap.Entry entry : contextCounts.getPrevCounts(phrase2).int2IntEntrySet()) {
                double info = -Math.log(1.0 / contextCounts.getNextTotal(entry.getIntKey()));
                double probPhrase2 = entry.getIntValue() / totalPrevPhrase2;
                totalScorePhrase2 += probPhrase2 * info;
            }
        }
        /**
         * Next contexts:
         */
        {
            double totalNextPhrase1 = contextCounts.getNextTotal(phrase1);
            double totalNextPhrase2 = contextCounts.getNextTotal(phrase2);
            for (Int2IntMap.Entry entry : contextCounts.getNextCounts(phrase1).int2IntEntrySet()) {
                double info = -Math.log(1.0 / contextCounts.getPrevTotal(entry.getIntKey()));
                double probPhrase1 = entry.getIntValue() / totalNextPhrase1;
                double probPhrase2 = totalNextPhrase2 == 0 ? 0 : contextCounts.getNextCounts(phrase2).get(entry.getIntKey()) / totalNextPhrase2;
                double sharedProb = Math.min(probPhrase1, probPhrase2);
                totalScorePhrase1 += probPhrase1 * info;
                totalSharedScore += sharedProb * info;
            }
            for (Int2IntMap.Entry entry : contextCounts.getNextCounts(phrase2).int2IntEntrySet()) {
                double info = -Math.log(1.0 / contextCounts.getPrevTotal(entry.getIntKey()));
                double probPhrase2 = entry.getIntValue() / totalNextPhrase2;
                totalScorePhrase2 += probPhrase2 * info;
            }
        }
        double sim = 2 * totalSharedScore / (totalScorePhrase1 + totalScorePhrase2);
        return sim;
    }

}

