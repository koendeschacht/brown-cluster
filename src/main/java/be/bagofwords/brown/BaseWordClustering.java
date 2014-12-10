package be.bagofwords.brown;

import be.bagofwords.util.Pair;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.*;
import java.util.*;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 02/12/14.
 */

public abstract class BaseWordClustering {

    private static final String UNKNOWN_PHRASE = "_UNKNOWN_";

    protected final String inputFile;
    protected final String outputFile;
    protected final int minFrequencyOfPhrase;

    public BaseWordClustering(String inputFile, String outputFile, int minFrequencyOfPhrase) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.minFrequencyOfPhrase = minFrequencyOfPhrase;
    }


    protected void writeOutput(Map<Integer, String> phraseMap, Int2IntOpenHashMap phraseToClusterMap, Map<Integer, ClusterHistoryNode> nodes, Int2IntOpenHashMap phraseFrequencies) throws IOException {
        List<String> outputLines = new ArrayList<>();
        for (Integer phraseInd : phraseToClusterMap.keySet()) {
            String phrase = phraseMap.get(phraseInd);
            String output = "";
            ClusterHistoryNode node = nodes.get(phraseToClusterMap.get(phraseInd));
            while (node != null) {
                ClusterHistoryNode parent = node.getParent();
                if (parent != null) {
                    if (parent.getLeftChild() == node) {
                        output = '0' + output;
                    } else {
                        output = '1' + output;
                    }
                }
                node = parent;
            }
            outputLines.add(output + '\t' + phrase + "\t" + phraseFrequencies.get(phraseInd));
        }
        Collections.sort(outputLines);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        for (String line : outputLines) {
            writer.write(line);
            writer.write('\n');
        }
        writer.close();
    }

    protected Map<Integer, ClusterHistoryNode> initializeHistoryNodes(Int2IntOpenHashMap phraseToClusterMap) {
        Map<Integer, ClusterHistoryNode> result = new HashMap<>();
        for (Integer cluster : phraseToClusterMap.values()) {
            result.put(cluster, new ClusterHistoryNode(cluster));
        }
        return result;
    }

    protected void updateClusterNodes(Map<Integer, ClusterHistoryNode> nodes, int smallCluster, int largeCluster) {
        ClusterHistoryNode parent = new ClusterHistoryNode(largeCluster);
        parent.setChildren(nodes.remove(smallCluster), nodes.get(largeCluster));
        nodes.put(largeCluster, parent);
    }

    protected ContextCountsImpl extractContextCounts(Map<Integer, String> phraseMap) throws IOException {
        Map<String, Integer> invertedPhraseMap = invert(phraseMap); //mapping of words to their index
        Map<Integer, Int2IntOpenHashMap> prevContextCounts = createEmptyCounts(phraseMap.size());
        Map<Integer, Int2IntOpenHashMap> nextContextCounts = createEmptyCounts(phraseMap.size());
        BufferedReader rdr = new BufferedReader(new FileReader(inputFile));
        while (rdr.ready()) {
            String line = rdr.readLine();
            List<String> phrases = splitLineInPhrases(line);
            Integer prevPhrase = null;
            for (String phrase : phrases) {
                Integer currPhrase = invertedPhraseMap.get(phrase);
                if (currPhrase == null) {
                    //infrequent phrase
                    currPhrase = invertedPhraseMap.get(UNKNOWN_PHRASE);
                }
                if (prevPhrase != null) {
                    nextContextCounts.get(prevPhrase).addTo(currPhrase, 1);
                    prevContextCounts.get(currPhrase).addTo(prevPhrase, 1);
                }
                prevPhrase = currPhrase;
            }
        }
        rdr.close();
        trimCounts(prevContextCounts);
        trimCounts(nextContextCounts);
        return new ContextCountsImpl(prevContextCounts, nextContextCounts);
    }

    private Map<String, Integer> invert(Map<Integer, String> map) {
        Map<String, Integer> invertedMap = new HashMap<>(map.size());
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            invertedMap.put(entry.getValue(), entry.getKey());
        }
        return invertedMap;
    }

    private void trimCounts(Map<Integer, Int2IntOpenHashMap> wordCounts) {
        wordCounts.values().stream().forEach(Int2IntOpenHashMap::trim);
    }

    private Map<Integer, Int2IntOpenHashMap> createEmptyCounts(int size) {
        Map<Integer, Int2IntOpenHashMap> result = new HashMap<>();
        for (int i = 0; i < size; i++) {
            result.put(i, MapUtils.createNewInt2IntMap());
        }
        return result;
    }

    protected Pair<Map<Integer, String>, Int2IntOpenHashMap> readPhrases() throws IOException {
        Map<String, Integer> rawPraseCounts = countPhrases();
        Map<Integer, String> phraseToIndexMap = assignWordsToIndexBasedOnFrequency(rawPraseCounts);
        Int2IntOpenHashMap phraseFrequencies = new Int2IntOpenHashMap(phraseToIndexMap.size());
        for (Map.Entry<Integer, String> entry : phraseToIndexMap.entrySet()) {
            phraseFrequencies.put(entry.getKey(), rawPraseCounts.get(entry.getValue()));
        }
        return new Pair<>(phraseToIndexMap, phraseFrequencies);
    }

    protected Map<Integer, String> assignWordsToIndexBasedOnFrequency(Map<String, Integer> phraseCounts) {
        List<String> allWords = new ArrayList<>(phraseCounts.keySet());
        Collections.sort(allWords, (word1, word2) -> -Integer.compare(phraseCounts.get(word1), phraseCounts.get(word2)));
        Map<Integer, String> wordMapping = new HashMap<>();
        int ind = 0;
        for (String word : allWords) {
            wordMapping.put(ind++, word);
        }
        return wordMapping;
    }

    protected Map<String, Integer> countPhrases() throws IOException {
        //Count how often every phrase occurs in the input
        Map<String, Integer> phraseCounts = countAllPhrases(inputFile);
        //Select phrases that occur >= minFrequencyOfPhrase
        int totalDroppedCounts = 0;
        Iterator<Map.Entry<String, Integer>> iterator = phraseCounts.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Integer> entry = iterator.next();
            if (entry.getValue() < minFrequencyOfPhrase) {
                totalDroppedCounts += entry.getValue();
                iterator.remove();
            }
        }
        if (totalDroppedCounts > 0) {
            phraseCounts.put(UNKNOWN_PHRASE, totalDroppedCounts);
        }
        return phraseCounts;
    }

    private Map<String, Integer> countAllPhrases(String textInputFile) throws IOException {
        Object2IntOpenHashMap<String> phraseCounts = new Object2IntOpenHashMap<>();
        BufferedReader rdr = new BufferedReader(new FileReader(textInputFile));
        while (rdr.ready()) {
            String line = rdr.readLine();
            List<String> phrases = splitLineInPhrases(line);
            for (String phrase : phrases) {
                phraseCounts.addTo(phrase, 1);
            }
        }
        rdr.close();
        return phraseCounts;
    }

    /**
     * Could be adapted to have phrases of more than 1 word (e.g. map collocations such as 'fast food' or 'prime minister' to a single phrase)
     */

    private List<String> splitLineInPhrases(String line) {
        String[] words = line.split("\\s");
        List<String> result = new ArrayList<>();
        for (String word : words) {
            if (!word.isEmpty()) {
                result.add(word);
            }
        }
        return result;
    }

}

