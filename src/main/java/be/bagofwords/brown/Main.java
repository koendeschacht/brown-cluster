package be.bagofwords.brown;

import be.bagofwords.ui.UI;

import java.io.IOException;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 11/12/14.
 */
public class Main {

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
}
