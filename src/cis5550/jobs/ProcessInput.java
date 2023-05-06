package cis5550.jobs;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProcessInput {
	private static final String stopwordTable = "stopwords.txt";
	
	public static String tokenizeAndNormalizeInput(String searchInput) {
		// Remove punctuation, CR, LF, and tab characters
		String noPunct = searchInput.replaceAll("[\\p{Punct}\\s]+", " ").replaceAll("[^a-zA-Z0-9 ]", "");
		// Convert string to lower case and split by space
		String input = noPunct.toLowerCase();
		return input;
	}
	
	public static String removeStopWords(String searchInput) throws IOException {
		// Read in stopwords.txt file as string of arrays
		List<String> stopwords = Files.readAllLines(Paths.get("data/" + stopwordTable));
		
		String noStopInput = null;
		// Remove stopwords if searchWords length is larger than two
		if (searchInput.split("\\s+").length > 2) {
			// Using removeAll to remove all stopwords has the fastest execution time
			ArrayList<String> allWords = Stream.of(searchInput.split("\\s+")).collect(Collectors.toCollection(ArrayList<String>::new));
			allWords.removeAll(stopwords);
			noStopInput = allWords.stream().collect(Collectors.joining(" "));
			System.out.println("output: " + noStopInput);
		}
		System.out.println("output: " + noStopInput);
		// If a input contains all stopwords or the length of the noStopInput is too short(i.e 1), then it is better to keep all them
		if (noStopInput == null || noStopInput.split("\\s+").length <= 1) return searchInput;
		else return noStopInput;
	}
	
	public static String stemWords(String searchInput) {
		Stemmer stemmer = new Stemmer();
        stemmer.add(searchInput.toCharArray(), searchInput.length());
        stemmer.stem();
        String stemmedWord = stemmer.toString();
		return stemmedWord;
	}
	
	public static Map<String, Set<String>> findSynonyms(String sentence, Map<String, double[]> wordVectors, int numSymnonyms) throws IOException {
		Map<String, Set<String>> result = new HashMap<>();
        // Split sentence into words
        String[] words = sentence.split("\\s+");
		for (String word : words) {
			Set<String> synonym = getSynonyms(word, numSymnonyms, wordVectors);
			if (synonym != null) {
				result.put(word, synonym);
			} else {
				Set<String> temp = new HashSet<>();
				temp.add(word);
				result.put(word, temp);
			}
		}
		return result;
    }
	
	public static Set<String> getSynonyms(String word, int numSynonyms, Map<String, double[]> wordVectors) {
		double[] targetVector = wordVectors.get(word);
	    if (targetVector == null) {
	        return null;
	    }
	    PriorityQueue<Pair<String, Double>> queue = new PriorityQueue<>(new Comparator<Pair<String, Double>>() {
	        @Override
	        public int compare(Pair<String, Double> o1, Pair<String, Double> o2) {
	            return Double.compare(o2.getValue(), o1.getValue());
	        }
	    });
	    for (Map.Entry<String, double[]> entry : wordVectors.entrySet()) {
	        String otherWord = entry.getKey();
	        if (!otherWord.equals(word)) {
	            double[] otherVector = entry.getValue();
	            double similarity = cosineSimilarity(targetVector, otherVector);
	            queue.offer(new Pair<>(otherWord, similarity));
	        }
	    }
	    List<String> synonyms = new LinkedList<>();
	    for (int i = 0; i < numSynonyms; i++) {
	        Pair<String, Double> pair = queue.poll();
	        if (pair == null) {
	            break;
	        }
	        synonyms.add(pair.getKey());
	    }
	    Set<String> result = new LinkedHashSet<>(synonyms);
	    return result;
	}

	private static double cosineSimilarity(double[] vector1, double[] vector2) {
	    double dotProduct = 0.0;
	    double norm1 = 0.0;
	    double norm2 = 0.0;
	    for (int i = 0; i < vector1.length; i++) {
	        dotProduct += vector1[i] * vector2[i];
	        norm1 += Math.pow(vector1[i], 2);
	        norm2 += Math.pow(vector2[i], 2);
	    }
	    return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
	}
	
	static class Pair<K, V> {
	    private final K key;
	    private final V value;

	    public Pair(K key, V value) {
	        this.key = key;
	        this.value = value;
	    }

	    public K getKey() {
	        return key;
	    }

	    public V getValue() {
	        return value;
	    }
	}
}