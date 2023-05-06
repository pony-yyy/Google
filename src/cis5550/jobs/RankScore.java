package cis5550.jobs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class RankScore {
	private static final String indexTable = "index";
	private static final String pageRankTable = "pageranks";
	private static Map<String, String> wordIDF = new HashMap<>();
	private static Map<String, Map<String, String>> wordUrlTF = new HashMap<>();
	private static Map<String, Map<String, List<String>>> wordUrlPos = new HashMap<>();
	private static Map<String, String> urlPagerank = new HashMap<>();
	
	private final static double pageRankWeight = 0.1;
	private final static double tfidfWeight = 10;
	
	// base for title
	private final static int base = 100000;
	
	public static Map<String, Double> calculatePriorityScore(Map<String, Set<String>> synonym, Map<String, Integer> wordFrenquency, String kvsInfo, Map<String, Double> pageRankMap, Map<String, String> titleMap) throws IOException {
//		readIndexTable(kvsClient);
//		readPageRankTable(kvsClient);
		KVSClient kvsClient = new KVSClient(kvsInfo);

		Map<String, Double> urlTFIDF = calculateTFIDF(synonym, wordFrenquency, kvsClient);
		Map<String, Double> urlPriority = calculatePriority(urlTFIDF, kvsClient, pageRankMap, titleMap, synonym.keySet());
		return urlPriority;
	}
	
	private static Map<String, Double> calculatePriority(Map<String, Double> urlTFIDF, KVSClient kvsClient, Map<String, Double> pageRankMap, Map<String, String> titleMap, Set<String> words) throws IOException {
		for (String key : urlTFIDF.keySet()) {
			Double TFIDF = urlTFIDF.get(key);
			// System.out.println("TFIDF: " + TFIDF);
			
			// Retrieve pagerank info
			Double pagerank = pageRankMap.getOrDefault(key, 0.15);	
	
			Double priorityScore = pageRankWeight * pagerank + tfidfWeight * TFIDF;
			
			// If the current url contains title, then add a very high score to it
			String title = titleMap.getOrDefault(key, "");
			for (String word: words) {
				if (title.replaceAll("[^a-zA-Z0-9 ]", " ").matches(".*\\b" + word + "\\b.*")) priorityScore += base;
			}
			
			urlTFIDF.put(key, priorityScore);
		}

		// Sort the hashmap by value in descending order
        Map<String, Double> sortedMap = urlTFIDF.entrySet()
            .stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new
                )
            );
        
		System.out.println("sorted results size: " + urlTFIDF.keySet().size());

		return sortedMap;
	}

	private static Map<String, Double> calculateTFIDF(Map<String, Set<String>> synonym, Map<String, Integer> wordFrequency, KVSClient kvsClient) throws IOException {
		// Root word only
		// Phrase search (at least two words)
		Map<String, Double> phraseSearchTFIDF = new LinkedHashMap<>();
		if (synonym.keySet().size() > 1) {
			phraseSearchTFIDF = phraseSearch(synonym, wordFrequency, kvsClient);
		}
		
		// Single root word search
		Map<String, Double> singleSearchTFIDF = singleSearch(synonym, wordFrequency, phraseSearchTFIDF, kvsClient);
		
		// TODO (could be done but not necessary since it will slow down the process)
		// Root word with synonym word
		
		// Synonym word only
		Map<String, Double> similarSearchTFIDF = similarSearch(synonym, wordFrequency, kvsClient);
		
		// Merge all three together into a combined url with TFIDF map
		for (String key : singleSearchTFIDF.keySet()) {
			phraseSearchTFIDF.put(key, phraseSearchTFIDF.getOrDefault(key, 0.0) + singleSearchTFIDF.get(key));
		}
		
		for (String key : similarSearchTFIDF.keySet()) {
			phraseSearchTFIDF.put(key, phraseSearchTFIDF.getOrDefault(key, 0.0) + similarSearchTFIDF.get(key));
		}

		return phraseSearchTFIDF;
	}
	
	private static Map<String, Double> similarSearch(Map<String, Set<String>> synonym, Map<String, Integer> wordFrequency, KVSClient kvsClient) throws IOException {
		Map<String, Double> urlTFIDF = new LinkedHashMap<>();
		for (Set<String> synList : synonym.values()) {
			Double weight = 0.5;
			for (String word : synList) {
				Row row = kvsClient.getRow(indexTable, word);
				String[] content = null;
				if (row != null) {
					for (String col : row.columns()) {
						content = row.get(col).split("\r\n");
						break;
					}
				} else {
					continue;
				}
				
				// Retrieve the all corresponding urls -> TF of the current word
				Map<String, String> curWordUrlsTF = new LinkedHashMap<>();
				for (int i = 1; i < content.length; i++) {
					String[] lineContent = content[i].split("\\s+");
					curWordUrlsTF.put(lineContent[0], lineContent[lineContent.length - 1]);
				}		
				
				for (String url : curWordUrlsTF.keySet()) {
					Double curWordTF = Double.parseDouble(curWordUrlsTF.get(url));
					Double curWordIDF = Double.parseDouble(content[0]);
					Double curWordTFIDF = curWordTF * curWordIDF;
					// 0.2 weights for synonym words
					urlTFIDF.put(url, curWordTFIDF * weight);
				}
				weight /= 2;
			}
		}

		// System.out.println("similar search urls: " + urlTFIDF.keySet());
		System.out.println("end of synonyms search");
		
		return urlTFIDF;
	}

	private static Map<String, Double> singleSearch(Map<String, Set<String>> synonym, Map<String, Integer> wordFrequency, Map<String, Double> phraseSearchTFIDF, KVSClient kvsClient) throws IOException {
		Set<String> rootWords = synonym.keySet();
		Map<String, Double> urlTFIDF = new LinkedHashMap<>();
		for (String word : rootWords) {
			// Retrieve the current word from the index table
			Row row = kvsClient.getRow("index", word);
			String[] content = null;
			if (row != null) {
				for (String col : row.columns()) {
					content = row.get(col).split("\r\n");
					break;
				}
			} else {
				continue;
			}			
			
			// Retrieve the all corresponding urls -> TF of the current word
			Map<String, String> curWordUrlsTF = new LinkedHashMap<>();
			for (int i = 1; i < content.length; i++) {
				String[] lineContent = content[i].split("\\s+");
				curWordUrlsTF.put(lineContent[0], lineContent[lineContent.length - 1]);
			}			
			
			Set<String> urls = curWordUrlsTF.keySet();
			for (String url : urls) {
				if (rootWords.size() == 1 || !phraseSearchTFIDF.containsKey(url)) {
					Double curWordTF = Double.parseDouble(curWordUrlsTF.get(url));
					Double curWordIDF = Double.parseDouble(content[0]);
					Integer curWordFreq = wordFrequency.get(word);
					Double curWordTFIDF = curWordTF * curWordIDF * curWordFreq;
					// 0.5 weights?
					urlTFIDF.put(url, curWordTFIDF * 10);
				}
			}
		}
		System.out.println("end of single root word search");
		
		return urlTFIDF;
	}

	public static Map<String, Double> phraseSearch(Map<String, Set<String>> synonym, Map<String, Integer> wordFrequency, KVSClient kvsClient) throws IOException {
		// Support phrase search -> two root words appear in the same page
		// O(n^3) ?
		Map<String, Double> urlTFIDF = new LinkedHashMap<>();
		// This queue only contains root words
		PriorityQueue<String> queue = new PriorityQueue<>(synonym.keySet());
		
		while (!queue.isEmpty()) {
			// Remove the current word
			String curWord = queue.poll();
			
			// Retrieve the current word from the index table
			Row row = kvsClient.getRow(indexTable, curWord);
			String[] content = null;
			if (row != null) {
				for (String col : row.columns()) {
					content = row.get(col).split("\r\n");
					break;
				}
			} else {
				continue;
			}
			
			// Retrieve the all corresponding urls -> pos and TF of the current word
			Map<String, List<String>> curWordUrlsPos = new LinkedHashMap<>();
			Map<String, String> curWordUrlsTF = new LinkedHashMap<>();
			for (int i = 1; i < content.length; i++) {
				String[] lineContent = content[i].split("\\s+");
				List<String> pos = new ArrayList<>();
				for (int j = 1; j < lineContent.length - 1; j++) {
					pos.add(lineContent[j]);
				}
				curWordUrlsPos.put(lineContent[0], pos);
				curWordUrlsTF.put(lineContent[0], lineContent[lineContent.length - 1]);
			}
			
			Set<String> curRootWordUrls = curWordUrlsPos.keySet();
			
			
			if (queue != null) {
				for (String otherWord : queue) {
					// Retrieve other word's row
					Row otherRow = kvsClient.getRow(indexTable, otherWord);
					String[] otherContent = null;
					if (otherRow != null) {
						for (String col : otherRow.columns()) {
							otherContent = otherRow.get(col).split("\r\n");
							break;
						}
					} else {
						continue;
					}
					
					// Retrieve the all corresponding urls of the current word
					Map<String, List<String>> otherWordUrlsPos = new LinkedHashMap<>();
					Map<String, String> otherWordUrlsTF = new LinkedHashMap<>();
					for (int i = 1; i < otherContent.length; i++) {
						String[] lineContent = otherContent[i].split("\\s+");
						List<String> pos = new ArrayList<>();
						for (int j = 1; j < lineContent.length - 1; j++) {
							pos.add(lineContent[j]);
						}
						otherWordUrlsPos.put(lineContent[0], pos);
						otherWordUrlsTF.put(lineContent[0], lineContent[lineContent.length - 1]);
					}
					
					Set<String> otherWordUrls = otherWordUrlsPos.keySet();
					
					for (String otherUrl : otherWordUrls) {
						// If the two words appears on the same page, retrieve their positions of that page
						if (curRootWordUrls.contains(otherUrl)) {
							List<String> curWordPos = curWordUrlsPos.get(otherUrl);
							List<String> otherWordPos = otherWordUrlsPos.get(otherUrl);
							Double curMax = Double.parseDouble(Collections.max(curWordPos));
							Double curMin = Double.parseDouble(Collections.min(curWordPos));
							Double otherMax = Double.parseDouble(Collections.max(otherWordPos));
							Double otherMin = Double.parseDouble(Collections.min(otherWordPos));
							Double upperBound = Math.max(curMax, otherMax) - Math.min(curMin, otherMin);
							
							upperBound = upperBound <= 0 ? 100 : upperBound;
							
							Double diff = upperBound;
							for (String cur : curWordPos) {
								for (String other : otherWordPos) {
									Double min = Math.abs(Double.parseDouble(cur) - Double.parseDouble(other));
									if ( min < diff) {
										diff = min;
									}
								}
							}
							
							Double curWordTF = Double.parseDouble(curWordUrlsTF.get(otherUrl));
							Double curWordIDF = Double.parseDouble(content[0]);
							Integer curWordFreq = wordFrequency.get(curWord);
							Double curWordTFIDF = curWordTF * curWordIDF * curWordFreq;
							
							Double otherWordTF = Double.parseDouble(otherWordUrlsTF.get(otherUrl));
							Double otherWordIDF = Double.parseDouble(otherContent[0]);
							Integer otherWordFreq = wordFrequency.get(otherWord);
							Double otherWordTFIDF = otherWordTF * otherWordIDF * otherWordFreq;
							
							// Use upperBound/diff as the weighted factor
							Double weight  = upperBound / diff;
							Double totalTFIDF = (curWordTFIDF + otherWordTFIDF) * weight;
							urlTFIDF.put(otherUrl, totalTFIDF);
						}
					}
				}
			}
		}
		System.out.println("end of phrase search of root words");
		
		return urlTFIDF;
	}

	// unused code
	public static void readIndexTable(KVSClient kvsClient) throws IOException {
		Scanner indexScanner = new Scanner(Paths.get("worker1/" + indexTable));
		String curWord = null;
		
		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("worker1/" + indexTable), "UTF-8"))) {
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				String curLine = line;
				if (!curLine.startsWith("http")) {
					// Put word and its IDF into the map
					String[] curContent = curLine.split("\\s+");
					curWord = curContent[0];
					wordIDF.put(curWord, curContent[3]);
				} else {
					String[] curContent = curLine.split("\\s+");
					// Put word and its urls with words positions into the map
					Map<String, List<String>> urlPos = wordUrlPos.getOrDefault(curWord, new HashMap<String, List<String>>());
					List<String> pos = new ArrayList<>();
					for (int i = 1; i < curContent.length - 1; i++) {
						pos.add(curContent[i]);
					}
					urlPos.put(curContent[0], pos);
					wordUrlPos.put(curWord, urlPos);
					
					// Put word and its urls with TF values into the map
					Map<String, String> urlTF = wordUrlTF.getOrDefault(curWord, new HashMap<String, String>());
					urlTF.put(curContent[0], curContent[curContent.length - 1]);
					wordUrlTF.put(curWord, urlTF);
				}
			}
		}
		indexScanner.close();
	}
	
	public static void readPageRankTable(KVSClient kvsClient) throws IOException {
		// Put url and its pagerank into the map
		Scanner pageRankScanner = new Scanner(Paths.get("worker1/" + pageRankTable));
		while (pageRankScanner.hasNextLine()) {
			String[] curLine = pageRankScanner.nextLine().split("\\s+");
			urlPagerank.put(curLine[0], curLine[3]);
		}
		pageRankScanner.close();
	}
}