package cis5550.jobs;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

import static cis5550.webserver.Server.*;

public class SearchApi {
    private static Map<String, Integer> wordFrenquency = new HashMap<>();
    private static Map<String, List<SearchOutput>> outputMap = new ConcurrentHashMap<>();
    private static Map<String, Long> accessedTimeMap = new ConcurrentHashMap<>();
    protected static Map<String, double[]> wordVectors = new HashMap<>();
    private static Map<String, Integer> searchwordCount = new HashMap<>();
    private static Map<String, Double> pageRankMap = new HashMap<>();
    private static Map<String, String> titleMap = new HashMap<>();
    private static Map<String, String> queryPageNumAndPageSizeMap = new HashMap<>();
    
    private final static long gcInterval = 1800000;
    private static Timer gcTimer = new Timer();
    
    private static int workerSize = 2;
    private static final int numSynonyms = 5;
    private static final String Glove = "glove.6B.50d.txt";
    private static final String EnglishWords = "englishwords.txt";
	private static final String pageRankTable = "pageranks.table";
    private static final int numHistoryUrls = 5;
    private static final int numAutoComplete = 10;
    
	protected static Trie trie = new Trie();

    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
    		System.out.println("Missing arguments");
    		return ;
    	}
    	
    	String kvsInfo = args[0];
    	String backendPort = args[1];
    	KVSClient kvsClient = new KVSClient(kvsInfo);
    	port(Integer.parseInt(backendPort));
    	System.out.println("Backend started at port: " + backendPort);
    	createWordVectors();
    	createTrie();
        garbageCollector();
        createPageRankTable();
        createTitleTable(kvsClient);
        get("/", (req, res) -> mainPage(req, res));
        get("/searchResults", (req, res) -> searchResultsPage(req, res));
        get("/search", (req, res) -> searchHandler(req, res, kvsClient, kvsInfo));
        get("/synonym", (req, res) -> findSynonyms(req, res));
        get("/autocomplete", (req, res) -> autoComplete(req, res));
        get("/emptyquery", (req, res) -> emptryQuery(req, res));
    }
    
    private static void createTitleTable(KVSClient kvsClient) throws IOException {	 
    	Iterator<Row> iter = kvsClient.scan("urlpages");
    	while (iter.hasNext()) {
    		Row curRow = iter.next();
    		titleMap.put(curRow.key(), curRow.get("title"));
    	}
    	System.out.println("title table generated");
	}

	public static String readHtmlFile(String filePath) throws Exception {
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
        return content;
    }

    public static String mainPage(Request req, Response res) throws Exception {
        // Read the HTML content to be returned
        String html = readHtmlFile("pages/index.html");
        res.type("text/html");
        res.write(html.getBytes());
        return "OK";
    }

    public static String searchResultsPage(Request req, Response res) throws Exception {
       	// No query input is given, return the 5 most recent search queries
		if (req.queryParams("query") == null) {
            res.status(404, "No query content");
			return "FAIL";
		}
        String queryItem = req.queryParams("query");
        
        // Read the HTML content to be returned
        String html = readHtmlFile("pages/searchResults.html");
        res.type("text/html");
        res.write(html.getBytes());
        return "OK";
    }
    
    public static void createPageRankTable() throws IOException {
    	for (int i = 1; i <= workerSize; i++) {
        	Scanner scanner = new Scanner(Paths.get("worker" + i + "/" + pageRankTable));
    		while (scanner.hasNextLine()) {
    		    String[] line = scanner.nextLine().split(" ");
    		    String url = line[0];
    		    Double pageRank = Double.parseDouble(line[line.length - 1]);
    		    pageRankMap.put(url, pageRank);
    		}
    		scanner.close();
    	}		   
    }
    
    public static void createWordVectors() throws IOException {
    	// word vectors for finding word synonyms
		Scanner scanner = new Scanner(Paths.get("data/" + Glove));
		while (scanner.hasNextLine()) {
		    String[] line = scanner.nextLine().split(" ");
		    String word = line[0];
		    double[] vector = new double[line.length - 1];
		    for (int i = 1; i < line.length; i++) {
		        vector[i - 1] = Double.parseDouble(line[i]);
		    }
		    wordVectors.put(word, vector);
		}
		scanner.close();
    }
	
	private static void createTrie() throws IOException {
		// Tries for auto-complete
		Scanner scanner = new Scanner(Paths.get("data/" + EnglishWords));
		while (scanner.hasNextLine()) {
			trie.insert(scanner.nextLine());
		}
		scanner.close();
	}
    
	private synchronized static void garbageCollector() {
		// Garbage collect search history every 1 minute
        gcTimer.schedule(new TimerTask() {
            @Override
            public void run() {
            	if (accessedTimeMap.entrySet() != null) {
            		for (Entry<String, Long> entry : accessedTimeMap.entrySet()) {
                		if (System.currentTimeMillis() - entry.getValue() >= gcInterval) {
                			outputMap.remove(entry.getKey());
                            accessedTimeMap.remove(entry.getKey());
                            queryPageNumAndPageSizeMap.remove(entry.getKey());
                            System.out.println("Output results of " + entry.getKey() + " was successfully garbage collected");
                        }
                	}
            	}
            }
        }, gcInterval, gcInterval);
    }
    
	private static String emptryQuery(Request req, Response res) throws Exception {
		// No query input is given, return the 5 most recent search queries
		if (req.queryParams("query") != null) {
			return "FAIL";
		}
		// Create a list of entries from the HashMap
        List<Map.Entry<String, Long>> entryList = new ArrayList<>(accessedTimeMap.entrySet());
        // Sort the list in reverse order based on the values
        Collections.sort(entryList, Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()));
        // Create a new LinkedHashMap from the sorted list
        Map<String, Long> reverseSortedMap = entryList.stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b, LinkedHashMap::new));
        List<EmptyqueryOutput> urls = new LinkedList<>();
        int i = 0;
        for (Entry<String, Long> entry : reverseSortedMap.entrySet()) {
        	if (i == numHistoryUrls) break;
        	urls.add(new EmptyqueryOutput(entry.getKey()));
        	i++;
        }
        
        Map<String, List<EmptyqueryOutput>> jsonMap = new HashMap<>();
        jsonMap.put("search-history", urls);
        
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String jsonResponse = gson.toJson(jsonMap);
        res.write(jsonResponse.getBytes());
		return "OK";
	}

	private static String autoComplete(Request req, Response res) throws Exception {
		String query = req.queryParams("query");
		String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
		String tokenizedInput = ProcessInput.tokenizeAndNormalizeInput(encodedQuery);
		
		String[] tokenized = tokenizedInput.split("\\s+");
		String targetWord = tokenized[tokenized.length - 1];
        List<String> autocompleteResults = trie.autocomplete(targetWord);
        List<AutocompleteOutput> output = new LinkedList<>();
        if (autocompleteResults == null) output.add(new AutocompleteOutput(targetWord, searchwordCount.getOrDefault(targetWord, 0)));
        else {
            int i = 0;
            for (String term : autocompleteResults) {
            	if (i == numAutoComplete) break;
            	output.add(new AutocompleteOutput(term, searchwordCount.getOrDefault(term, 0)));
            	i++;
            }
        }
        
        Map<String, List<AutocompleteOutput>> jsonMap = new HashMap<>();
        jsonMap.put("suggestions", output);
        
        Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        String jsonResponse = gson.toJson(jsonMap);
        
        res.write(jsonResponse.getBytes());
		return "OK";
	}

    public static String searchHandler(Request req, Response res, KVSClient kvsClient, String kvsInfo) throws Exception {
    	try {
    		String query = req.queryParams("query");
    		String pageSize = req.queryParams("pageSize");
    		String pageNum = req.queryParams("pageNum");
    		
    		Gson gson = new GsonBuilder().setPrettyPrinting().create();
    		
            String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
    		// If the encodedQuery has its corresponding hashmap, then we can retrieve the result to speed up the query processing speed
            if (outputMap.keySet() != null && outputMap.keySet().contains(encodedQuery) && queryPageNumAndPageSizeMap.get(encodedQuery).equals(pageSize + "_" + pageNum)) {
            	List<SearchOutput> storedResults = outputMap.get(encodedQuery);
                Map<String, List<SearchOutput>> jsonMap = new HashMap<>();
                jsonMap.put("results", storedResults);
            	String jsonResponse = gson.toJson(jsonMap);
            	accessedTimeMap.put(encodedQuery, System.currentTimeMillis());
                res.write(jsonResponse.getBytes());
                return "OK";
            }
            // Process search inputs
            String tokenizedInput = ProcessInput.tokenizeAndNormalizeInput(encodedQuery);
            String noStopWordInput = ProcessInput.removeStopWords(tokenizedInput);
            System.out.println("no stop input: " + noStopWordInput);
            // stemming or not? depends
            String stemmedWord = ProcessInput.stemWords(noStopWordInput);
            
            // Store each root words frequency
            for (String word : noStopWordInput.split("\\s+")) {
            	wordFrenquency.put(word, wordFrenquency.getOrDefault(word, 1) + 1);
            }
            // use stemmed word or no stop word input?
            Map<String, Set<String>> synonym = ProcessInput.findSynonyms(noStopWordInput, wordVectors, numSynonyms);
            
            // Get the priorityScoreMap in descending order
            Map<String, Double> priorityScoreMap = RankScore.calculatePriorityScore(synonym, wordFrenquency, kvsInfo, pageRankMap, titleMap);
            
            List<SearchOutput> storedResults = generateOutput(pageSize, pageNum, priorityScoreMap, kvsClient, noStopWordInput);
            
            // Put the total results into the outputMap
            outputMap.put(encodedQuery, storedResults);
            // Put the pageSize and PageNum into the map
            queryPageNumAndPageSizeMap.put(encodedQuery, pageSize + "_" + pageNum);  
        	
            Map<String, List<SearchOutput>> jsonMap = new HashMap<>();
            List<SearchOutput> count = new ArrayList<>();
            count.add(new SearchOutput(String.valueOf(priorityScoreMap.keySet().size()), "", ""));
            
            jsonMap.put("count", count);
            jsonMap.put("results", storedResults);
            String jsonResponse = gson.toJson(jsonMap);
            
            // Update the time of the query in the accessedTimeMap 
            accessedTimeMap.put(encodedQuery, System.currentTimeMillis());
            
            // Put each search word and its frequency into the searchwordCount map
            for (String word : tokenizedInput.split("\\s+")) {
            	searchwordCount.put(word, searchwordCount.getOrDefault(word, 0) + 1);
            }
            
            res.write(jsonResponse.getBytes());
            return "OK";
    	} catch (Exception e) {
    		res.write("Error Occured".getBytes());
    		return "Internal Server Error";
    	}
    }
    
    private static List<SearchOutput> generateOutput(String pageSize, String pageNum, Map<String, Double> priorityScoreMap, KVSClient kvsClient, String noStopWordInput) throws IOException {
    	List<SearchOutput> storedResults = new LinkedList<>();
    	Integer startIndex = Integer.parseInt(pageSize) * (Integer.parseInt(pageNum) - 1);
    	Integer endIndex = Integer.parseInt(pageSize) * Integer.parseInt(pageNum);
    	int count = 0;
    	for (String url : priorityScoreMap.keySet()) {
    		count++;
    		if (count > startIndex && count <= endIndex) {
    			Row row = kvsClient.getRow("urlpages", url);
                if (row == null) continue;
                String title = row.get("title").replaceAll("[^a-zA-Z0-9 ]", " ");
                String content = row.get("content").replaceAll("[^a-zA-Z0-9\\s.,?!]", "").replaceAll("[\\r\\n\\t\\s+]+", " ").trim();
                
                String snippet = "";
                for (String word : noStopWordInput.split(" ")) {
                	int beginIndex = 0;
                	int snippetIndex = content.indexOf(word, beginIndex);
                    
                	if (snippetIndex != -1) {
                        String text = content.substring(snippetIndex);
                        for (int i = 0; i < text.length(); i++) {
                        	if (text.charAt(i) == ',' || text.charAt(i) == ';' || text.charAt(i) == '.' || text.charAt(i) == '?' || text.charAt(i) == '!') {
                        		 text = text.substring(0, i) + text.charAt(i);
                                 snippet += text + "...";
                                 beginIndex = i;
                                 break;
                        	}
                        }
                	}
                }
                if (snippet.length() == 0) {
                	Pattern commonPattern = Pattern.compile("\\b\\S+\\b");
                	Matcher commonMatcher = commonPattern.matcher(content);
                	int numWords = 0; // Counter for the number of words matched so far
                    int maxWords = 20; // Maximum number of words to match
                    StringBuilder sb = new StringBuilder();
                    Random random = new Random();
                    int contentLength = content.length();
                    int snippetIndex;
                    if (contentLength > 20) {
                    	snippetIndex = random.nextInt(content.length() - 20);
                    } else {
                    	snippetIndex = 0;
                    }

                	// Loop through the matches and concatenate the first ten words
                    commonMatcher.find(snippetIndex);
                	while (commonMatcher.find() && numWords < maxWords) {
                		sb.append(commonMatcher.group());
                		sb.append(" ");
                		numWords++;
                	}
                	
                	
                    snippet += sb.toString() + "...";
                }

                storedResults.add(new SearchOutput(url, title, snippet));
    		}
    		if (count >= endIndex) break;
    	}
		return storedResults;
	}

	private static String findSynonyms(Request req, Response res) throws Exception {
    	String query = req.queryParams("query");
    	String encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8);
    	String tokenizedInput = ProcessInput.tokenizeAndNormalizeInput(encodedQuery);
        String noStopWordInput = ProcessInput.removeStopWords(tokenizedInput);
        Map<String, Set<String>> synonym = ProcessInput.findSynonyms(noStopWordInput, wordVectors, numSynonyms);
        Set<SynonymWords> storedSynonym = new HashSet<>();
        for (Entry<String, Set<String>> entry : synonym.entrySet()) {
        	System.out.println(entry.getKey());
        	System.out.println(entry.getValue());
        	storedSynonym.add(new SynonymWords(entry.getKey(), entry.getValue()));
        }
        
   		Set<String> similarSentences = new LinkedHashSet<>();
        for (String word : noStopWordInput.split("\\s+")) {
        	String originalString = tokenizedInput + " ";
        	int wordIndex = 0;
        	// Replace root words in a sentence with their synonyms
        	if ((wordIndex = tokenizedInput.indexOf(word, wordIndex)) != -1) {
        		// Find the start and end positions of the word to replace
        		String before = originalString.substring(0, wordIndex);
        		String after = originalString.substring(originalString.indexOf(" ", wordIndex));

        		// Construct the new string with the replaced word
        		String replaceWord = synonym.get(word).iterator().next();
        		String newString = before + replaceWord + after;
        		similarSentences.add(newString.trim());
        	}
        }
        SimilarOutput output = new SimilarOutput(similarSentences, storedSynonym);
        
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        Map<String, SimilarOutput> jsonMap = new HashMap<>();
        jsonMap.put("results", output);
        String jsonResponse = gson.toJson(jsonMap);
        res.write(jsonResponse.getBytes());
		return "OK";
	}
    
    static class GenericOutput<T> {
    	private T content;

        public GenericOutput(T content) {
        	this.content = content;
        }

        public T getContent() {
            return content;
        }
    }
    
    static class EmptyqueryOutput {
    	private String query;
    	
    	public EmptyqueryOutput(String query) {
    		this.query = query;
    	}
    	
    	public String getQuery() {
    		return query;
    	}
    }
    
    static class AutocompleteOutput {
    	private String term;
    	private int count;
    	
    	public AutocompleteOutput(String term, int count) {
    		this.term = term;
    		this.count = count;
    	}
    	
    	public String getTerm() {
    		return term;
    	}
    	
    	public int getCount() {
    		return count;
    	}
    }
    
    static class SimilarOutput {
    	private Set<String> similarSentences;
    	private Set<SynonymWords> storedSynonyms;
    	
    	public SimilarOutput(Set<String> similarSentences, Set<SynonymWords> storedSynonyms) {
    		this.similarSentences = similarSentences;
    		this.storedSynonyms = storedSynonyms;
    	}
    	
    	public Set<String> getSimilarSentences() {
	    	return similarSentences;
	    }
	    
	    public Set<SynonymWords> getStoredSynonyms() {
	    	return storedSynonyms;
	    }
    }
    
    static class SynonymWords {
    	private String rootWord;
    	private Set<String> similarWords;
    	
    	public SynonymWords(String rootWord, Set<String> similarWords) {
    		this.rootWord = rootWord;
    		this.similarWords = similarWords;
    	}
    	
    	public String getRootWord() {
	    	return rootWord;
	    }
	    
	    public Set<String> getSimilarWords() {
	    	return similarWords;
	    }
    }
    
    static class SearchOutput {
	    private String url;
	    private String title;
	    private String snippet;

	    public SearchOutput(String url, String title, String snippet) {
	        this.url = url;
	        this.title = title;
	        this.snippet = snippet;
	    }
	    
	    public String getUrl() {
	    	return url;
	    }
	    
	    public String getTitle() {
	    	return title;
	    }
	    
	    public String getSnippet() {
	    	return snippet;
	    }
	}
    
    static class TrieNode {
        Map<Character, TrieNode> children;
        boolean isWord;

        public TrieNode() {
            this.children = new HashMap<>();
            this.isWord = false;
        }
    }
    
    static class Trie {
        TrieNode root;

        public Trie() {
            this.root = new TrieNode();
        }

        public void insert(String word) {
            TrieNode node = root;
            for (char c : word.toCharArray()) {
                node.children.putIfAbsent(c, new TrieNode());
                node = node.children.get(c);
            }
            node.isWord = true;
        }

        public List<String> autocomplete(String prefix) {
            List<String> results = new ArrayList<>();
            TrieNode node = root;
            for (char c : prefix.toCharArray()) {
                if (node.children.containsKey(c)) {
                    node = node.children.get(c);
                } else {
                    return results;
                }
            }
            findWords(node, prefix, results);
            return results;
        }

        private void findWords(TrieNode node, String prefix, List<String> results) {
            if (node.isWord) {
                results.add(prefix);
            }
            for (char c : node.children.keySet()) {
                findWords(node.children.get(c), prefix + c, results);
            }
        }
    }
}