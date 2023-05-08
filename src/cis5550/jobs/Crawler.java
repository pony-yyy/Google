package cis5550.jobs;

import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.*;


public class Crawler {
	public final static String crawlerName = "cis5550-crawler";
	
	public final static String crawlTable = "crawl";
	public final static String hostsTable = "hosts";
	public final static String urlFrontierTable = "urlFrontier";
	public final static String crawlLogTable = "crawlLog";
	
	public final static int pageSizeLimit = 10000000; // 10MB
	public final static long crawlTimeLimit = 300; // in seconds
	
	public static class Robots {
		public float crawlDelay;
		public ArrayList<String[]> crawlRules;
		
		public Robots() {
			crawlDelay = 1.0F; // default value
			crawlRules = new ArrayList<String[]>();
		}
	}
	
	
	public static void run(FlameContext context, String[] args) throws Exception {
		if (args.length == 0) {
			context.output("No seed URL found");
			return;
		}
		else {
			context.output("OK");
		}
		
		KVSClient contextKVS = context.getKVS();
		String masterAddr = contextKVS.getMaster();
		
		// Create persistent tables
		contextKVS.persist(crawlTable);
		contextKVS.persist(hostsTable);
		contextKVS.persist(urlFrontierTable);
		contextKVS.persist(crawlLogTable);
		
		
		FlameRDD.StringToIterable lambda = crawlLambda(masterAddr);
		
		FlameContext.RowToString getUrlFrontier = (row) -> {
			String url = row.get("url");
			if (url == null) return null;
			
			String crawledStr = row.get("crawled");
			// crawled column is not null. This means the crawled is set to "true"
			if (crawledStr != null) return null;
			
			return url;
		};
		
		
		
		FlameRDD urlQueue;
		if (contextKVS.count(urlFrontierTable) > 0) {
			urlQueue = context.fromTable(urlFrontierTable, getUrlFrontier);
		}
		else {
			ArrayList<String> seedUrls = new ArrayList<String>();
			
			for (String seedUrl : args) {
				seedUrl = normalizeURL(seedUrl, null);
				if (seedUrl != null) seedUrls.add(seedUrl);
			}
			
			urlQueue = context.parallelize(seedUrls);
			
			updateURLFrontier(contextKVS, urlQueue.collect(), null);
		}
		
		writeToLog(contextKVS, urlQueue.count() + " urls are loaded. Start crawling.", null);
		
		while (urlQueue.count() != 0) {
			try {
				urlQueue = urlQueue.flatMap(lambda);
			} catch (Exception e) {
				writeToLog(contextKVS, "ERROR: urlQueue.flatMap(lambda)", e);
				return;
			}
		}
		
	}
	
	
	
	public static Iterable<String> crawlURL(String urlArg, String masterAddr) throws IOException {
//		System.out.println("Start crawling " + urlArg);
		String urlHashKey = Hasher.hash(urlArg);
		HashSet<String> newUrls = new HashSet<String>();
		KVSClient kvsClient = new KVSClient(masterAddr);
		
			
		// Don't stop the flame worker if something is wrong
		try {
		///////////////////////////// TRY BLOCK STARTS /////////////////////////////
				
		// skip crawled url
		if (kvsClient.existsRow(crawlTable, urlHashKey)) return newUrls;
			
		// check robots.txt and see if the url is allowed to be crawled
		Robots robots = checkRobot(urlArg, kvsClient);
		String[] urlElements = URLParser.parseURL(urlArg);
		if (!allowToCrawl(urlElements[3], robots)) return newUrls;
			
		// Check whether crawling is too frequent
		if (isFrequentAccess(urlArg, kvsClient, robots)) {
			newUrls.add(urlArg);
			// no need to update urlFrontier here
			return newUrls;
		}
			
		updateLastAccessedTime(urlArg, kvsClient);
			
			
		URL url = new URL(urlArg);
			
		// Send HEAD request first
		HttpURLConnection headConnection = (HttpURLConnection) url.openConnection();
		headConnection.setInstanceFollowRedirects(false);
		headConnection.setRequestMethod("HEAD");
		headConnection.setRequestProperty("User-Agent", crawlerName);
		headConnection.connect();
			
		// Only crawl English contents
		String contentLanguage = headConnection.getHeaderField("Content-Language");
		if (contentLanguage != null && !contentLanguage.contains("en")) {
			writeToLog(kvsClient, "Give up crawling (1) a foreign-language page at " + urlArg, null);
			updateURLFrontier(kvsClient, newUrls, urlArg);
			return newUrls;
		}
			
			
		Row newRow = new Row(urlHashKey);
		newRow.put("url", urlArg);
			
		String contentType = headConnection.getHeaderField("Content-Type");
		if (contentType != null) newRow.put("contentType", contentType);
		String length = headConnection.getHeaderField("Content-Length");
		if (length != null) newRow.put("length", length);
			
		int headStatusCode = headConnection.getResponseCode();
		newRow.put("responseCode", Integer.toString(headStatusCode));
		
		if (headStatusCode != 200) {
			if (headStatusCode == 301 || headStatusCode == 302 || headStatusCode == 303 || headStatusCode == 307 || headStatusCode == 308) {
				String redirectedUrl = headConnection.getHeaderField("Location");
				redirectedUrl = normalizeURL(redirectedUrl, urlArg);
				if (redirectedUrl != null) newUrls.add(redirectedUrl);
			}
			
			kvsClient.putRow(crawlTable, newRow);
				
			updateURLFrontier(kvsClient, newUrls, urlArg);
			return newUrls;
		}
			
			
		// Send GET request if the head request returns status code 200
		HttpURLConnection getConnection = (HttpURLConnection) url.openConnection();
		getConnection.setInstanceFollowRedirects(false);
		getConnection.setRequestMethod("GET");
		getConnection.setRequestProperty("User-Agent", crawlerName);
		getConnection.connect();
			
		if (contentType != null && contentType.contains("text/html")) {
			String page = "";
			InputStream inputStream = getConnection.getInputStream();
			int pageSizeCount = 0;
			int c;
			while ((c = inputStream.read()) != -1 && pageSizeCount++ < pageSizeLimit) page += (char) c;
				
			if (isInEnglish(page)) {
				newRow.put("page", page);
				newUrls = extractURLs(page, urlArg);
			}
			else {
				writeToLog(kvsClient, "Give up crawling (2) a foreign-language page at " + urlArg, null);
				// Don't record the url in the crawlTable
				updateURLFrontier(kvsClient, newUrls, urlArg);
				return newUrls;
			}
				
		}
			
		kvsClient.putRow(crawlTable, newRow);
			
		updateURLFrontier(kvsClient, newUrls, urlArg);
		return newUrls;
			
		///////////////////////////// TRY BLOCK ENDS /////////////////////////////
		} catch (Exception e) {
			writeToLog(kvsClient, "ERROR: crawling " + urlArg, e);
				
			updateURLFrontier(kvsClient, newUrls, urlArg);
			return newUrls;
		}
	}
	
	
	public static FlameRDD.StringToIterable crawlLambda(String masterAddr) {
		FlameRDD.StringToIterable lambda = (urlArg) -> {
			Iterable<String> newUrls = new HashSet<String>();
			
			ExecutorService executor = Executors.newSingleThreadExecutor();
			Future<Iterable<String>> handler = executor.submit(() -> crawlURL(urlArg, masterAddr));
			
			KVSClient kvsClient = new KVSClient(masterAddr);
			try {
				newUrls = handler.get(crawlTimeLimit, TimeUnit.SECONDS);
	        } catch (TimeoutException e) {
	        	handler.cancel(true);
	        	writeToLog(kvsClient, "Give up crawling at " + urlArg + " because it takes too long time.", e);
	        	
	        } catch (Exception e) {
	        	writeToLog(kvsClient, "Crawling error at " + urlArg, e);
	        } finally {
//	        	System.out.println("End crawling " + urlArg);
	            executor.shutdownNow();
	        }
			
			return newUrls;
//			return crawlURL(urlArg, masterAddr);
		};
		
		return lambda;
	}
	


	public static Robots checkRobot(String url, KVSClient kvsClient) throws Exception {
		String[] urlElements = URLParser.parseURL(url);
		if (urlElements[0] == null || urlElements[1] == null) return new Robots();
		String host = urlElements[0] + "://" + urlElements[1];
		String hostHashKey = Hasher.hash(host);
		

		byte[] robotByteContent = kvsClient.get(hostsTable, hostHashKey, "robots");
		String content = "";
		if (robotByteContent == null) {
			URL robotUrl = new URL(host + "/robots.txt");
			HttpURLConnection getConnection = (HttpURLConnection) robotUrl.openConnection();
			getConnection.setRequestMethod("GET");
			getConnection.setRequestProperty("User-Agent", crawlerName);
			getConnection.connect();
			
			int getStatusCode = getConnection.getResponseCode();
			if (getStatusCode != 200) return new Robots();
			
			InputStream inputStream = getConnection.getInputStream();
			int c;
			while ((c = inputStream.read()) != -1) content += (char) c;
			
			Row hostRow = new Row(hostHashKey);
			hostRow.put("host", host);
			hostRow.put("robots", content);
			kvsClient.putRow(hostsTable, hostRow);
		}
		else {
			content = new String(robotByteContent);
		}
		
		return parseRobot(content);
	}
	
	
	public static Robots parseRobot(String content) {
		Robots robots = new Robots();
		
		int startIndex = content.indexOf("User-agent: " + crawlerName);
		if (startIndex == -1) {
			startIndex = content.indexOf("User-agent: *");
			if (startIndex == -1) return robots;
		}
		content = content.substring(startIndex);

		Iterator<String> lines = content.lines().iterator();
		// skip user-agent line
		if (lines.hasNext()) lines.next();
		

		while (lines.hasNext()) {
			String line = lines.next();
			String[] rule = line.split(":");
			if (rule.length < 2) break;
			rule[1] = rule[1].trim();
						
			if (rule[0].equals("Crawl-delay")) robots.crawlDelay = Float.parseFloat(rule[1]);
			else robots.crawlRules.add(rule);
			
			if (line.equals("")) break;
		}
		
		return robots;
	}
	
	
	
	public static boolean isFrequentAccess(String url, KVSClient kvsClient, Robots robots) throws IOException {
		String[] urlElements = URLParser.parseURL(url);
		if (urlElements[0] == null || urlElements[1] == null) return false;
		String host = urlElements[0] + "://" + urlElements[1];
		String hostHashKey = Hasher.hash(host);
		
		Row urlRow = kvsClient.getRow(hostsTable, hostHashKey);
		if (urlRow == null) return false;
		
		long currTime = System.currentTimeMillis();
		String lastAccessedTimeStr = urlRow.get("lastAccessedTime");
		if (lastAccessedTimeStr == null) return false;
		
		long lastAccessedTime = Long.parseLong(lastAccessedTimeStr);
		if (currTime - lastAccessedTime > robots.crawlDelay * 1000) return false;
		
		return true;
	}
	
	
	public static void updateLastAccessedTime(String url, KVSClient kvsClient) throws IOException {
		String[] urlElements = URLParser.parseURL(url);
		if (urlElements[0] == null || urlElements[1] == null) return;
		String host = urlElements[0] + "://" + urlElements[1];
		String hostHashKey = Hasher.hash(host);
		
		kvsClient.put(hostsTable, hostHashKey, "lastAccessedTime", Long.toString(System.currentTimeMillis()));
	}
	
	
	public static boolean allowToCrawl(String domainAbsolutePath, Robots robots) {
		// rule is always a string array with length 2
		for (String[] rule : robots.crawlRules) {
			if (domainAbsolutePath.startsWith(rule[1])) {
				if (rule[0].equals("Allow")) return true;
				else if (rule[0].equals("Disallow")) return false;
			}
		}
		
		return true;
	}
	
	
	public static void writeToLog(KVSClient kvsClient, String message, Exception e) throws IOException {
		Long currTime = System.currentTimeMillis();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String timestampStr = dateFormat.format(new Timestamp(currTime));
		message = timestampStr + ": " + message;
		
		
		String timeHash = Hasher.hash(Long.toString(currTime));
		Row newRow = new Row(timeHash);
		
		if (e != null) {
			message += "\n";
			StringWriter strWriter = new StringWriter();
			PrintWriter printWriter = new PrintWriter(strWriter);
			
			e.printStackTrace(printWriter);
			String exceptionStr = strWriter.toString();
			newRow.put("exception", exceptionStr);
			
			printWriter.close();
		}
		
		newRow.put("message", message);
		kvsClient.putRow(crawlLogTable, newRow);
	}
	
	
	public static void updateURLFrontier(KVSClient kvsClient, Iterable<String> urls, String crawledUrl) throws IOException {
		if (urls != null) {
			for (String url : urls) {
				String urlHashKey = Hasher.hash(url);
				// TODO: don't put url which already exists
				kvsClient.put(urlFrontierTable, urlHashKey, "url", url);
			}
		}
		
		if (crawledUrl == null) return;
		String crawledUrlHashKey = Hasher.hash(crawledUrl);		
		kvsClient.put(urlFrontierTable, crawledUrlHashKey, "crawled", "true");
	}
	
	
	public static HashSet<String> extractURLs(String page, String pageUrl) {
		HashSet<String> urls = new HashSet<String>();
		
		Pattern urlPattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
		Matcher urlMatcher = urlPattern.matcher(page);
		while (urlMatcher.find()) {
			String url = urlMatcher.group(1);
			// Normalize and filter url
			url = normalizeURL(url, pageUrl);
			if (url != null) urls.add(url);
		}
        
		return urls;
	}
	
	
	public static String normalizeURL(String url, String baseUrl) {
		int cutIndex = url.indexOf('#');
		if (cutIndex != -1) {
			url = url.substring(0, cutIndex);
		}
		if (url.equals("") || url.equals("/")) return null;
		
		
		String[] urlElements = URLParser.parseURL(url);
				
		if (filterURL(urlElements[3])) return null;
		
		
		// absolute path
		if (urlElements[0] != null) {
			// Filter out protocols other than http or https
			if (!urlElements[0].equals("http") && !urlElements[0].equals("https")) return null;
			
			if (urlElements[2] == null) {
				if (urlElements[0].equals("http")) urlElements[2] = "80";
				else if (urlElements[0].equals("https")) urlElements[2] = "443";
			}
			url = urlElements[0] + "://" + urlElements[1] + ":" + urlElements[2] + urlElements[3];
			return url;
		}
		
		if (baseUrl == null) return null;
		String[] baseUrlElements = URLParser.parseURL(baseUrl);
		
		// remove the last "/"
//		if (baseUrlElements[3].equals("/")) baseUrlElements[3] = "";
		if (baseUrlElements[3].charAt(baseUrlElements[3].length()-1) == '/') baseUrlElements[3] =  baseUrlElements[3].substring(0, baseUrlElements[3].length()-1);
		
		// absolute path relative to the domain
		if (url.charAt(0) == '/') {
			url = baseUrlElements[0] + "://" + baseUrlElements[1] + ((baseUrlElements[2] == null) ? "" : (":" + baseUrlElements[2])) + url;
			return url;
		}
		
		
		// relative path
		int lastSlashIndex = baseUrlElements[3].lastIndexOf('/');
		if (lastSlashIndex != -1) {
			String baseUrlFile = baseUrlElements[3].substring(lastSlashIndex, baseUrlElements[3].length());
			// if url is ended in abc.html, remove the file in url
			if (baseUrlFile.contains(".")) baseUrlElements[3] = baseUrlElements[3].substring(0, lastSlashIndex);
		}
		

		// relative path contains ..
		// TODO: baseUrlElements[3].lastIndexOf('/') can be -1
		cutIndex = url.indexOf("..");
		while (cutIndex != -1) {
			try {
				baseUrlElements[3] = baseUrlElements[3].substring(0, baseUrlElements[3].lastIndexOf('/'));
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
			
			url = url.substring(cutIndex + 2);
			cutIndex = url.indexOf("..");
		}
		
		
		url = baseUrlElements[0] + "://" + baseUrlElements[1] + ((baseUrlElements[2] == null) ? "" : (":" + baseUrlElements[2])) 
				+ baseUrlElements[3] + (url.charAt(0) == '/' ? "" : "/") + url;
		
		return url;
	}
	
	
	// true means to drop the url
	public static boolean filterURL(String domainAbsolutePath) {
		// filter file formats
		int lastDotIndex = domainAbsolutePath.lastIndexOf('.');
		if (lastDotIndex != -1 && lastDotIndex + 1 < domainAbsolutePath.length()) {
			String urlFileFormat = domainAbsolutePath.substring(lastDotIndex + 1, domainAbsolutePath.length());
			urlFileFormat = urlFileFormat.toLowerCase();
			if (urlFileFormat.equals("jpg") || urlFileFormat.equals("jpeg") || urlFileFormat.equals("gif") || urlFileFormat.equals("png")
					|| urlFileFormat.equals("txt")) {
				return true;
			}
		}
		
		// filter urls like this: /a/b/c/d/e/f (6 slashes)
		int slashCount = 0;
		for (int i = 0; i < domainAbsolutePath.length(); i++) {
			if (domainAbsolutePath.charAt(i) == '/') slashCount++;
		}
		if (slashCount > 5) return true;
		
		
		return false;
	}
	
	
	public static boolean isInEnglish(String htmlPage) {
		Pattern pattern = Pattern.compile("<html.*lang=\"(.*?)\".*>", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(htmlPage);
		if (matcher.find()) {
			String language = matcher.group(1);
			if (language.contains("en")) return true;
			else return false;
		}
		
		// Remove all html tags
		htmlPage = htmlPage.replaceAll("<script.*?>.*?</script>", "");
		htmlPage = htmlPage.replaceAll("<.*?>", "");
		// Remove whitespace and punctuation
		htmlPage = htmlPage.replaceAll("[\\s\\p{Punct}]", "");
		
		
		// TODO: improve the English detection method. This method can't distinguish languages like French 
		int englishCount = 0;
		for (int i = 0; i < htmlPage.length(); i++) {
			char c = htmlPage.charAt(i);
			if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) englishCount++;
		}
		
		// 70% of characters should be in English
		if (englishCount / ((float)htmlPage.length()) > 0.7f) {
			return true;
		}
		
		return false;
	}
	
}
