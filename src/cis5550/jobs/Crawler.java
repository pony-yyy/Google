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
	public final static String robotsTable = "robots";
	public final static String urlFrontierTable = "urlFrontier";
	public final static String crawlLogTable = "crawlLog";
	
	
	public final static int pageSizeLimit = 2200000; // 2.2MB
	public final static long crawlTimeLimit = 2; // in seconds
	
	public final static int linkBacklinkRatio = 1;
	
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
		contextKVS.persist(robotsTable);
		contextKVS.persist(urlFrontierTable);
		contextKVS.persist(crawlLogTable);
		
		
		FlameRDD.StringToIterable crawlLambda = crawlLambda(masterAddr);
		
		
		FlameContext.RowToString getUrlFrontier = (row) -> {
			String url = row.get("url");
			if (url == null) return null;
			
			String crawledStr = row.get("crawled");
			// crawled column is not null. This means the crawled is set to "true"
			if (crawledStr != null) return null;
			
			return url;
		};
		
		
		ArrayList<String> seedUrls = new ArrayList<String>();
		
		for (String seedUrl : args) {
			seedUrl = normalizeURL(seedUrl, null);
			if (seedUrl != null) seedUrls.add(seedUrl);
		}
		
		updateURLFrontier(contextKVS, seedUrls, null);
		FlameRDD urlQueue = context.fromTable(urlFrontierTable, getUrlFrontier);
		
		FlameRDD nextUrlQueue = urlQueue;
		
		
		writeToLog(contextKVS, urlQueue.count() + " urls are loaded. Start crawling.", null);
		
		while (nextUrlQueue.count() != 0) {
			try {
				urlQueue = nextUrlQueue;
				
				nextUrlQueue = urlQueue.flatMap(crawlLambda);
				urlQueue.delete();
				
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
		
		
//		writeToLog(kvsClient, "Start crawling " + urlArg, null);
			
		// Don't stop the flame worker if something is wrong
		try {
		///////////////////////////// TRY BLOCK STARTS /////////////////////////////
				
		// skip crawled url
		if (kvsClient.existsRow(crawlTable, urlHashKey)) {
			updateURLFrontier(kvsClient, null, urlArg);
			return newUrls;
		}
		// Give up some urls based on statistics
		if (!willingToCrawlURL(kvsClient, urlArg, true)) {
//			writeToLog(kvsClient, "Give up crawling " + urlArg + " based on statistics", null);
			updateURLFrontier(kvsClient, null, urlArg);
			return newUrls;
		}
			
		// check robots.txt and see if the url is allowed to be crawled
		Robots robots = checkRobot(urlArg, kvsClient);
		String[] urlElements = URLParser.parseURL(urlArg);
		if (!allowToCrawl(urlElements[3], robots)) {
			updateHostsFailedAccess(kvsClient, getHostURL(urlElements), "robotsBanAccess");
			updateURLFrontier(kvsClient, newUrls, urlArg);
			return newUrls;
		}
			
		// Check whether crawling is too frequent
		if (isFrequentAccess(urlArg, kvsClient, robots)) {
//			writeToLog(kvsClient, "Frequent Access " + urlArg, null);
			newUrls.add(urlArg);
			// no need to update urlFrontier here
			return newUrls;
		}
			
		updateLastAccessedTime(urlArg, kvsClient);
			
			
		URL url = new URL(urlArg);
			
		// Send HEAD request first
		HttpURLConnection headConnection = (HttpURLConnection) url.openConnection();
		headConnection.setConnectTimeout(1000);
		headConnection.setReadTimeout(1000);
		headConnection.setInstanceFollowRedirects(false);
		headConnection.setRequestMethod("HEAD");
		headConnection.setRequestProperty("User-Agent", crawlerName);
		headConnection.connect();
			
		// Only crawl English contents
		String contentLanguage = headConnection.getHeaderField("Content-Language");
		if (contentLanguage != null && !contentLanguage.contains("en")) {
			updateHostsFailedAccess(kvsClient, getHostURL(urlElements), "wrongLangAccess");
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
			headConnection.disconnect();
			
			kvsClient.putRow(crawlTable, newRow);
				
			if (headStatusCode >= 400 && headStatusCode < 500) updateHostsFailedAccess(kvsClient, getHostURL(urlElements), "forbiddenAccess");
			updateURLFrontier(kvsClient, newUrls, urlArg);
			return newUrls;
		}
		headConnection.disconnect();
			
			
		// Send GET request if the head request returns status code 200
		HttpURLConnection getConnection = (HttpURLConnection) url.openConnection();
		getConnection.setConnectTimeout(1000);
		getConnection.setReadTimeout(1000);
		getConnection.setInstanceFollowRedirects(false);
		getConnection.setRequestMethod("GET");
		getConnection.setRequestProperty("User-Agent", crawlerName);
		getConnection.connect();
		
		if (contentType != null && contentType.contains("text/html")) {
			InputStream inputStream = getConnection.getInputStream();
			byte[] pageByte = inputStream.readNBytes(pageSizeLimit);
			String page = new String(pageByte);
			
			getConnection.disconnect();
			// remove all useless tags to save space
			page = removeUselessTags(page);
			
				
			if (isInEnglish(page)) {
				String canonicalUrl = extractCanonicalUrl(page, urlArg);
				if (canonicalUrl != null && !canonicalUrl.equals(urlArg)) {
					newRow.put("canonicalURL", canonicalUrl);
					newUrls.add(canonicalUrl);
				}
				else {
					newRow.put("page", page);
					newUrls = extractURLs(page, urlArg);
					
					String currHost = getHostURL(urlElements);
					updateHostsBacklinkCount(kvsClient, newUrls, currHost);
					
					// Give up some urls based on statistics
					newUrls = willingToCrawlURLs(kvsClient, newUrls);
				}
				
			}
			else {
				updateHostsFailedAccess(kvsClient, getHostURL(urlElements), "wrongLangAccess");
				
				writeToLog(kvsClient, "Give up crawling (2) a foreign-language page at " + urlArg, null);
				// Don't record the url in the crawlTable
				updateURLFrontier(kvsClient, newUrls, urlArg);
				return newUrls;
			}
				
		}
		getConnection.disconnect();
			
		kvsClient.putRow(crawlTable, newRow);
			
		updateURLFrontier(kvsClient, newUrls, urlArg);
		return newUrls;
			
		///////////////////////////// TRY BLOCK ENDS /////////////////////////////
		} catch (SocketTimeoutException e) {
			updateHostsFailedAccess(kvsClient, getHostURL(URLParser.parseURL(urlArg)), "timeoutAccess");
			
			writeToLog(kvsClient, "Connection or read timeout: " + urlArg, null);
				
			updateURLFrontier(kvsClient, newUrls, urlArg);
			return newUrls;
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
	        	// give up crawling this url
	        	
	        	updateHostsFailedAccess(kvsClient, getHostURL(URLParser.parseURL(urlArg)), "timeoutAccess");
	        	updateURLFrontier(kvsClient, null, urlArg);
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
		String host = getHostURL(urlElements);
		String hostHashKey = Hasher.hash(host);
		
		
		Row hostRow = kvsClient.getRow(robotsTable, hostHashKey);
		
		
		String content = "";
		if (hostRow == null) {			
			URL robotUrl = new URL(host + "/robots.txt");
			HttpURLConnection getConnection = (HttpURLConnection) robotUrl.openConnection();
//			getConnection.setConnectTimeout(2000);
//			getConnection.setReadTimeout(2000);
			getConnection.setRequestMethod("GET");
			getConnection.setRequestProperty("User-Agent", crawlerName);
			getConnection.connect();
			
			int getStatusCode = getConnection.getResponseCode();
			if (getStatusCode != 200) return new Robots();
			
			InputStream inputStream = getConnection.getInputStream();
			byte[] contentByte = inputStream.readAllBytes();
			content = new String(contentByte);
			// Only keep the User-agent: * part for the crawler
			Pattern userAgentPattern = Pattern.compile("(?i)User-agent: \\*(?s)(.*?)(?=\\nUser-agent:|\\z)");
			Matcher userAgentMatcher = userAgentPattern.matcher(content);
			if (userAgentMatcher.find()) {
				content = userAgentMatcher.group(0);
			}
			else content = "NULL";
			
			hostRow = new Row(hostHashKey);
			hostRow.put("host", host);
			hostRow.put("robots", content);
			kvsClient.putRow(robotsTable, hostRow);
		}
		else {
			content = hostRow.get("robots");
			if (content == null) return new Robots();
		}
		
		return parseRobot(content);
	}
	
	
	public static Robots parseRobot(String content) {
		Robots robots = new Robots();
		
		int startIndex = content.indexOf("User-agent: " + crawlerName);
		// some robots.txt uses User-Agent instead of User-agent, e.g. www.reddit.com
		if (startIndex == -1) startIndex = content.indexOf("User-Agent: " + crawlerName);
		
		if (startIndex == -1) {
			startIndex = content.indexOf("User-agent: *");
			// some robots.txt uses User-Agent instead of User-agent
			if (startIndex == -1) startIndex = content.indexOf("User-Agent: *");
			
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
		String host = getHostURL(urlElements);
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
		String host = getHostURL(urlElements);
		String hostHashKey = Hasher.hash(host);
		
		Row hostRow = kvsClient.getRow(hostsTable, hostHashKey);
		if (hostRow == null) {
			hostRow = new Row(hostHashKey);
			hostRow.put("host", host);
		}
		
		hostRow.put("lastAccessedTime", Long.toString(System.currentTimeMillis()));
		int accessCount = 0;
		String accessCountStr = hostRow.get("access");
		if (accessCountStr != null) accessCount = Integer.parseInt(accessCountStr);
		accessCount++;
		hostRow.put("access", Integer.toString(accessCount));
		
		kvsClient.putRow(hostsTable, hostRow);
	}
	
	
	public static boolean allowToCrawl(String domainAbsolutePath, Robots robots) {
		// rule is always a string array with length 2
		for (String[] rule : robots.crawlRules) {
//			if (domainAbsolutePath.startsWith(rule[1])) {
//				if (rule[0].equals("Allow")) return true;
//				else if (rule[0].equals("Disallow")) return false;
//			}

			// Support matching with * operator
			boolean hasMatch = true;
			int strIndex = 0;
			for (int i = 0; i < rule[1].length(); i++) {
				char c = rule[1].charAt(i);
				
				if (i == rule[1].length()-1 && c == '/' && domainAbsolutePath.charAt(strIndex) == '/') break;
				else if (c != '*' && c != domainAbsolutePath.charAt(strIndex)) {
					hasMatch = false;
					break;
				}
				// match everything until next character of the rule is matched
				else if (c == '*' && i+1 < rule[1].length()) {
					i++;
					char nextChar = rule[1].charAt(i);
					while (strIndex < domainAbsolutePath.length() && domainAbsolutePath.charAt(strIndex) != nextChar) strIndex++;
					
					if (strIndex >= domainAbsolutePath.length()) {
						hasMatch = false;
						break;
					}
					else if (domainAbsolutePath.charAt(strIndex) == nextChar) continue;
				}
				else if (c == '*' && i+1 >= rule[1].length()) break;
				
				strIndex++;
				if (strIndex >= domainAbsolutePath.length()) {
					hasMatch = false;
					break;
				}
			}
			
			if (hasMatch) {
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
				
				Row urlRow = kvsClient.getRow(urlFrontierTable, urlHashKey);
				
				
				if (urlRow == null) {
					kvsClient.put(urlFrontierTable, urlHashKey, "url", url);
					continue;
				}
				
//				kvsClient.putRow(urlFrontierTable, urlRow);
			}
		}
		
		if (crawledUrl == null) return;
		String crawledUrlHashKey = Hasher.hash(crawledUrl);		
		kvsClient.put(urlFrontierTable, crawledUrlHashKey, "crawled", "true");
		
	}
	
	
	public static HashSet<String> extractURLs(String page, String pageUrl) {
		HashSet<String> urls = new HashSet<String>();
		
//		Pattern urlPattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
		Pattern urlPattern = Pattern.compile("<a[^>]+?href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
		
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
			// Filter out weired domain with "?" 
			if (urlElements[1].contains("?")) return null;
			
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
//				e.printStackTrace();
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
		// Remove urls with length greater than 300
		if (domainAbsolutePath.length() > 300) return true;
		
		// filter file formats
		int lastDotIndex = domainAbsolutePath.lastIndexOf('.');
		if (lastDotIndex != -1 && lastDotIndex + 1 < domainAbsolutePath.length()) {
			String urlFileFormat = domainAbsolutePath.substring(lastDotIndex + 1, domainAbsolutePath.length());
			urlFileFormat = urlFileFormat.toLowerCase();
			if (urlFileFormat.equals("jpg") || urlFileFormat.equals("jpeg") || urlFileFormat.equals("gif") || urlFileFormat.equals("png")
					|| urlFileFormat.equals("txt") || urlFileFormat.equals("js") || urlFileFormat.equals("pdf")) {
				return true;
			}
		}
		
		
		int slashCount = 0;
		for (int i = 0; i < domainAbsolutePath.length(); i++) {
			// Remove all urls with queries!!!
			if (domainAbsolutePath.charAt(i) == '?') return true;
			if (domainAbsolutePath.charAt(i) == '/') slashCount++;
		}
		// filter urls like this: /a/b/c/d/e/f (6 slashes)
		if (slashCount > 5) return true;
		
		
		return false;
	}
	
	
	public static boolean isInEnglish(String htmlPage) {		
		Pattern pattern = Pattern.compile("<html[^>]+lang=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(htmlPage);
		if (matcher.find()) {
			String language = matcher.group(1);
			language = language.toLowerCase();
			if (language.contains("en")) return true;
			else return false;
		}
		
		// Remove all html tags (script tag is already removed for html page previously)
//		htmlPage = htmlPage.replaceAll("<script[^>]*?>[\\s\\S]*?</script>", "");		
		htmlPage = htmlPage.replaceAll("<[^>]*?>", "");
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
	
	
	public static String extractCanonicalUrl(String htmlPage, String pageUrl) {
		Pattern headPattern = Pattern.compile("<head\\b[^>]*>[\\s\\S]*?</head>", Pattern.CASE_INSENSITIVE);
		Matcher headMatcher = headPattern.matcher(htmlPage);
		String headSection;
		if (headMatcher.find()) {
			headSection = headMatcher.group();
		}
		else return null;
		
		
		Pattern pattern = Pattern.compile("<link[^>]+rel=\"canonical\"[^>]+href=\"([^\"]*)\"", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(headSection);
		if (matcher.find()) {
			String canonicalUrl = matcher.group(1);
			canonicalUrl = normalizeURL(canonicalUrl, pageUrl);
			return canonicalUrl;
		}
		else return null;
		
	}
	
	
	public static String getHostURL(String[] urlElements) {
		return urlElements[0] + "://" + urlElements[1];
	}
	
	
	public static void updateHostsBacklinkCount(KVSClient kvsClient, Iterable<String> outLinks, String currHost) throws IOException {
//		writeToLog(kvsClient, "Start updating backLinks", null);
		
		// Multiple links on a page to a single host only count once
		HashSet<String> outHosts = new HashSet<String>();
		for (String outLink : outLinks) {
			String outHost = getHostURL(URLParser.parseURL(outLink));			
			outHosts.add(outHost);
		}
		
		for (String outHost : outHosts) {
			String outHostHashKey = Hasher.hash(outHost);
			
			Row outHostRow = kvsClient.getRow(hostsTable, outHostHashKey);
			if (outHostRow == null) {
				outHostRow = new Row(outHostHashKey);
				outHostRow.put("host", outHost);
			}
			
			
			if (!similarHosts(currHost, outHost)) {
				int backlinkCount = 0;
				String backLinkCountStr = outHostRow.get("backlinkCount");
				if (backLinkCountStr != null) backlinkCount = Integer.parseInt(backLinkCountStr);
				backlinkCount++;
				
				int access = 0;
				int robotsBanAccess = 0;
				String accessStr = outHostRow.get("access");
				if (accessStr != null) access = Integer.parseInt(accessStr);
				String robotsBanAccessStr = outHostRow.get("robotsBanAccess");
				if (robotsBanAccessStr != null) robotsBanAccess = Integer.parseInt(robotsBanAccessStr);
				
				// Limit the number of urls from the same host in a single queue
				if (backlinkCount > access + robotsBanAccess + 20) continue;
				
				outHostRow.put("backlinkCount", Integer.toString(backlinkCount));
			}
			
			kvsClient.putRow(hostsTable, outHostRow);
			
		}
		
//		writeToLog(kvsClient, "End updating backLinks", null);
	}
	
	
	public static void updateHostsFailedAccess(KVSClient kvsClient, String host, String failedAccessColKey) throws IOException {
		String hostHashKey = Hasher.hash(host);
		
		Row hostRow = kvsClient.getRow(hostsTable, hostHashKey);
		if (hostRow == null) {
			hostRow = new Row(hostHashKey);
		}
		
		int failedAccess = 0;
		String failedAccessStr = hostRow.get(failedAccessColKey);
		if (failedAccessStr != null) failedAccess = Integer.parseInt(failedAccessStr);
		failedAccess++;
		hostRow.put(failedAccessColKey, Integer.toString(failedAccess));
		
		kvsClient.putRow(hostsTable, hostRow);
	}
	
	
	public static boolean willingToCrawlURL(KVSClient kvsClient, String url, boolean isInQueue) throws IOException {
		String host = getHostURL(URLParser.parseURL(url));
		String hostHashKey = Hasher.hash(host);
		
		Row hostRow = kvsClient.getRow(hostsTable, hostHashKey);
		if (hostRow == null) return true;
		
		
		int access = 0;
		int failedAccess = 0;
		
		int robotsBanAccess = 0;
		int timeoutAccess = 0;
		int wrongLangAccess = 0;
		int forbiddenAccess = 0;
		
		
		String accessStr = hostRow.get("access");
		if (accessStr != null) access = Integer.parseInt(accessStr);
		
		
		String robotsBanAccessStr = hostRow.get("robotsBanAccess");
		if (robotsBanAccessStr != null) robotsBanAccess = Integer.parseInt(robotsBanAccessStr);
		String timeoutAccessStr = hostRow.get("timeoutAccess");
		if (timeoutAccessStr != null) timeoutAccess = Integer.parseInt(timeoutAccessStr);
		String wrongLangAccessStr = hostRow.get("wrongLangAccess");
		if (wrongLangAccessStr != null) timeoutAccess = Integer.parseInt(wrongLangAccessStr);
		String forbiddenAccessStr = hostRow.get("forbiddenAccess");
		if (forbiddenAccessStr != null) forbiddenAccess = Integer.parseInt(forbiddenAccessStr);
		
		
		int totalAccess = access + robotsBanAccess;
		
		
		int backlinkCount = 0;
		String backlinkCountStr = hostRow.get("backlinkCount");
		if (backlinkCountStr != null) backlinkCount = Integer.parseInt(backlinkCountStr);
		int linkCount = 1;
		String linkCountStr = hostRow.get("linkCount");
		if (linkCountStr != null) linkCount = Integer.parseInt(linkCountStr);
		
		if (isInQueue) {
			if (backlinkCount == 0 && linkCount >= 1 && totalAccess == 1) return false;
			if (backlinkCount != 0 && totalAccess >= linkCount) return false;
		}
		else {
			if (backlinkCount == 0 && linkCount >= 1) return false;
			if (backlinkCount != 0 && linkCount > backlinkCount * linkBacklinkRatio) return false;
		}
		
		
		
		failedAccess = timeoutAccess + wrongLangAccess + forbiddenAccess;
		
		// Simply assume that robots.txt banned all directories in this case
		if (access <= 3 && robotsBanAccess > 10) return false;
		else if (access >= 3 && (wrongLangAccess * 2) > access) return false;
		else if (access >= 5 && (timeoutAccess * 5) > access) return false;
		else if (access >= 10 && (failedAccess * 5) > (access * 4)) return false;
		
		double dropProbability = Math.random();
		if (access >= 5 && timeoutAccess > 0 && dropProbability < (timeoutAccess / (double)access)) return false;
		
		
		
		return true;
	}
	
	
	public static HashSet<String> willingToCrawlURLs(KVSClient kvsClient, Iterable<String> urls) throws IOException {
		
		HashSet<String> crawlUrls = new HashSet<String>();
		for (String outLink : urls) {
			if (willingToCrawlURL(kvsClient, outLink, false)) {
				String urlHashKey = Hasher.hash(outLink);
				if (kvsClient.existsRow(crawlTable, urlHashKey)) continue;
				
				String outHost = getHostURL(URLParser.parseURL(outLink));
				
				String outHostHashKey = Hasher.hash(outHost);
				Row outHostRow = kvsClient.getRow(hostsTable, outHostHashKey);
				if (outHostRow == null) {
					outHostRow = new Row(outHostHashKey);
					outHostRow.put("host", outHost);
				}
						
				// linkCount is the number of links for crawling, backlinkCount is the number of links
				// from other websites
				int linkCount = 1;
				String linkCountStr = outHostRow.get("linkCount");
				if (linkCountStr != null) linkCount = Integer.parseInt(linkCountStr) + 1;
						
				int backlinkCount = 0;
				String backlinkCountStr = outHostRow.get("backlinkCount");
				if (backlinkCountStr != null) backlinkCount = Integer.parseInt(backlinkCountStr);
				if (backlinkCount != 0 && linkCount <= backlinkCount * linkBacklinkRatio) {
					crawlUrls.add(outLink);
					
					outHostRow.put("linkCount", Integer.toString(linkCount));
					kvsClient.putRow(hostsTable, outHostRow);
				}
				
			}
		}
		return crawlUrls;
	}
	
	
	public static boolean similarHosts(String host1, String host2) {
		String[] host1Elements = URLParser.parseURL(host1);
		String[] host2Elements = URLParser.parseURL(host2);
		
		if (host1Elements[1] == null || host2Elements[1] == null) return false;
		String[] host1Names = host1Elements[1].split("\\.");
		String[] host2Names = host2Elements[1].split("\\.");
		
		String[] shortHost = host1Names;
		String[] longHost = host2Names;
		if (host1Names.length > host2Names.length) {
			shortHost = host2Names;
			longHost = host1Names;
		}
		
		HashSet<String> words = new HashSet<String>(Arrays.asList(longHost));
		// length - 1 so that domain extensions won't be included
		for (int i = 0; i < shortHost.length - 1; i++) {
			if (shortHost[i].equals("www")) continue;
			if (words.contains(shortHost[i])) return true;
		}
		
		return false;
	}
	
	
	public static String removeUselessTags(String page) {
		page = page.replaceAll("<(script|style)[^>]*?>[\\s\\S]*?</\\1>", "");
		page = page.replaceAll("<(script|style)[^>]*?>[\\s\\S]*", "");
		
		
		page = page.replaceAll("<(?!/|a|head|title|html|link\srel=\"canonical\")[^>]*>", "");
		page = page.replaceAll("</(?!a|head|title|html|link)[^>]*>", "");
		
		page = page.replaceAll("<a[^>]*(href=\"[^\"]*\")[^>]*>", "<a $1>");
//		page = page.replaceAll("<a[^>]*(?!href=\"[^\"]*\")[^>]*>", "<a>");
		
		page = page.replaceAll("<(article|header)[^>]*>", "");
		page = page.replaceAll("</(article|header)[^>]*>", "");
		
		page = page.replaceAll("\\s+", " ");
		
		return page;
	}
	
}
