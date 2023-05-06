package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

public class Indexer {
    public static void run(FlameContext context, String[] strings) throws Exception {
        FlameRDD tmpRDD = context.fromTable("crawl", row -> {
            return row.get("url") + "\r\n" + row.get("page");
        });
        FlamePairRDD pagesRDD = tmpRDD.mapToPair(s -> {
            String[] splits = s.split("\\r\\n", 2);
            return new FlamePair(splits[0], splits[1]);
        });
        pagesRDD = pagesRDD.flatMapToPair(pair ->{
            //filter tags
            String page = pair._2().replaceAll("(<!DOCTYPE[^>]*>|<!--[\\s\\S]*?-->)", " ").toLowerCase(); //first filter comments
            String regex = "<(/?)\\s*([a-zA-Z0-9]*)(>|\\s+(\"[^\"]*\"|'[^']*'|[^'\">])*>)";
            String[] filterTag = new String[]{"head", "script", "style"}; //todo add more tags that the content need to be ignored
            int pageIndex = 0;
            StringBuilder sb = new StringBuilder();
            Pattern tagPattern = Pattern.compile(regex);
            Matcher tagMatcher = tagPattern.matcher(page);
            while (tagMatcher.find()){
                sb.append(page, pageIndex, tagMatcher.start()).append(" ");
                pageIndex = tagMatcher.end();
                if (tagMatcher.group(1).isEmpty()){
                    boolean flag = false;
                    String ctag = "";
                    for (String ft : filterTag){
                        if (tagMatcher.group(2).equals(ft)){
                            flag = true;
                            ctag = ft;
                            break;
                        }
                    }
                    if (flag){
                        while(tagMatcher.find() && !(tagMatcher.group(1).equals("/") && tagMatcher.group(2).equals(ctag))) {}
                        try{
                            pageIndex = tagMatcher.end();
                        }
                        catch (Exception e){
                            pageIndex = page.length();
                            break;
                        }
                    }
                }
            }
            sb.append(page.substring(pageIndex));

            //seperate words
            Pattern pattern = Pattern.compile("\\b\\w+\\b");
            Matcher matcher = pattern.matcher(sb.toString());
            StringBuilder result = new StringBuilder();
            while (matcher.find()){
                if (!result.isEmpty()){
                    result.append(",");
                }
                result.append(matcher.group());
            }
            return Collections.singletonList(new FlamePair(pair._1(), result.toString()));
        });
        pagesRDD.saveAsTable("urlpages");

        FlamePairRDD indexRDD = pagesRDD.flatMapToPair(pair -> {
            Map<String, List<Integer>> wordsPos = new HashMap<>();
            int wordIndex = 0;
            String[] words = pair._2().split(",");
            for (String word : words){
                wordIndex++;
                if (!wordsPos.containsKey(word)) {
                    wordsPos.put(word, new ArrayList<>());
                }
                wordsPos.get(word).add(wordIndex);
                Stemmer stemmer = new Stemmer();
                stemmer.add(word.toCharArray(), word.length());
                stemmer.stem();
                String stemmedWord = stemmer.toString();
                if (!stemmedWord.equals(word)){
                    if (!wordsPos.containsKey(stemmedWord)){
                        wordsPos.put(stemmedWord, new ArrayList<>());
                    }
                    wordsPos.get(stemmedWord).add(wordIndex);
                }
            }

            //generate results
            List<FlamePair> results = new ArrayList<>();
            for (var wp : wordsPos.entrySet()){
                StringBuilder sb = new StringBuilder();
                sb.append(wp.getValue().size()).append(" ");
                sb.append(pair._1());
                for (int i : wp.getValue()){
                    sb.append(" ").append(i);
                }
                results.add(new FlamePair(wp.getKey(), sb.toString()));
            }
            return results;
        });

        indexRDD = indexRDD.foldByKey("", (s, s1) -> {
            if (s.isEmpty()){
                return s1;
            }
            else{
                return s + "," + s1;
            }
        });
        indexRDD = indexRDD.flatMapToPair(pair -> {
            String[] urls = pair._2().split(",");
            List<String[]> urlElements = new ArrayList<>();
            for (String url : urls) {
                String[] uSplit = url.split(" ", 3);
                urlElements.add(uSplit);
            }
            urlElements.sort((o1, o2) -> Integer.compare(Integer.parseInt(o2[0]), Integer.parseInt(o1[0])));
            StringBuilder sb = new StringBuilder();
            sb.append(urlElements.get(0)[1]).append(":").append(urlElements.get(0)[2]);
            for (int i = 1; i < urlElements.size(); i++){
                sb.append(",").append(urlElements.get(i)[1]).append(" ").append(urlElements.get(i)[2]); //todo :
            }
            return Collections.singletonList(new FlamePair(pair._1(), sb.toString()));
        });
        indexRDD.saveAsTable("index");
    }
}
