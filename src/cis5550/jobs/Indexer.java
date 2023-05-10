package cis5550.jobs;

import cis5550.flame.*;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Indexer {
    public static void savePersistentTable(FlameContext context, FlamePairRDD rdd, String name) throws Exception{
        context.getKVS().persist(name);
        rdd.flatMap(pair -> {
            pair = decodePair(pair);
            context.getKVS().put(name, pair._1(), "value", pair._2());  //todo just fit index table
            return Collections.emptyList();
        });
    }
    public static void savePersistentTable(FlameContext context, FlameRDD rdd, String name) throws Exception{
        context.getKVS().persist(name);
        rdd.flatMap(s -> {
            context.getKVS().put(name, Hasher.hash(s), "value", s);     // it will remove the same elements
            return Collections.emptyList();
        });
    }

    public static FlamePair encodePair(String a, String b){
        return new FlamePair(Hasher.hash(a).substring(1, 6)+ ":" + a, b);
    }
    public static FlamePair decodePair(FlamePair pair){
        return new FlamePair(pair._1().substring(6), pair._2());
    }

    public static void indexJoin(FlameContext context, FlamePairRDD indexRDD) throws Exception{
        indexRDD.flatMap(pair -> {
            //pair = decodePair(pair);
            byte[] origin = context.getKVS().get("index", pair._1(), "value");
            if (origin == null){
                context.getKVS().put("index", pair._1(), "value", pair._2());
            }
            else{
                String sorigin = new String(origin);
                String[] sorigins = sorigin.split("\r\n", 2);
                String[] laters = pair._2().split("\r\n", 2);
                int num = Integer.parseInt(sorigins[0]) + Integer.parseInt(laters[0]);
                context.getKVS().put("index", pair._1(), "value", String.valueOf(num) + "\r\n" + sorigins[1] + "\r\n" + laters[1]);
            }
            return null;
        });
    }

    public static void run(FlameContext context, String[] strings) throws Exception {
        int iterNum = 6;
        int interval = 25 / iterNum;
        int pageNum = 0;
        context.getKVS().persist("index");
        for (int iteri = 0; iteri <= iterNum; iteri++) {
            context.output("round " + String.valueOf(iteri) + " start\n");

            List<String> rowStrings = new ArrayList<>();
            String startString = Character.toString((char)'a' + iteri * interval);
            String endString = iteri == iterNum ? null : Character.toString((char)('a' + (iteri + 1)*interval));
            for (Iterator<Row> partRows = context.getKVS().scan("crawl", startString, endString); partRows.hasNext();){
                Row row = partRows.next();
                if (row.get("url").contains("..") || row.get("page") == null || row.get("page").isEmpty()) {
                    continue;
                }    //todo ..bug
                rowStrings.add(row.get("url").trim().split("\\s")[0].replace(",", "%2C") + "\r\n" + row.get("page"));
            }
            FlameRDD tmpRDD = context.parallelize(rowStrings);

//            FlameRDD tmpRDD = context.fromTable("crawl", row -> {
//                if (row.get("url").contains("..") || row.get("page") == null || row.get("page").isEmpty() || row.key().charAt(1) > 'a' + (iteri+1)*interval) {
//                    return null;
//                }    //todo ..bug
//                return row.get("url").trim().split("\\s")[0].replace(",", "%2C") + "\r\n" + row.get("page");    //the last character of key can't be space
//            });
            pageNum += tmpRDD.count();
            FlamePairRDD pagesRDD = tmpRDD.mapToPair(s -> {
                String[] splits = s.split("\\r\\n", 2);
                return encodePair(splits[0], splits[1]);
            });
            tmpRDD.delete();
            context.getKVS().persist("urlpages");
            FlamePairRDD wordsRDD = pagesRDD.flatMapToPair(pair -> {
                pair = decodePair(pair);
                //filter tags
                //var time = System.currentTimeMillis();
                System.out.println("Extracting words: " + pair._1());
                try {
                    String pageTitle = null;
                    String page = pair._2().replaceAll("(<!DOCTYPE[^>]*>|<!--[\\s\\S]*?-->)", " ").toLowerCase(); //first filter comments
                    String regex = "<(/?)([a-zA-Z0-9]+)[^>]*>";
                    String[] filterTag = new String[]{"head", "script", "style"}; //todo add more tags that the content need to be ignored
                    int pageIndex = 0;
                    StringBuilder sb = new StringBuilder();
                    Pattern tagPattern = Pattern.compile(regex);
                    Matcher tagMatcher = tagPattern.matcher(page);
                    while (tagMatcher.find()) {
                        sb.append(page, pageIndex, tagMatcher.start()).append(" ");
                        pageIndex = tagMatcher.end();
                        if (tagMatcher.group(1).isEmpty()) {
                            boolean flag = false;
                            String ctag = "";
                            for (String ft : filterTag) {
                                if (tagMatcher.group(2).equals(ft)) {
                                    flag = true;
                                    ctag = ft;
                                    break;
                                }
                            }
                            if (flag) {
                                int titleStart = -1;
                                while (tagMatcher.find() && !(tagMatcher.group(1).equals("/") && tagMatcher.group(2).equals(ctag))) {
                                    if (pageTitle == null) {
                                        if (tagMatcher.group(2).equals("title") && tagMatcher.group(1).isEmpty()) {
                                            titleStart = tagMatcher.end();
                                        } else if (tagMatcher.group(2).equals("title") && tagMatcher.group(1).equals("/") && titleStart > 0) {
                                            pageTitle = page.substring(titleStart, tagMatcher.start());
                                        }
                                    }
                                }
                                try {
                                    pageIndex = tagMatcher.end();
                                } catch (Exception e) {
                                    pageIndex = page.length();
                                    break;
                                }
                            }
                        }
                    }
                    sb.append(page.substring(pageIndex));

                    Row newRow = new Row(pair._1());
                    newRow.put("title", pageTitle == null ? "" : pageTitle);
                    newRow.put("content", sb.toString().replaceAll("[^ -~]", ""));
                    context.getKVS().putRow("urlpages", newRow);

                    return Collections.singletonList(encodePair(pair._1(), sb.toString()));
                } catch (Exception e) {
                    e.printStackTrace();
                    return Collections.emptyList();
                }
            });
            pagesRDD.delete();

            FlamePairRDD indexRDD = wordsRDD.flatMapToPair(pair -> {
                pair = decodePair(pair);
                System.out.println("Indexing: " + pair._1());

                Pattern pattern = Pattern.compile("\\b\\w+\\b");
                Matcher matcher = pattern.matcher(pair._2());
                List<String> words = new ArrayList<>();
                while (matcher.find()) {
                    words.add(matcher.group());
                }

                Map<String, List<Integer>> wordsPos = new HashMap<>();
                int wordIndex = 0;
                for (String word : words) {
                    wordIndex++;
                    if (word.length() > 40 || word.length() <= 1) {
                        continue;
                    }
                    if (!wordsPos.containsKey(word)) {
                        wordsPos.put(word, new ArrayList<>());
                    }
                    wordsPos.get(word).add(wordIndex);
                    Stemmer stemmer = new Stemmer();
                    stemmer.add(word.toCharArray(), word.length());
                    stemmer.stem();
                    String stemmedWord = stemmer.toString();
                    if (!stemmedWord.equals(word)) {
                        if (!wordsPos.containsKey(stemmedWord)) {
                            wordsPos.put(stemmedWord, new ArrayList<>());
                        }
                        wordsPos.get(stemmedWord).add(wordIndex);
                    }
                }
                float wordCountInverse = 1.f / wordIndex;

                //generate results
                List<FlamePair> results = new ArrayList<>();
                for (var wp : wordsPos.entrySet()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(wp.getValue().size() * wordCountInverse).append(" ");
                    sb.append(pair._1());
                    for (int i : wp.getValue()) {
                        sb.append(" ").append(i);
                    }
                    results.add(encodePair(wp.getKey(), sb.toString()));
                }
                //System.out.println((System.currentTimeMillis() - time) / 1000.f);

                return results;
            });
            wordsRDD.delete();

            FlamePairRDD indexRDD1 = indexRDD.foldByKey("", (s, s1) -> {
                if (s.isEmpty()) {
                    return s1;
                } else {
                    return s + "\r\n" + s1;
                }
            });
            indexRDD.delete();
            FlamePairRDD indexRDD2 = indexRDD1.flatMapToPair(pair -> {
                pair = decodePair(pair);
                //System.out.println("Combining: " + pair._1());

                String[] urls = pair._2().split("\r\n");
                List<String[]> urlElements = new ArrayList<>();
                for (String url : urls) {
                    String[] uSplit = url.split(" ", 2);
                    urlElements.add(uSplit);
                }
                urlElements.sort((o1, o2) -> Float.compare(Float.parseFloat(o2[0]), Float.parseFloat(o1[0])));
                StringBuilder sb = new StringBuilder();
                sb.append(urlElements.size()).append("\r\n");
                sb.append(urlElements.get(0)[1]).append(" ").append(urlElements.get(0)[0]);
                for (int i = 1; i < urlElements.size(); i++) {
                    sb.append("\r\n").append(urlElements.get(i)[1]).append(" ").append(urlElements.get(i)[0]);
                }
                return Collections.singletonList(encodePair(pair._1(), sb.toString()));
            });
            indexRDD1.delete();
            indexJoin(context, indexRDD2);
            //savePersistentTable(context, indexRDD2, "index");
            indexRDD2.delete();
            context.output("round " + String.valueOf(iteri) + " finished\n");
        }

        FlamePairRDD indexRDD = new FlamePairRDDImpl("index", context.getKVS(), (FlameContextImpl) context);
        int finalPageNum = context.getKVS().count("urlpages");
        context.getKVS().persist("index2");
        indexRDD.flatMap(pair -> {
            pair = decodePair(pair);
            String[] pairs = pair._2().split("\r\n", 2);
            String newValue = String.valueOf(Math.log((double) finalPageNum / Integer.parseInt(pairs[0]))) + "\r\n" + pairs[1];
            context.getKVS().put("index2", pair._1(), "value", newValue);
            return null;
        });
        //context.getKVS().tableGC("index", "indexGC");
        context.getKVS().delete("index");
        context.getKVS().rename("index2", "index");
    }
}
