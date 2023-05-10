package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank {

    public static FlamePair encodePair(String a, String b){
        return new FlamePair(Hasher.hash(a).substring(0, 5)+ ":" + a, b);
    }
    public static FlamePair decodePair(FlamePair pair){
        return new FlamePair(pair._1().substring(6), pair._2());
    }

    public static String normalizeURL(String url, String base) throws Exception {
        url = url.trim().split("\\s")[0].replace(",", "%2C");
        if (url.getBytes().length >= 1024){     //todo filter too long string
            return null;
        }

        URL baseURL = new URL(base);
        String protocal = baseURL.getProtocol();
        String host = baseURL.getHost();
        int port = baseURL.getPort() >= 0 ? baseURL.getPort() : baseURL.getDefaultPort();
        String[] routerParts = new String[0];
        if (!baseURL.getPath().isEmpty()){
            routerParts = baseURL.getPath().split("/", -1);
            routerParts = Arrays.copyOfRange(routerParts, 1, routerParts.length);
        }

        if (url.equals("#") || url.contains("..")){     //todo ..
            return null;
        }
        //todo ? queryparm
        url = url.split("#")[0];
        if (url.isEmpty()){
            return null;
        }
        if (url.startsWith("/")){
            return protocal + "://" + host + ":" + port + url;
        }
        else if (url.startsWith("http://") || url.startsWith("https://")){
            URL testURL = new URL(url);
            String path = testURL.getPath();
            if (path.isEmpty()){
                path = "/";
            }
            if (testURL.getPort() < 0){
                return testURL.getProtocol() + "://" + testURL.getHost() + ":" + testURL.getDefaultPort() + path;
            }
            else{
                return testURL.getProtocol() + "://" + testURL.getHost() + ":" + testURL.getPort() + path;
            }
        }
        else{
            int routerLength = routerParts.length == 0 ? 0 : routerParts.length - 1;
            while (url.startsWith("../")){
                url = url.substring(3);
                routerLength--;
                if (routerLength < 0){
                    return null;
                }
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < routerLength; i++){
                sb.append(routerParts[i]);
                sb.append("/");
            }
            return protocal + "://" + host + ":" + port + "/" + sb.toString() + url;
        }
    }

    public static String normalizeURL(String url) throws Exception {
        url = url.split("\\?")[0];  //todo remove the query params
        url = url.split("#")[0];
        URL testURL = new URL(url);
        String path = testURL.getPath();
        if (path.isEmpty()){
            path = "/";
        }
        if (testURL.getPort() < 0){
            return testURL.getProtocol() + "://" + testURL.getHost() + ":" + testURL.getDefaultPort() + path;
        }
        else{
            return testURL.getProtocol() + "://" + testURL.getHost() + ":" + testURL.getPort() + path;
        }
    }

    public static boolean filterURL(String url){
        try{
            URL testURL = new URL(url);
        }
        catch (Exception e){
            return false;
        }
        if (url.endsWith(".jpg") || url.endsWith(".jpeg") || url.endsWith(".gif") || url.endsWith(".png") || url.endsWith(".txt")){
            return false;
        }
        else{
            return true;
        }
    }


    public static void run(FlameContext context, String[] strings) throws Exception {
        double threshod = Double.parseDouble(strings[0]);
        double percentage = 1;
        if (strings.length == 2){
            percentage = Double.parseDouble(strings[1]) / 100.0;
        }

        FlameRDD tmpRDD = context.fromTable("crawl", row -> {
            if (row.get("url").contains("..") || row.get("page") == null || row.get("page").isEmpty()){
                return null;
            }	//todo ..bug
            return row.get("url").trim().split("\\s")[0].replace(",", "%2C") + "\r\n" + row.get("page");    //the last character of key can't be space
        });
        FlamePairRDD pagesRDD = tmpRDD.mapToPair(s -> {
            String[] splits = s.split("\\r\\n", 2);
            return encodePair(splits[0], splits[1]);
        });
        tmpRDD.delete();
        pagesRDD.saveAsTable("pages");
        FlamePairRDD pageRank = pagesRDD.flatMapToPair(pair -> {
            pair = decodePair(pair);
            String page = pair._2();
            String pattern = "<a[^>]*?href=\"([^\"]*)\"[^>]*>";
            Pattern regex = Pattern.compile(pattern);
            Matcher matcher = regex.matcher(page);
            StringBuilder links = new StringBuilder();
            links.append("1.0,1.0");
            Set<String> linkSet = new HashSet<>();
            while (matcher.find()) {
                String link = matcher.group(1);
                String normalizedLink = null;
                try {
                    normalizedLink = normalizeURL(link, pair._1());
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                if (normalizedLink != null && !normalizedLink.equals(pair._1()) && !linkSet.contains(normalizedLink)){
                    linkSet.add(normalizedLink);
                    if (context.getKVS().getRow("pages", encodePair(normalizedLink, " ")._1()) != null){
                        links.append(",").append(normalizedLink);
                    }
                }
            }
            links.append(",").append(linkSet.size());
            return Collections.singletonList(encodePair(pair._1(), links.toString()));
        });
        pagesRDD.delete();
        int iterNum = 0;
        while (true){
            iterNum++;
            FlamePairRDD transfer = pageRank.flatMapToPair(pair -> {
                pair = decodePair(pair);
                String[] elements = pair._2().split(",");
                double rc = Double.parseDouble(elements[0]);
                double rp = Double.parseDouble(elements[1]);
                List<FlamePair> results = new ArrayList<>();

                int n = Integer.parseInt(elements[elements.length - 1]);
                results.add(encodePair(pair._1(), "0.0"));
                for (int i = 2; i < elements.length - 1; i++){
                    results.add(encodePair(elements[i], String.valueOf(0.85 * rc / n)));
                }
                return results;
            });
            FlamePairRDD aggregate = transfer.foldByKey("0", (s, s1) -> {
                return String.valueOf(Double.parseDouble(s) + Double.parseDouble(s1));
            });
            transfer.delete();
            FlamePairRDD join = pageRank.join(aggregate);
            aggregate.delete();
            FlamePairRDD pageRank2 = join.flatMapToPair(pair -> {
                pair = decodePair(pair);
                String[] elements = pair._2().split(",");
                double rc = Double.parseDouble(elements[0]);
                double rp = Double.parseDouble(elements[1]);
                double t  = Double.parseDouble(elements[elements.length - 1]);
                rp = rc;
                rc = 0.15 + t;
                StringBuilder sb = new StringBuilder();
                sb.append(String.valueOf(rc) + "," + String.valueOf(rp));
                for (int i = 2; i < elements.length - 1; i++){
                    sb.append(","+elements[i]);
                }
                return Collections.singletonList(encodePair(pair._1(), sb.toString()));
            });
            pageRank.delete();
            pageRank = pageRank2;
            join.delete();

            FlameRDD diff = pageRank.flatMap(pair -> {
                pair = decodePair(pair);
                String[] elements = pair._2().split(",");
                int b;
                if (Math.abs(Double.parseDouble(elements[0]) - Double.parseDouble(elements[1])) < threshod){
                    b = 1;
                }
                else{
                    b = 0;
                }
                return Collections.singleton(String.valueOf(b));
            });
            int addDiff = Integer.parseInt(diff.fold("0", (s, s1) -> {
                return String.valueOf(Integer.parseInt(s) + Integer.parseInt(s1));
            }));
            if (addDiff >= diff.count() * percentage){
                break;
            }
            diff.delete();
        }
        context.output(String.valueOf(iterNum));
        context.getKVS().persist("pageranks");
        pageRank.flatMapToPair(pair -> {
            pair = decodePair(pair);
            String[] elements = pair._2().split(",");
            double rc = Double.parseDouble(elements[0]);
            context.getKVS().put("pageranks", pair._1(), "rank", String.valueOf(rc));
            return Collections.emptyList();
        });
        context.getKVS().clean();
    }
}

