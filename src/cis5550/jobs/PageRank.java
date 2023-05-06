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
    public static String normalizeURL(String url, String base) throws Exception {
        URL baseURL = new URL(base);
        String protocal = baseURL.getProtocol();
        String host = baseURL.getHost();
        int port = baseURL.getPort() >= 0 ? baseURL.getPort() : baseURL.getDefaultPort();
        String[] routerParts = new String[0];
        if (!baseURL.getPath().isEmpty()){
            routerParts = baseURL.getPath().split("/", -1);
            routerParts = Arrays.copyOfRange(routerParts, 1, routerParts.length);
        }

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

        FlameRDD tmp = context.fromTable("crawl", row -> {
            return row.get("url") + "\r\n" + row.get("page");
        });

        FlamePairRDD pageRank = tmp.mapToPair(s -> {
            String[] splits = s.split("\\r\\n", 2);
            String page = splits[1];
            String pattern = "<a\\s+(\"[^\"]*\"|'[^']*'|[^'\">])*?href=\"([^\"]*)\"(\"[^\"]*\"|'[^']*'|[^'\">])*>";
            Pattern regex = Pattern.compile(pattern);
            Matcher matcher = regex.matcher(page);
            StringBuilder links = new StringBuilder();
            links.append("1.0,1.0");
            Set<String> linkSet = new HashSet<>();
            while (matcher.find()) {
                String link = matcher.group(2);
                String normalizedLink = normalizeURL(link, splits[0]);
                if (normalizedLink != null && !linkSet.contains(normalizedLink)){
                    links.append(",").append(normalizedLink);
                    linkSet.add(normalizedLink);
                }
            }
            return new FlamePair(splits[0], links.toString());
        });
        int iterNum = 0;
        while (true){
            iterNum++;
            FlamePairRDD transfer = pageRank.flatMapToPair(pair -> {
                String[] elements = pair._2().split(",");
                double rc = Double.parseDouble(elements[0]);
                double rp = Double.parseDouble(elements[1]);
                List<FlamePair> results = new ArrayList<>();

                int n = elements.length - 2;
                results.add(new FlamePair(pair._1(), "0.0"));
                for (int i = 2; i < elements.length; i++){
                    results.add(new FlamePair(elements[i], String.valueOf(0.85 * rc / n)));
                }
                return results;
            });
            FlamePairRDD aggregate = transfer.foldByKey("0", (s, s1) -> {
                return String.valueOf(Double.parseDouble(s) + Double.parseDouble(s1));
            });
            FlamePairRDD join = pageRank.join(aggregate);
            pageRank = join.flatMapToPair(pair -> {
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
                return Collections.singletonList(new FlamePair(pair._1(), sb.toString()));
            });

            FlameRDD diff = pageRank.flatMap(pair -> {
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
        }
        context.output(String.valueOf(iterNum));
        pageRank = pageRank.flatMapToPair(pair -> {
            String[] elements = pair._2().split(",");
            double rc = Double.parseDouble(elements[0]);
            context.getKVS().put("pageranks", pair._1(), "rank", String.valueOf(rc));
            return Collections.emptyList();
        });

    }
}

