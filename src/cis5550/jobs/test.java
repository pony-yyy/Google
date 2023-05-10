package cis5550.jobs;

import cis5550.kvs.Row;
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
    public static void main(String[] args) throws Exception {
        KVSClient kvs = new KVSClient("100.24.160.86:8000");
        System.out.println(kvs.getRow("index", "nnnnn"));

//        URL var1 = new URL("http://127.0.0.1:8001/data/crawl");
//        HttpURLConnection var2 = (HttpURLConnection)var1.openConnection();
//        var2.setRequestMethod("GET");
//        var2.connect();
//        var in = var2.getInputStream();
//        Row r;
//        int count = 0;
//        try{
//            do {
//                count++;
//                r = Row.readFrom(in);
//
//            } while (r != null);
//        }
//        catch (Exception e){
//            System.out.println(count);
//        }
//        System.out.println(count);


        //open file and load into string
//        String a = new String(Files.readAllBytes(Paths.get("html")));
//
//        String page = a.replaceAll("(<!DOCTYPE[^>]*>|<!--[\\s\\S]*?-->)", " ").toLowerCase(); //first filter comments
//        String regex = "<(/?)([a-zA-Z0-9]+)[^>]*>";
//        String[] filterTag = new String[]{"head", "script", "style"}; //todo add more tags that the content need to be ignored
//        int pageIndex = 0;
//        StringBuilder sb = new StringBuilder();
//        Pattern tagPattern = Pattern.compile(regex);
//        Matcher tagMatcher = tagPattern.matcher(page);
//        while (tagMatcher.find()) {
//            System.out.println(tagMatcher.group());
//            sb.append(page, pageIndex, tagMatcher.start()).append(" ");
//            pageIndex = tagMatcher.end();
//            if (tagMatcher.group(1).isEmpty()) {
//                boolean flag = false;
//                String ctag = "";
//                for (String ft : filterTag) {
//                    if (tagMatcher.group(2).equals(ft)) {
//                        flag = true;
//                        ctag = ft;
//                        break;
//                    }
//                }
//                if (flag) {
//                    int is = 0;
//                    while (tagMatcher.find() && !(tagMatcher.group(1).equals("/") && tagMatcher.group(2).equals(ctag))) {
//                        System.out.println(tagMatcher.group());
//                    }
//                    try {
//                        pageIndex = tagMatcher.end();
//                    } catch (Exception e) {
//                        pageIndex = page.length();
//                        break;
//                    }
//                }
//            }
//        }
//        sb.append(page.substring(pageIndex));
//
//        //seperate words
//        StringBuilder result = new StringBuilder();
//        Pattern pattern = Pattern.compile("\\b\\w+\\b");
//        Matcher matcher = pattern.matcher(sb.toString());
//        while (matcher.find()) {
//            if (!result.isEmpty()) {
//                result.append(",");
//            }
//            result.append(matcher.group());
//        }
//        System.out.println(result.toString());
    }
}
