package cis5550.generic;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.webserver.Server;

public class Master {
    private static class WorkerInfo{
        public String ip;
        public int port;
        public long lastPing;
        public WorkerInfo(String ip, int port){
            this.ip = ip;
            this.port = port;
            this.lastPing = System.currentTimeMillis();
        }
    }

    private static final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();

    public static Vector<String> getWorkers(){
        Vector<String> results = new Vector<>();
        for (var entry : workers.entrySet()) {
            WorkerInfo info = entry.getValue();
            if (info.lastPing > System.currentTimeMillis() - 15000){
                results.add(entry.getValue().ip + ":" + entry.getValue().port);
            }
        }
        return results;
    }
    public static String getWorkerString(){
        StringBuilder sb = new StringBuilder();
        int lineNum = 0;
        for (var entry : workers.entrySet()) {
            WorkerInfo info = entry.getValue();
            if (info.lastPing > System.currentTimeMillis() - 15000){
                sb.append(entry.getKey() + "," + entry.getValue().ip + ":" + entry.getValue().port + "\n");
                lineNum++;
            }
        }
        //insert a newline at the beginning
        sb.insert(0, lineNum + "\n");
        return sb.toString();
    }

    public static String workerTable(){
        StringBuilder sb = new StringBuilder();
        sb.append("<table border=\"1\"><tr><th>ID</th><th>IP</th><th>Port</th><th>Link</th></tr>");
        for (var entry : workers.entrySet()) {
            WorkerInfo info = entry.getValue();
            if (info.lastPing > System.currentTimeMillis() - 15000) {
                String ipPort = info.ip + ":" + info.port;
                String link = "<a href=\"http://" + ipPort + "\">" + ipPort + "</a>";
                sb.append("<tr><td>" + entry.getKey() + "</td><td>" + info.ip + "</td><td>" + info.port + "</td><td>" + link + "</td></tr>");
            }
        }
        sb.append("</table>");
        return sb.toString();
    }

    public static void registerRoutes(){
        Server.get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String port = req.queryParams("port");
            if (id == null || port == null){
                res.status(400, "Bad Request");
                return "";
            }
            String ip = req.ip();
            workers.put(id, new WorkerInfo(ip, Integer.parseInt(port)));
            return "OK";
        });
        Server.get("/workers", (req, res) -> getWorkerString());
    }


    private static class CleanWorkerTask extends TimerTask {
        public void run() {
            for (var entry : workers.entrySet()) {
                WorkerInfo info = entry.getValue();
                if (info.lastPing < System.currentTimeMillis() - 15000){
                    workers.remove(entry.getKey());
                }
            }
        }
    }

    //create a method to delete workers that haven't pinged in a while periodically
    public static void startCleanThread(){
        Timer timer = new Timer();
        TimerTask task = new CleanWorkerTask();
        timer.schedule(task, 0, 10000);
    }
}
