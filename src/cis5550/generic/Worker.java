package cis5550.generic;

import cis5550.tools.HTTP;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.Timer;

public class Worker {
    private static class PingTask extends TimerTask {
        @Override
        public void run() {
            String urlString = "http://" + masterIP + "/ping?id=" + id + "&port=" + port;
            try{
                HTTP.doRequest("GET", urlString, null);
            }
            catch (Exception e){
                System.out.println("Error in ping. The server may be closed.");
                System.exit(1);
            }
        }
    }
    public static String id;
    public static int port;
    public static String dir;
    public static String masterIP;
    public static void startPingThread(){
        Timer timer = new Timer();
        timer.schedule(new PingTask(), 0, 5000);
    }
}
