package cis5550.webserver;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;
import java.util.Timer;
import java.util.TimerTask;

import cis5550.tools.Logger;

public class Server extends Thread {
    private static Server serverInstance = null;
    private static boolean flag = false;
    private static final Map<Method, List<PathRoute>> mRouteTable = new HashMap<>();
    private static final Logger mLogger = Logger.getLogger(Server.class);
    private static ServerSocket mServer;
    private static ServerSocket mSecureServer;
    private static String mRootPath = null;
    private static int mPort = 80;
    private static int mSecurePort = -1;
    private static BlockingQueue<Socket> clientQueue;
    private static final int NUM_WORKERS = 100;
    static final Map<String, SessionImpl> mSessions = new HashMap<>();


    private static class PathRoute{
        public String[] paths; public Route route;
        PathRoute(String p, Route r){
            paths = p.split("/");
            paths = Arrays.stream(paths).filter(x -> !x.isEmpty()).toArray(String[]::new);
            route = r;
        }
    }
    private enum Method {
        GET, HEAD, POST, PUT
    }
    private static class Header{
        public Method method; public String url; public String protocol;
        public Map<String, String> content = new HashMap<>();
    }

    private static class AcceptThread extends Thread{
        private ServerSocket server;
        AcceptThread(ServerSocket server){
            this.server = server;
        }
        @Override
        public void run(){
            while (true){
                Socket client;
                try {
                    client = this.server.accept();
                    mLogger.info("Incoming connection from" + client.getRemoteSocketAddress());
                    clientQueue.put(client);
                }
                catch (Exception e) {
                    mLogger.error("Failed to accept a connection");
                }
            }
        }
    }

    private static class ClientThread extends Thread{
        @Override
        public void run(){
            while (true){
                Socket client;
                try{
                    client = clientQueue.take();
                }
                catch (Exception e){
                    continue;
                }
                while (true) {
                    try {
                        parseRequest(client);
                    }
                    catch (Exception e) {
                        mLogger.info("Connection from " + client.getRemoteSocketAddress() + " is closed");
                        break;
                    }
                }
                try{
                    client.close();
                }
                catch (Exception e){

                }
            }

        }
    }

    public static class staticFiles{
        public static void location(String s){
            mRootPath = s;
        }
    }

    public static void get(String s, Route r){
        init();
        mRouteTable.get(Method.GET).add(new PathRoute(s, r));
    }

    public static void post(String s, Route r){
        init();
        mRouteTable.get(Method.POST).add(new PathRoute(s, r));
    }

    public static void put(String s, Route r){
        init();
        mRouteTable.get(Method.PUT).add(new PathRoute(s, r));
    }

    public static void port(int p){
        mPort = p;
    }

    public static void securePort(int p){
        mSecurePort = p;
    }

    private static void init(){
        if (serverInstance == null){
            serverInstance = new Server();
        }
        if (!flag){
            mRouteTable.put(Method.PUT, new ArrayList<>());
            mRouteTable.put(Method.GET, new ArrayList<>());
            mRouteTable.put(Method.POST, new ArrayList<>());
            flag = true;
            serverInstance.start();
        }
    }

    //The exception mainly means that the connection is closed when read from input stream.
    private static BufferedReader getHeaderReader(Socket client) throws IOException{
        InputStream in = client.getInputStream();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int matchPtr = 0, numBytesRead = 0;
        while (matchPtr < 4) {
            int b = 0;
            b = in.read();
            if (b>=0){
                numBytesRead++;
            }
            else {
                throw new IOException();
            }
            buffer.write(b);
            if ((((matchPtr==0) || (matchPtr==2)) && (b=='\r')) || (((matchPtr==1) || (matchPtr==3)) && (b=='\n')))
                matchPtr ++;
            else
                matchPtr = 0;
        }
        return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.toByteArray())));
    }

    private static boolean parseHeader(BufferedReader reader, ResponseImpl response, Header header) throws IOException{
        String firstLine = reader.readLine();
        String[] elements = firstLine.split(" ");
        if (elements.length != 3) {
            response.sendWithError(400);
            return false;
        }
        Method method;
        switch (elements[0]){
            case "GET" -> method = Method.GET;
            case "HEAD" -> {method = Method.HEAD; response.headerOnly = true;}
            case "POST" -> method = Method.POST;
            case "PUT" -> method = Method.PUT;
            default -> {response.sendWithError(501); return false;}
        }

        header.method = method;
        header.url = elements[1];
        header.protocol = elements[2];
        if (!header.protocol.equals("HTTP/1.1")){
            response.sendWithError(505);
            return false;
        }

        while (true) {
            String l = reader.readLine();
            if (l.equals(""))
                break;

            String[] p2 = l.split(":", 2);
            if (p2.length == 2) {
                header.content.put(p2[0].toLowerCase().trim(), p2[1].trim());
            } else {
                response.sendWithError(400);
                return false;
            }
        }
        if (header.content.get("host") == null){
            response.sendWithError(400);
            return false;
        }
        return true;
    }

    private static byte[] getBody(Socket client, int contentLength) throws IOException{
        InputStream in = client.getInputStream();
        byte[] body = in.readNBytes(contentLength);
        if (body.length < contentLength){
            throw new IOException();    //todo
        }
        return body;
//        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
//        for (int i = 0; i < contentLength; i++){
//            int b = in.read();
//            if (b<0) {
//                throw new IOException();
//            }
//            buffer.write(b);
//        }
//        return buffer.toByteArray();
    }

    //the exception means the connection should be closed
    private static void parseRequest(Socket client) throws IOException{
        BufferedReader reader = getHeaderReader(client);
        ResponseImpl response = new ResponseImpl(client);
        response.protocol = "HTTP/1.1";
        response.server = "test";
        Header header = new Header();
        //error handling
        if (!parseHeader(reader, response, header)){
            mLogger.info("Response with error code " + response.getStatusCode());
            return;
        }

        // get request body
        byte[] requestBody = null;
        if (header.content.get("content-length") != null){
            try{
                int contentLength = Integer.parseInt(header.content.get("content-length"));
                if (contentLength > 0){
                    requestBody = getBody(client, contentLength);
                }
                else if (contentLength < 0){
                    response.sendWithError(400);
                    mLogger.info("Response with error code " + response.getStatusCode());
                    return;
                }
            }
            catch (Exception e){
                response.sendWithError(400);
                mLogger.info("Response with error code " + response.getStatusCode());
                return;
            }
        }

        mLogger.info("Client request " + header.url);

        //dynamic route
        String[] urlParam = header.url.split("\\?");
        String[] urls = urlParam[0].split("/");
        urls = Arrays.stream(urls).filter(x -> !x.isEmpty()).toArray(String[]::new);
        String qparamString = null;
        if (urlParam.length == 2){
            qparamString = urlParam[1];
        }
        for (PathRoute pr : mRouteTable.get(header.method)){
            boolean match = true;
            Map<String, String> params = new HashMap<>();
            if (pr.paths.length == urls.length){
                for (int i = 0; i < urls.length; i++){
                    if (pr.paths[i].charAt(0) == ':'){
                        params.put(pr.paths[i].substring(1), URLDecoder.decode(urls[i]));
                    }
                    else if (!pr.paths[i].equals(urls[i])){
                        match = false;
                        break;
                    }
                }
            }
            else{
                match = false;
            }

            if (match){
                Map<String, String> qparams = new HashMap<>();
                if (qparamString != null){
                    String[] qparamStrings = qparamString.split("&");
                    for (String qs : qparamStrings){
                        String[] keyValue = qs.split("=");
                        if (keyValue.length == 2){
                            qparams.put(URLDecoder.decode(keyValue[0]), URLDecoder.decode(keyValue[1]));
                        }
                        else if (keyValue.length == 1){
                            qparams.put(URLDecoder.decode(keyValue[0]), "");
                        }
                    }
                }
                if (header.content.containsKey("content-type") && header.content.get("content-type").equals("application/x-www-form-urlencoded") && requestBody != null){
                    String[] qparamStrings = new String(requestBody).split("&");
                    for (String qs : qparamStrings){
                        String[] keyValue = qs.split("=");
                        if (keyValue.length == 2){
                            qparams.put(URLDecoder.decode(keyValue[0]), URLDecoder.decode(keyValue[1]));
                        }
                        else if (keyValue.length == 1){
                            qparams.put(URLDecoder.decode(keyValue[0]), "");
                        }
                    }
                }

                Request request = new RequestImpl(header.method.name(), header.url, header.protocol, header.content, qparams, params, (InetSocketAddress)client.getRemoteSocketAddress(), requestBody, serverInstance);
                response.status(200, "OK");
                Object handleResult;
                try {
                    handleResult = pr.route.handle(request, response);
                }
                catch (Exception e){
                    if (response.getHasWrite()){
                        throw new IOException();
                    }
                    response.sendWithError(500);
                    mLogger.info("Response with error code " + response.getStatusCode());
                    return;
                }
                if (response.getHasWrite()){
                    throw new IOException();
                }
                if (handleResult != null){
                    String handleString = handleResult.toString();
                    response.body(handleString);
                    response.sendResponse();
                }
                else{
                    response.sendResponse();
                }
                return;
            }
        }

        //static file
        if (header.method == Method.POST || header.method == Method.PUT){
            response.sendWithError(405);
            mLogger.info("Response with error code " + response.getStatusCode());
            return;
        }
        if (mRootPath == null){
            response.sendWithError(404);
            mLogger.info("Response with error code " + response.getStatusCode());
            return;
        }
        if (header.url.startsWith("../")){
            response.sendWithError(403);
            mLogger.info("Response with error code " + response.getStatusCode());
            return;
        }
        String filePath = mRootPath + "/" + header.url;
        FileInputStream stream;
        try{
            stream = new FileInputStream(filePath);
        }
        catch (Exception e){
            response.sendWithError(404);
            mLogger.info("Response with error code " + response.getStatusCode());
            return;
        }

        response.status(200, "OK");
        String[] partUrl = header.url.split("\\.");
        if (partUrl.length<2){
            response.contentType = "application/octet-stream";
        } else if (partUrl[partUrl.length - 1].equals("txt")) {
            response.contentType = "text/plain";
        } else if (partUrl[partUrl.length - 1].equals("html")) {
            response.contentType = "text/html";
        } else if (partUrl[partUrl.length - 1].equals("jpg") || partUrl[partUrl.length - 1].equals("jpeg")) {
            response.contentType = "image/jpeg";
        } else {
            response.contentType = "application/octet-stream";
        }
        response.bodyAsBytes(stream.readAllBytes());
        mLogger.info("Send " + filePath + " with type " + response.contentType);
        response.sendResponse();
        return;
    }

    public void run(){
        try {
            mServer = new ServerSocket(mPort);
            mLogger.info(String.format("Open socket at %d", mPort));

            if (mSecurePort > 0){
                String pwd = "secret";
                KeyStore keyStore = KeyStore.getInstance("JKS");
                keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
                keyManagerFactory.init(keyStore, pwd.toCharArray());
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
                ServerSocketFactory factory = sslContext.getServerSocketFactory();
                mSecureServer = factory.createServerSocket(mSecurePort);
                mLogger.info(String.format("Open secure socket at %d", mSecurePort));
            }
        }
        catch (Exception e){
            mLogger.error(String.format("Can't open socket at %d", mPort));
            return;
        }

        clientQueue = new ArrayBlockingQueue<>(10000);
        Thread acceptThread = new AcceptThread(mServer);
        Thread[] clientThreads = new Thread[NUM_WORKERS];
        acceptThread.start();
        if (mSecureServer != null){
            Thread secureAcceptThread = new AcceptThread(mSecureServer);
            secureAcceptThread.start();
        }
        for (int i = 0; i < NUM_WORKERS; i++){
            clientThreads[i] = new ClientThread();
            clientThreads[i].start();
        }

        //clean session
        Timer timer = new Timer();
        TimerTask cleanSessionTask = new TimerTask() {
            @Override
            public void run(){
                for (SessionImpl s : mSessions.values()){
                    if (!s.isValid()){
                        s.invalidate();
                    }
                }
            }
        };
        long delay = 1000;
        long interval = 1000;
        timer.scheduleAtFixedRate(cleanSessionTask, delay, interval);
    }
}

