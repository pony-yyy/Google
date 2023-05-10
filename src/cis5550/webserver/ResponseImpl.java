package cis5550.webserver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ResponseImpl implements Response {
    private Socket client;
    private Map<String, String> header = new HashMap<>();
    private int statusCode = 200;
    private String reasonPhase = "OK";
    public String protocol;
    public String contentType = "text/html";
    public String server;
    public int contentLength;
    private byte[] body = new byte[0];
    public boolean headerOnly = false;
    private static final String responseFormat = "%s %d %s\r\nContent-Type: %s\r\nServer: %s\r\n";
    private boolean hasWrite = false;

    ResponseImpl(Socket c){
        client = c;
    }

    public boolean getHasWrite(){
        return hasWrite;
    }
    public void sendWithError(int code) throws IOException{
        statusCode = code;
        switch (statusCode) {
            case 400 -> reasonPhase = "Bad Request";
            case 403 -> reasonPhase = "Forbidden";
            case 404 -> reasonPhase = "Not Found";
            case 405 -> reasonPhase = "Not Allowed";
            case 500 -> reasonPhase = "Internal Server Error";
            case 501 -> reasonPhase = "Not Implemented";
            case 505 -> reasonPhase = "HTTP Version Not Supported";
            default -> reasonPhase = "";
        }
        contentType = "text/plain";
        String msg = String.format("%d %s", statusCode, reasonPhase);
        body = msg.getBytes();
        if (!hasWrite){
            send();
        }
    }

    public int getStatusCode(){
        return statusCode;
    }

    public byte[] toBytes(){
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        try{
            StringBuilder headText = new StringBuilder(String.format(responseFormat, protocol, statusCode, reasonPhase, contentType, server));
            if (body != null){  //deal with write()
                contentLength = body.length;
                headText.append(String.format("Content-Length: %d\r\n", contentLength));
            }
            for (var line : header.entrySet()){
                headText.append(String.format("%s: %s\r\n", line.getKey(), line.getValue()));
            }
            headText.append("\r\n");
            result.write(headText.toString().getBytes());
            if (!headerOnly && body != null){
                result.write(body);
            }
            return result.toByteArray();
        }
        catch (Exception e){
            return result.toByteArray();
        }
    }

    private void send() throws IOException{
        byte[] data = toBytes();
        client.getOutputStream().write(data);
        client.getOutputStream().flush();
    }
    public void sendResponse() throws IOException {
        if (!hasWrite){
            send();
        }
    }

    //Interface Response function
    public void body(String body){
        this.body = body.getBytes();
    }
    public void bodyAsBytes(byte[] bodyArg){
        this.body = bodyArg;
    }
    public void header(String name, String value){
        if (!hasWrite){
            String lowName = name.toLowerCase();
            if (lowName.equals("content-type")){
                contentType = value;
            }
            else{
                header.put(lowName, value);
            }
        }
    }
    public void type(String contentType){
        this.contentType = contentType;
    }
    public void status(int statusCode, String reasonPhrase){
        if (!hasWrite){
            this.statusCode = statusCode;
            this.reasonPhase = reasonPhrase;
        }
    }
    public void write(byte[] b) throws Exception{
        if (!hasWrite){
            hasWrite = true;
            body = null;
            header.put("Connection", "close");
            send();
        }
        if (!headerOnly){
            client.getOutputStream().write(b);
            client.getOutputStream().flush();
        }
    }

    // EXTRA CREDIT ONLY - please see the handout for details. If you are not doing the extra
    // credit, please implement this with a dummy method that does nothing.
    public void redirect(String url, int responseCode){

    }

    // EXTRA CREDIT ONLY - please see the handout for details. If you are not doing the extra
    // credit, please implement this with a dummy method that does nothing.
    public void halt(int statusCode, String reasonPhrase){

    }
}

