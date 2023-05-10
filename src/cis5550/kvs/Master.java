package cis5550.kvs;

import cis5550.webserver.Server;

public class Master extends cis5550.generic.Master{
    public static void main(String[] args){
        int port = 8000;
        if (args.length > 0){
            try{
                port = Integer.parseInt(args[0]);
            }
            catch (Exception e){
                System.out.println("Invalid port number");
                return;
            }
        }
        else{
            System.out.println("No port specified, using default port 8000");
        }
        Server.port(port);
        registerRoutes();
        Server.get("/", (req, res) -> {return "<html><head></head><body><h1>KVS Master</h1>" + workerTable() + "</body></html>";});
        startCleanThread();
    }
}
