package cis5550.kvs;

import cis5550.webserver.Server;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Worker extends cis5550.generic.Worker{
    private static final Map<String, Map<String, Row>> tables = new ConcurrentHashMap<>();
    private static final Object tablePutLock = new Object();
    private static final Map<String, RandomAccessFile> tableFiles = new HashMap<>();
    private static final Set<String> persistantTables = new HashSet<>();

    public static void main(String[] args){
        try{
            port = Integer.parseInt(args[0]);
            dir = args[1];
            masterIP = args[2];
        }
        catch (Exception e){
            System.out.println("Wrong arguments");
            return;
        }
        Server.port(port);

        //get id
        try{
            BufferedReader reader = new BufferedReader(new FileReader(dir + "/id"));
            id = reader.readLine();
        }
        catch (Exception e){
            id = "";
            for (int i = 0; i < 5; i++){
                id += (char)('a' + (int)(Math.random() * 26));
            }
            try{
                File file = new File(dir + "/id");
                file.createNewFile();
                FileWriter writer = new FileWriter(file);
                writer.write(id);
                writer.close();
            }
            catch (Exception e2){
                System.out.println("Error in writing id");
            }
        }

        //recovery
        File dirFile = new File(dir);
        File[] files = dirFile.listFiles();
        if (files != null){
            for (File file : files){
                if (file.getName().endsWith(".table")){
                    String tableName = file.getName().substring(0, file.getName().length() - 6);
                    persistantTables.add(tableName);
                    try{
                        RandomAccessFile tableFile = new RandomAccessFile(file, "rw");
                        tableFiles.put(tableName, tableFile);
                        Map<String, Row> table = new ConcurrentHashMap<>();
                        tables.put(tableName, table);
                        while (tableFile.getFilePointer() < tableFile.length()){
                            long pos = tableFile.getFilePointer();
                            Row row = Row.readFrom(tableFile);
                            if (row == null){
                                break;
                            }
                            table.put(row.key, new Row(row.key, pos));
                        }
                    }
                    catch (Exception e){
                        System.out.println("Error in recovery");
                    }
                }
            }
        }

        //register routes
        Server.put("/persist/:T", (req, res) -> {
            String table = req.params("T");
            synchronized (tablePutLock) {
                if (!tables.containsKey(table)) {
                    persistantTables.add(table);
                    createTable(table);
                    res.status(200, "OK");
                    return "OK";
                } else {
                    res.status(403, "Forbidden");
                    return null;
                }
            }
        });
        Server.put("/data/:T/:R/:C", (req, res) -> {
            String table = req.params("T");
            String row = req.params("R");
            String col = req.params("C");
            byte[] value = req.bodyAsBytes();
            synchronized (tablePutLock){
                Row rowObj = getRow(table, row);
                if (rowObj == null){
                    rowObj = new Row(row);
                }
                rowObj.put(col, value);
                putRow(table, row, rowObj);
            }
            return "OK";
        });
        Server.put("/data/:T", (req, res) -> {
            String table = req.params("T");
            byte[] value = req.bodyAsBytes();
            synchronized (tablePutLock){
                ByteArrayInputStream streamValue = new ByteArrayInputStream(value);
                while (streamValue.available() > 0){
                    Row row;
                    try {
                        row = Row.readFrom(streamValue);
                    }
                    catch (Exception e){
                        res.status(400, "Bad Request");
                        return null;
                    }
                    if (row == null){
                        break;
                    }
                    putRow(table, row.key, row);
                }

            }
            return "OK";
        });
        Server.get("/data/:T/:R/:C", (req, res) -> {
            String table = req.params("T");
            String row = req.params("R");
            String col = req.params("C");

            Row rowObj = getRow(table, row);
            if (rowObj == null){
                res.status(404, "Not Found");
            }
            else{
                byte[] value = rowObj.getBytes(col);
                if (value == null){
                    res.status(404, "Not Found");
                }
                else{
                    res.bodyAsBytes(value);
                }
            }
            return null;
        });
        Server.get("/data/:T/:R", (req, res) -> {
            String table = req.params("T");
            String row = req.params("R");

            Row rowObj = getRow(table, row);
            if (rowObj == null){
                res.status(404, "Not Found");
            }
            else{
                res.bodyAsBytes(rowObj.toByteArray());
            }
            return null;
        });
        Server.get("/data/:T", (req, res) -> {
            String table = req.params("T");

            Map<String, Row> tableObj = tables.get(table);
            if (tableObj == null){
                res.status(404, "Not Found");
            }
            else{
                String startRow = req.queryParams("startRow");
                String endRow = req.queryParams("endRowExclusive");
                for (var rowEntry : tableObj.entrySet()){
                    if (startRow != null && rowEntry.getKey().compareTo(startRow) < 0){
                        continue;
                    }
                    if (endRow != null && rowEntry.getKey().compareTo(endRow) >= 0){
                        continue;
                    }
                    res.write(getRow(table, rowEntry.getKey()).toByteArray());
                    res.write("\n".getBytes());
                }
                res.write("\n".getBytes());
            }
            return null;
        });
        Server.put("/rename/:T", (req, res) -> {
            String table = req.params("T");
            String newName = req.body();
            if (newName == null){
                res.status(400, "Bad Request");
                return null;
            }
            synchronized (tablePutLock){
                if (!tables.containsKey(table)){
                    res.status(404, "Not Found");
                    return null;
                }
                if (tables.containsKey(newName)){
                    res.status(409, "Conflict");
                    return null;
                }
                tables.put(newName, tables.get(table));
                tables.remove(table);
                if (persistantTables.contains(table)) {
                    persistantTables.remove(table);
                    persistantTables.add(newName);
                    tableFiles.get(table).close();
                    tableFiles.remove(table);
                    try {
                        File file = new File(dir + "/" + table + ".table");
                        if (!file.renameTo(new File(dir + "/" + newName + ".table"))) {
                            System.out.println("Error in rename");
                            System.exit(1);
                        }
                    } catch (Exception e) {
                        System.out.println("Error in rename");
                        System.exit(1);
                    }
                    tableFiles.put(newName, new RandomAccessFile(dir + "/" + newName + ".table", "rw"));
                }
            }
            return "OK";
        });
        Server.put("/delete/:T", (req, res) -> {
            String table = req.params("T");
            synchronized (tablePutLock){
                if (!tables.containsKey(table)){
                    res.status(404, "Not Found");
                    return null;
                }
                tables.remove(table);
                if (persistantTables.contains(table)) {
                    persistantTables.remove(table);
                    tableFiles.get(table).close();
                    tableFiles.remove(table);
                    File file = new File(dir + "/" + table + ".table");
                    if (!file.delete()) {
                        System.out.println("Error in delete");
                        System.exit(1);
                    }
                }
            }
            return "OK";
        });
        Server.put("/clean", (req, res) ->{
            synchronized (tablePutLock){
                for (String t : tables.keySet()){
                    if (!persistantTables.contains(t)){
                        tables.remove(t);
                    }
                }
            }
            return "OK";
        });
        Server.put("/tableGC/:T", (req, res) -> {
            String table = req.params("T");
            String gcTable = req.body();
            synchronized (tablePutLock){
                if (!persistantTables.contains(table)){
                    res.status(404, "Not Found");
                    return null;
                }
                persistantTables.add(gcTable);
                createTable(gcTable);
                for (var entry : tables.get(table).entrySet()){
                    putRow(gcTable, entry.getKey(), getRow(table, entry.getKey()));
                }
                persistantTables.remove(table);
                tables.remove(table);
                tableFiles.get(table).close();
                tableFiles.remove(table);
                File file = new File(dir + "/" + table + ".table");
                if (!file.delete()) {
                    System.out.println("Error in delete");
                    System.exit(1);
                }
            }
            return "OK";
        });
        Server.get("/tables", (req, res) -> {
            StringBuilder sb = new StringBuilder();
            for (String tableName : tables.keySet()){
                sb.append(tableName + "\n");
            }
            res.type("text/plain");
            return sb.toString();
        });
        Server.get("/count/:T", (req, res) -> {
            String table = req.params("T");
            if (!tables.containsKey(table)){
                res.status(404, "Not Found");
                return null;
            }
            return tables.get(table).size();
        });

        //view routes
        Server.get("/", (req, res) -> {
            StringBuilder sb = new StringBuilder();
            sb.append("<html><body><h1>Table List</h1><table border=\"1\"><tr><th>Table Name</th><th>Number of Keys</th><th>If Persistent</th></tr>");
            for (String tableName : tables.keySet()){
                sb.append("<tr><td><a href=\"/view/" + tableName + "\">" + tableName + "</a></td>");
                sb.append("<td>" + tables.get(tableName).size() + "</td>");
                sb.append("<td>" + (persistantTables.contains(tableName) ? "persistant" : "") + "</td></tr>");
            }
            sb.append("</table></body></html>");

            return sb.toString();
        });
        Server.get("/view/:T", (req, res) -> {
            String table = req.params("T");
            String fromRow = req.queryParams("fromRow");
            if (!tables.containsKey(table)){
                res.status(404, "Not Found");
                return null;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("<html><body><h1>Table: " + table + "</h1>");
            int count = 0;
            //sort the key of the table
            List<String> keys = new ArrayList<>(tables.get(table).keySet());
            Collections.sort(keys);
            List<Row> rows = new ArrayList<>();
            Set<String> cols = new HashSet<>();
            String nextRow = null;
            for (String key : keys){
                if (fromRow != null && key.compareTo(fromRow) < 0){
                    continue;
                }
                if (count == 10){
                    nextRow = key;
                    break;
                }
                Row r = getRow(table, key);
                rows.add(r);
                cols.addAll(r.columns());
                count++;
            }
            sb.append("<table border=\"1\"><tr><th>Row</th>");
            List<String> colsList = new ArrayList<>(cols);
            Collections.sort(colsList);
            for (String col : colsList){
                sb.append("<th>" + col + "</th>");
            }
            sb.append("</tr>");
            for (Row r : rows){
                sb.append("<tr><td>" + r.key() + "</td>");
                for (String col : colsList){
                    String colValue = r.get(col);
                    if (colValue == null){
                        sb.append("<td></td>");
                    }
                    else{
                        sb.append("<td>" + colValue + "</td>");
                    }
                }
                sb.append("</tr>");
            }
            sb.append("</table>");

            if (nextRow != null) {
                sb.append("<p><a href=\"/view/" + table + "?fromRow=" + nextRow + "\">Next</a></p>");
            }
            sb.append("</body></html>");

            return sb.toString();
        });


        startPingThread();
    }

    private static Row getRow(String table, String row){
        if (!tables.containsKey(table)){
            return null;
        }
        else if (!tables.get(table).containsKey(row)){
            return null;
        }
        else{
            Row rowObj = tables.get(table).get(row);
            if (rowObj.rowPos != -1){
                RandomAccessFile file = tableFiles.get(table);
                try{
                    synchronized (file){
                        file.seek(rowObj.rowPos);
                        rowObj = Row.readFrom(file);
                    }
                }
                catch (Exception e){
                    System.out.println("Error in reading from file with the table " + table);
                    System.exit(1);
                }
            }
            return rowObj;
        }
    }
    private static void putRow(String table, String row, Row rowObj){   //todo thread safe
        if (!tables.containsKey(table)){
            createTable(table);
        }
        //logging
        //in memory
        if (persistantTables.contains(table)){
            RandomAccessFile file = tableFiles.get(table);
            long rowPos = 0;
            try {
                synchronized (file){
                    rowPos = file.length();
                    file.seek(rowPos);
                    file.write(rowObj.toByteArray());
                    file.write("\n".getBytes());
                }
            }
            catch (Exception e){
                System.out.println("Error in writing into file with the table " + table);
                System.exit(1);
            }
            tables.get(table).put(row, new Row(row, rowPos));
        }
        else{
            tables.get(table).put(row, rowObj);
        }
    }

    private static void createTable(String table){
        tables.put(table, new ConcurrentHashMap<>());
        try{
            if (persistantTables.contains(table)){
                RandomAccessFile tableFile = new RandomAccessFile(dir + "/" + table + ".table", "rw");
                tableFiles.put(table, tableFile);
            }
        }
        catch (Exception e){
            System.out.println("Error in creating file with the table " + table);
            System.exit(1);
        }
    }
}
