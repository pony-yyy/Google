package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;
import cis5550.webserver.Response;

class Worker extends cis5550.generic.Worker {
    public static File myJAR;

    @FunctionalInterface
    interface RowOp {
        void apply(Row row, KVSClient kvs, Request request, Object lambda) throws Exception;
    }

    private static String invokeOperation(Request request, Response response, RowOp rowOp) throws Exception {
        String inputTable = request.queryParams("inputTable");
        String outputTable = request.queryParams("outputTable");
        String kvsMaster = request.queryParams("kvsMaster");
        String keyStart = request.queryParams("keyStart");
        String keyEnd = request.queryParams("keyEnd");
        Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        KVSClient kvs = Master.kvs = new KVSClient(kvsMaster);
        //kvs.persist(outputTable);   //todo
        for (Iterator<Row> it = kvs.scan(inputTable, keyStart, keyEnd); it.hasNext();){
            Row r = it.next();
            if (r != null) {
                rowOp.apply(r, kvs, request, lambda);
            }
        }
        return "OK";
    }

	public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <masterIP:port>");
            System.exit(1);
        }
        port = Integer.parseInt(args[0]);
        masterIP = args[1];
        myJAR = new File("__worker" + port + "-current.jar");
        id = "";
        for (int i = 0; i < 5; i++) {
            id += (char) ('a' + (int) (Math.random() * 26));
        }

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        post("/rdd/flatMap", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                int count = 0;
                Iterable<String> results = ((FlameRDD.StringToIterable) lambda).op(row.get("value"));
                if (results != null) {
                    for (String result : results) {
                        kvs.put(req.queryParams("outputTable"), Hasher.hash(row.key() + count), "value", result);
                        count++;
                    }
                }
            });
        });
        post("/rdd/mapToPair", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                FlamePair pair = ((FlameRDD.StringToPair) lambda).op(row.get("value"));
                if (pair != null) {
                    kvs.put(req.queryParams("outputTable"), pair.a, row.key(), pair.b);
                }
            });
        });
        post("/pairRdd/foldByKey", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                String accumulator = req.queryParams("zeroElement");
                for (String column : row.columns()) {
                    String value = row.get(column);
                    accumulator = ((FlamePairRDD.TwoStringsToString) lambda).op(accumulator, value);
                }
                kvs.put(req.queryParams("outputTable"), row.key(), "value", accumulator);
            });
        });
        post("/fromTable", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                String result = ((FlameContext.RowToString) lambda).op(row);
                if (result != null) {
                    kvs.put(req.queryParams("outputTable"), row.key(), "value", result);
                }
            });
        });
        post("/rdd/flatMapToPair", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                int count = 0;
                Iterable<FlamePair> results = ((FlameRDD.StringToPairIterable) lambda).op(row.get("value"));
                if (results != null) {
                    for (FlamePair result : results) {
                        kvs.put(req.queryParams("outputTable"), result.a, Hasher.hash(row.key() + count), result.b);
                        count++;
                    }
                }
            });
        });
        post("/pairRdd/flatMap", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                int count = 0;
                for (String column : row.columns()) {
                    String value = row.get(column);
                    Iterable<String> results = ((FlamePairRDD.PairToStringIterable) lambda).op(new FlamePair(row.key(), value));
                    if (results != null) {
                        for (String result : results) {
                            kvs.put(req.queryParams("outputTable"), Hasher.hash(row.key() + count), "value", result);
                            count++;
                        }
                    }
                }
            });
        });
        post("/pairRdd/flatMapToPair", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                int count = 0;
                for (String column : row.columns()) {
                    String value = row.get(column);
                    Iterable<FlamePair> results = ((FlamePairRDD.PairToPairIterable) lambda).op(new FlamePair(row.key(), value));
                    if (results != null) {
                        for (FlamePair result : results) {
                            kvs.put(req.queryParams("outputTable"), result.a, Hasher.hash(row.key() + count), result.b);
                            count++;
                        }
                    }
                }
            });
        });
        post("/rdd/distinct", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                kvs.put(req.queryParams("outputTable"), row.get("value"), "value", row.get("value"));
            });
        });
        post("/pairRdd/join", (request, response) -> {
            return invokeOperation(request, response, (row, kvs, req, lambda) -> {
                String otherTable = req.queryParams("otherTable");
                Row otherRow = kvs.getRow(otherTable, row.key());
                if (otherRow != null) {
                    for (String column : row.columns()) {
                        String value = row.get(column);
                        for (String otherColumn : otherRow.columns()) {
                            String otherValue = otherRow.get(otherColumn);
                            kvs.put(req.queryParams("outputTable"), row.key(), Hasher.hash(column) + Hasher.hash(otherColumn), value + "," + otherValue); //todo
                        }
                    }
                }
            });
        });
        post("/rdd/fold", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String kvsMaster = request.queryParams("kvsMaster");
            String keyStart = request.queryParams("keyStart");
            String keyEnd = request.queryParams("keyEnd");
            Object lambda = Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = Master.kvs = new KVSClient(kvsMaster);
            String accumulator = request.queryParams("zeroElement");
            for (Iterator<Row> it = kvs.scan(inputTable, keyStart, keyEnd); it.hasNext(); ) {
                Row r = it.next();
                if (r != null) {
                    accumulator = ((FlamePairRDD.TwoStringsToString) lambda).op(accumulator, r.get("value"));
                }
            }
            return accumulator;
        });

        startPingThread();
    }
}
