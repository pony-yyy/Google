package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;


public class FlameContextImpl implements FlameContext, Serializable {
    public String outputString = "";
    private final String id;
    private int sequence = 0;
    private Vector<String> workers;
    private String jarPath;
    public FlameContextImpl(Vector<String> workers, String jarPath, int id) {
        this.id = String.valueOf(id);
        this.workers = workers;
        this.jarPath = jarPath;
    }

    @Override
    public KVSClient getKVS() {
        return Master.kvs;
    }

    @Override
    public void output(String s) {
        outputString += s;
    }

    private String generateTableName(){
        sequence++;
        return id + "," + sequence;
    }
    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        KVSClient kvsClient = getKVS();
        String tableName = generateTableName();
        int n = 1;
        for (String s : list) {
            kvsClient.put(tableName, Hasher.hash(String.valueOf(n)), "value", s);
            n++;
        }
        return new FlameRDDImpl(tableName, kvsClient, this);
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        KVSClient kvsClient = getKVS();

        String name = invokeOperation("/fromTable", lambda, tableName, null);
        return new FlameRDDImpl(name, kvsClient, this);
    }

    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {

    }

    public String invokeOperation(String operation, Object lambdaObject, String tableName, String additionalParam) throws Exception{
        KVSClient kvsClient = getKVS();

        byte[] lambda = Serializer.objectToByteArray(lambdaObject);
        Partitioner partitioner = new Partitioner();
        int n = kvsClient.numWorkers();
        if (n < 1){
            throw new Exception("No worker available");
        }
        for (int i = 0; i < n-1; i++){
            partitioner.addKVSWorker(kvsClient.getWorkerAddress(i), kvsClient.getWorkerID(i), kvsClient.getWorkerID(i + 1));
        }
        partitioner.addKVSWorker(kvsClient.getWorkerAddress(n-1), kvsClient.getWorkerID(n-1), null);
        partitioner.addKVSWorker(kvsClient.getWorkerAddress(n-1), null, kvsClient.getWorkerID(0));
        for (String worker : workers){
            partitioner.addFlameWorker(worker);
        }
        Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();

        Thread threads[] = new Thread[partitions.size()];
        HTTP.Response results[] = new HTTP.Response[partitions.size()];
        int count = 0;
        String newTableName = generateTableName();
        for (var partition : partitions){
            StringBuilder sb = new StringBuilder();
            sb.append("http://"+partition.assignedFlameWorker + operation);
            sb.append("?inputTable=" + tableName);
            sb.append("&outputTable=" + newTableName);
            sb.append("&kvsMaster=" + kvsClient.getMaster());
            if (partition.fromKey != null){
                sb.append("&keyStart=" + partition.fromKey);
            }
            if (partition.toKeyExclusive != null){
                sb.append("&keyEnd=" + partition.toKeyExclusive);
            }
            if (additionalParam != null){
                sb.append("&" + additionalParam);
            }
            final String url = sb.toString();
            final int j = count;
            threads[count] = new Thread("Run #"+(count+1)) {
                public void run() {
                    try {
                        results[j] = HTTP.doRequest("POST", url, lambda);
                    }catch (Exception e){
                        results[j] = null;
                        e.printStackTrace();
                    }
                }
            };
            threads[count].start();
            count++;
        }
        for (Thread thread : threads) {
            thread.join();
        }
        for (HTTP.Response result : results) {
            if (result == null || result.statusCode() != 200) {
                throw new Exception("Failed to invoke operation"); //todo
            }
        }
        return newTableName;
    }

    public String invokeOperationFold(String operation, FlamePairRDD.TwoStringsToString lambda, String tableName, String additionalParam) throws Exception{
        KVSClient kvsClient = getKVS();

        Partitioner partitioner = new Partitioner();
        int n = kvsClient.numWorkers();
        if (n < 1){
            throw new Exception("No worker available");
        }
        for (int i = 0; i < n-1; i++){
            partitioner.addKVSWorker(kvsClient.getWorkerAddress(i), kvsClient.getWorkerID(i), kvsClient.getWorkerID(i + 1));
        }
        partitioner.addKVSWorker(kvsClient.getWorkerAddress(n-1), kvsClient.getWorkerID(n-1), null);
        partitioner.addKVSWorker(kvsClient.getWorkerAddress(n-1), null, kvsClient.getWorkerID(0));
        for (String worker : workers){
            partitioner.addFlameWorker(worker);
        }
        Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();

        Thread threads[] = new Thread[partitions.size()];
        HTTP.Response results[] = new HTTP.Response[partitions.size()];
        int count = 0;
        for (var partition : partitions){
            StringBuilder sb = new StringBuilder();
            sb.append("http://"+partition.assignedFlameWorker + operation);
            sb.append("?inputTable=" + tableName);
            sb.append("&kvsMaster=" + kvsClient.getMaster());
            if (partition.fromKey != null){
                sb.append("&keyStart=" + partition.fromKey);
            }
            if (partition.toKeyExclusive != null){
                sb.append("&keyEnd=" + partition.toKeyExclusive);
            }
            if (additionalParam != null){
                sb.append("&" + additionalParam);
            }
            final String url = sb.toString();
            final int j = count;
            threads[count] = new Thread("Run #"+(count+1)) {
                public void run() {
                    try {
                        results[j] = HTTP.doRequest("POST", url, Serializer.objectToByteArray(lambda));
                    }catch (Exception e){
                        results[j] = null;
                        e.printStackTrace();
                    }
                }
            };
            threads[count].start();
            count++;
        }
        for (Thread thread : threads) {
            thread.join();
        }
        String accumulator = null;
        for (HTTP.Response result : results) {
            if (result == null || result.statusCode() != 200) {
                throw new Exception("Failed to invoke operation"); //todo
            }
            if (accumulator == null) {
                accumulator = new String(result.body());
            } else {
                accumulator = lambda.op(accumulator, new String(result.body()));
            }
        }
        return accumulator;
    }
}
