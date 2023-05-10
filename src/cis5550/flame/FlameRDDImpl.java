package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;


import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class FlameRDDImpl implements FlameRDD{
    private String tableName;
    private final KVSClient kvsClient;
    private final FlameContextImpl context;

    FlameRDDImpl(String tableName, KVSClient kvsClient , FlameContextImpl context) {
        this.tableName = tableName;
        this.kvsClient = kvsClient;
        this.context = context;
    }

    @Override
    public int count() throws Exception {
        return kvsClient.count(tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        kvsClient.rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    @Override
    public FlameRDD distinct() throws Exception {
        String newTableName = context.invokeOperation("/rdd/distinct", null, tableName, null);
        return new FlameRDDImpl(newTableName, kvsClient, context);
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Iterator<Row> iter = kvsClient.scan(tableName);
        Vector<String> strings = new Vector<>();
        for (int i = 0; i < num && iter.hasNext(); i++) {
            strings.add(iter.next().get("value"));
        }
        return strings;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        return context.invokeOperationFold("/rdd/fold", lambda, tableName, "zeroElement=" + zeroElement);
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> list = new ArrayList<>();
        for (Iterator<Row> it = kvsClient.scan(tableName); it.hasNext(); ) {
            Row r = it.next();
            list.add(r.get("value"));
        }
        return list;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String newTableName = context.invokeOperation("/rdd/flatMap", lambda, tableName, null);
        return new FlameRDDImpl(newTableName, kvsClient, context);
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String newTableName = context.invokeOperation("/rdd/flatMapToPair", lambda, tableName, null);
        return new FlamePairRDDImpl(newTableName, kvsClient, context);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String newTableName = context.invokeOperation("/rdd/mapToPair", lambda, tableName, null);
        return new FlamePairRDDImpl(newTableName, kvsClient, context);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        return null;
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        return null;
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        return null;
    }

    @Override
    public void delete() throws Exception {
        kvsClient.delete(tableName);
    }
}
