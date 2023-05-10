package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlamePairRDDImpl implements FlamePairRDD {
    private String tableName;
    private KVSClient kvsClient;
    private FlameContextImpl context;
    public FlamePairRDDImpl(String tableName, KVSClient kvsClient , FlameContextImpl context) {
        this.tableName = tableName;
        this.kvsClient = kvsClient;
        this.context = context;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> list = new ArrayList<>();
        for (Iterator<Row> it = kvsClient.scan(tableName); it.hasNext(); ) {
            Row r = it.next();
            for (String column : r.columns()){
                list.add(new FlamePair(r.key(), r.get(column)));
            }
        }
        return list;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String newTableName = context.invokeOperation("/pairRdd/foldByKey", lambda, tableName, "zeroElement=" + zeroElement);
        return new FlamePairRDDImpl(newTableName, kvsClient, context);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        kvsClient.rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String newTableName = context.invokeOperation("/pairRdd/flatMap", lambda, tableName, null);
        return new FlameRDDImpl(newTableName, kvsClient, context);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String newTableName = context.invokeOperation("/pairRdd/flatMapToPair", lambda, tableName, null);
        return new FlamePairRDDImpl(newTableName, kvsClient, context);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        String newTableName = context.invokeOperation("/pairRdd/join", null, tableName, "otherTable=" + ((FlamePairRDDImpl)other).tableName);
        return new FlamePairRDDImpl(newTableName, kvsClient, context);
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        return null;
    }

    @Override
    public void delete() throws Exception{
        kvsClient.delete(tableName);
    }

}
