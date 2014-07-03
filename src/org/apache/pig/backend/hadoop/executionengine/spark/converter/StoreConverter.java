package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.antlr.grammar.v3.ANTLRParser.throwsSpec_return;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.spark.StoreSparkFunc;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.function.Function;

import com.google.common.collect.Lists;

/**
 * Converter that takes a POStore and stores it's content.
 *
 * @author billg
 */
@SuppressWarnings({ "serial"})
public class StoreConverter implements POConverter<Tuple, Tuple2<Text, Tuple>, POStore> {

private static final StoreFuncInterface StoreSparkFunc = null;
//    private static final FromTupleFunction FROM_TUPLE_FUNCTION = new FromTupleFunction();

    private PigContext pigContext;

    public StoreConverter(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    @Override
    public RDD<Tuple2<Text, Tuple>> convert(List<RDD<Tuple>> predecessors, POStore physicalOperator) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        // convert back to KV pairs
//        RDD<Tuple2<Text, Tuple>> rddPairs = rdd.map(FROM_TUPLE_FUNCTION, SparkUtil.<Text, Tuple>getTuple2Manifest());
//        PairRDDFunctions<Text, Tuple> pairRDDFunctions = new PairRDDFunctions<Text, Tuple>(rddPairs,
//                SparkUtil.getManifest(Text.class), SparkUtil.getManifest(Tuple.class),null);

        JobConf storeJobConf = SparkUtil.newJobConf(pigContext);
        POStore poStore = configureStorer(storeJobConf, physicalOperator);
        StoreFuncInterface storeFunc = poStore.getStoreFunc();
        if(storeFunc instanceof StoreSparkFunc)
        {
        	StoreSparkFunc storeSparkFunc = (StoreSparkFunc)storeFunc;
        	storeSparkFunc.putRDD(rdd, poStore.getSFile().getFileName(), storeJobConf);
        }
        else
        {
        	throw new RuntimeException("StoreFunc should be an instance of StoreSparkFunc- Found: "+storeFunc.toString());
        }
//        pairRDDFunctions.saveAsNewAPIHadoopFile(poStore.getSFile().getFileName(),Text.class, Tuple.class, PigOutputFormat.class, storeJobConf);

        return null;
    }

    private static POStore configureStorer(JobConf jobConf,
            PhysicalOperator physicalOperator) throws IOException {
        ArrayList<POStore> storeLocations = Lists.newArrayList();
        POStore poStore = (POStore)physicalOperator;
        storeLocations.add(poStore);
        StoreFuncInterface sFunc = poStore.getStoreFunc();
        sFunc.setStoreLocation(poStore.getSFile().getFileName(), new org.apache.hadoop.mapreduce.Job(jobConf));
        poStore.setInputs(null);
        poStore.setParentPlan(null);

        jobConf.set(JobControlCompiler.PIG_MAP_STORES, ObjectSerializer.serialize(Lists.newArrayList()));
        jobConf.set(JobControlCompiler.PIG_REDUCE_STORES, ObjectSerializer.serialize(storeLocations));
        return poStore;
    }

//    private static class FromTupleFunction extends AbstractFunction1<Tuple, Tuple2<Text, Tuple>>
//            implements Serializable {
//
//        private static Text EMPTY_TEXT = new Text();
//
//        public Tuple2<Text, Tuple> apply(Tuple v1) {
//            return new Tuple2<Text, Tuple>(EMPTY_TEXT, v1);
//        }
//    }
}
