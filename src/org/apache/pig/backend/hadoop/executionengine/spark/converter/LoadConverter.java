package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkLauncher;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckerException;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.spark.LoadSparkFunc;

import scala.Function1;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;

import com.google.common.collect.Lists;

/**
 * Converter that loads data via POLoad and converts it to RRD&lt;Tuple>. Abuses the interface a bit
 * in that there is no inoput RRD to convert in this case. Instead input is the source path of the
 * POLoad.
 *
 * @author billg
 */
@SuppressWarnings({ "serial"})
public class LoadConverter implements POConverter<Tuple, Tuple, POLoad> {

	private static final Log LOG = LogFactory.getLog(LoadConverter.class);

    private PigContext pigContext;
    private PhysicalPlan physicalPlan;
    private SparkContext sparkContext;

    public LoadConverter(PigContext pigContext, PhysicalPlan physicalPlan, SparkContext sparkContext) {
        this.pigContext = pigContext;
        this.physicalPlan = physicalPlan;
        this.sparkContext = sparkContext;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessorRdds, POLoad poLoad) throws IOException {
        if (predecessorRdds.size()!=0) {
            throw new RuntimeException("Should not have predecessors for Load. Got : "+predecessorRdds);
        }

        JobConf loadJobConf = SparkUtil.newJobConf(pigContext);
        configureLoader(physicalPlan, poLoad, loadJobConf);
        LoadFunc loadFunc = poLoad.getLoadFunc();
        if(loadFunc instanceof LoadSparkFunc)
        {
        	LOG.info("!!!!!!!!!!  LoadSparkFunc  !!!!!!!!!!!!");
        	LoadSparkFunc loadSparkFunc = (LoadSparkFunc) loadFunc;
        	return loadSparkFunc.getRDDfromContext(sparkContext, poLoad.getLFile().getFileName(), loadJobConf);
        }
        else
        {
        	throw new IOException("LoadFunc should be an instance of LoadSparkFunc");
        }
//        // don't know why but just doing this cast for now
//        RDD<Tuple2<Text, Tuple>> hadoopRDD = sparkContext.newAPIHadoopFile(
//                poLoad.getLFile().getFileName(), PigInputFormat.class,
//                Text.class, Tuple.class, loadJobConf);
//
//        // map to get just RDD<Tuple>
//        return hadoopRDD.map(TO_TUPLE_FUNCTION, SparkUtil.getManifest(Tuple.class));
    }

   

    /**
     * stolen from JobControlCompiler
     * TODO: refactor it to share this
     * @param physicalPlan
     * @param poLoad
     * @param jobConf
     * @return
     * @throws java.io.IOException
     */
    private static JobConf configureLoader(PhysicalPlan physicalPlan,
                                           POLoad poLoad, JobConf jobConf) throws IOException {

        Job job = new Job(jobConf);
        LoadFunc loadFunc = poLoad.getLoadFunc();

        loadFunc.setLocation(poLoad.getLFile().getFileName(), job);

        // stolen from JobControlCompiler
        ArrayList<FileSpec> pigInputs = new ArrayList<FileSpec>();
        //Store the inp filespecs
        pigInputs.add(poLoad.getLFile());

        ArrayList<List<OperatorKey>> inpTargets = Lists.newArrayList();
        ArrayList<String> inpSignatures = Lists.newArrayList();
        ArrayList<Long> inpLimits = Lists.newArrayList();
        //Store the target operators for tuples read
        //from this input
        List<PhysicalOperator> loadSuccessors = physicalPlan.getSuccessors(poLoad);
        List<OperatorKey> loadSuccessorsKeys = Lists.newArrayList();
        if(loadSuccessors!=null){
            for (PhysicalOperator loadSuccessor : loadSuccessors) {
                loadSuccessorsKeys.add(loadSuccessor.getOperatorKey());
            }
        }
        inpTargets.add(loadSuccessorsKeys);
        inpSignatures.add(poLoad.getSignature());
        inpLimits.add(poLoad.getLimit());

        jobConf.set("pig.inputs", ObjectSerializer.serialize(pigInputs));
        jobConf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
        jobConf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatures));
        jobConf.set("pig.inpLimits", ObjectSerializer.serialize(inpLimits));

        return jobConf;
    }
}
