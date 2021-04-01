package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;

public class PartitionUtil {

    public AggBinaryOp.SparkAggType getSparkMMAggregationType( DataCharacteristics dc )
    {
        if( dc.dimsKnown() && dc.getRows()<=dc.getBlocksize() && dc.getCols()<=dc.getBlocksize() )
            return AggBinaryOp.SparkAggType.SINGLE_BLOCK;
        else
            return AggBinaryOp.SparkAggType.MULTI_BLOCK;
    }




}
