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

    public static AggBinaryOp.SparkAggType getSparkMMAggregationType(DataCharacteristics dc) {
        if (dc.dimsKnown() && dc.getRows() <= 1000 && dc.getCols() <= 1000)
            return AggBinaryOp.SparkAggType.SINGLE_BLOCK;
        else
            return AggBinaryOp.SparkAggType.MULTI_BLOCK;
    }

    public static boolean requiresAggregation(boolean isLeft,
                                              DataCharacteristics dc1,
                                              DataCharacteristics dc2) {
        //worst-case assumption (for plan correctness)
        boolean ret = true;
        //right side cached (no agg if left has just one column block)
        if (!isLeft && dc1.getCols() >= 0 //known num columns
                && dc1.getCols() <= 1000) {
            ret = false;
        }
        //left side cached (no agg if right has just one row block)
        if (isLeft && dc2.getRows() >= 0 //known num rows
                && dc2.getRows() <= 1000) {
            ret = false;
        }
        return ret;
    }


}
