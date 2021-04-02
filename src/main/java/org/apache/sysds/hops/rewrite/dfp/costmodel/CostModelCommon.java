package org.apache.sysds.hops.rewrite.dfp.costmodel;

import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.hops.estim.EstimatorBasicAvg;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;

public class CostModelCommon {

    public static final long defaultWorkerNumber = 6;
    public static final long defaultExecutorCores = 24;

    public static double CpuSpeed = 1.0;
    public static double ShuffleSpeed = 5.0;
    public static double BroadCaseSpeed = 3.0;
    public static double JoinSpeed = 5.0;

    public static int defaultBlockSize = 1000;

    public static boolean useMncEstimator = false;
    public static SparsityEstimator metadataEstimator = new EstimatorBasicAvg();
    public static EstimatorMatrixHistogram mncEstimator = new EstimatorMatrixHistogram();

    public static boolean MMShowCostFlag = false;


    public static long getNumberReducers() {
        return defaultWorkerNumber * defaultExecutorCores;
    }

    public static long reducerNumber(long rows, long cols) {
        if (defaultBlockSize <= 0) return getNumberReducers();
        long nrb = (long) Math.ceil((double) rows / defaultBlockSize);
        long ncb = (long) Math.ceil((double) cols / defaultBlockSize);
        long numReducer = Math.min(nrb * ncb, getNumberReducers());
        numReducer = Math.max(numReducer, 1);
        return numReducer;
    }

    public static long workerNumber(long rows, long cols) {
        if (defaultBlockSize <= 0) return defaultWorkerNumber;
        long nrb = (long) Math.ceil((double) rows / defaultBlockSize);
        long ncb = (long) Math.ceil((double) cols / defaultBlockSize);
        long numReducer = Math.min(nrb * ncb, defaultWorkerNumber);
        numReducer = Math.max(numReducer, 1);
        return numReducer;
    }

    public static double matrixSize(DataCharacteristics dc) {
        return OptimizerUtils.estimateSizeExactSparsity(dc);
    }

    public static double matrixSize(MatrixBlock mb) {
        return OptimizerUtils.estimateSizeExactSparsity(mb.getDataCharacteristics());
    }

    public static double matrixSize(long r, long c, double sp) {
        return OptimizerUtils.estimateSizeExactSparsity(r, c, sp);
    }

    public static long workerNumber(DataCharacteristics dc) {
        return Math.min(SparkUtils.getNumPreferredPartitions(dc), defaultWorkerNumber);
    }

    public static long rowBlocks(long r) {
        return (long) Math.ceil(r / (double) defaultBlockSize);
    }

    public static long rowBlocks(DataCharacteristics dc) {
        return (long) Math.ceil(dc.getRows() / (double) defaultBlockSize);
    }

    public static long colBlocks(long c) {
        return (long) Math.ceil(c / (double) defaultBlockSize);
    }

    public static long colBlocks(DataCharacteristics dc) {
        return (long) Math.ceil(dc.getCols() / (double) defaultBlockSize);
    }

    public static long matrixBlocks(long r, long c) {
        return (long) (Math.ceil(c / (double) defaultBlockSize) * Math.ceil(r / (double) defaultBlockSize));
    }

    public static long matrixBlocks(DataCharacteristics dc) {
        return (long) (Math.ceil(dc.getCols() / (double) defaultBlockSize) * Math.ceil(dc.getRows() / (double) defaultBlockSize));
    }

    public static double box(double n, double m) {
        return n * (1.0 - Math.pow((n - 1.0) / n, m));
    }

}
