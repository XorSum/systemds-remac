package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import java.util.HashMap;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.getMatrixHistogram;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.setMatrixHistogram;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2.*;

public class HopCostEstimator {
    protected static final Log LOG = LogFactory.getLog(FakeCostEstimator2.class.getName());

    private static double CpuSpeed = 1.0;
    private static double ShuffleSpeed = 5.0;
    private static double BroadCaseSpeed = 3.0;

    private static long defaultBlockSize = 1000;


   // private static SparsityEstimator estimator = new EstimatorBasicAvg();
    private static SparsityEstimator estimator = new EstimatorMatrixHistogram();



    public static double estimate(Hop hop) {
        double ans = 0;

        if (HopRewriteUtils.isMatrixMultiply(hop)) {
            hop.constructLops();
            AggBinaryOp.MMultMethod method = ((AggBinaryOp) hop).getMMultMethod();

            System.out.println(method);
            switch (method) {
                case MAPMM_L:
                    ans = estimateMapMM((AggBinaryOp) hop, method);
                    break;
                case MAPMM_R:
                    ans = estimateMapMM((AggBinaryOp) hop, method);
                    break;
                case CPMM:
                    ans = estimateCPMM((AggBinaryOp) hop);
                    break;
                case RMM:
                    ans = estimateRMM((AggBinaryOp) hop);
                    break;
                case MAPMM_CHAIN:
//                    System.out.println(hop);
//                    System.out.println(hop.getOpString());
                 //   System.exit(0);
                    ans = hop.getDim1() * hop.getDim2();
                    break;
                case TSMM:
                    ans = hop.getDim1()*hop.getDim2()*hop.getDim2();
                    break;
                default:
                    ans = estimateCPMM((AggBinaryOp) hop);
            }
            System.out.println("matrix multply cost = " + ans);
        } else {
            ans = hop.getDim1() * hop.getDim2();
        }
        return ans;
    }

    private static double estimateMapMM(AggBinaryOp hop, AggBinaryOp.MMultMethod method) {
        //  System.out.println("MPMM " + t.input1.getName() + " %*% " + t.input2.getName() + " -> " + t.output.getName());

        DataCharacteristics dc1 = hop.getInput().get(0).getDataCharacteristics();
        DataCharacteristics dc2 = hop.getInput().get(1).getDataCharacteristics();
        DataCharacteristics dc3 = hop.getDataCharacteristics();

        double broadcastCost = Double.MAX_VALUE;
        double computeCost = Double.MAX_VALUE;
        double shuffleCost = Double.MAX_VALUE;
        long r2 = reducerNumber(dc3.getRows(), dc3.getCols());
        if (method == AggBinaryOp.MMultMethod.MAPMM_L) {
            long r1 = Math.min(OptimizerUtils.getNumReducers(false), (long) Math.ceil((double) dc2.getRows() / defaultBlockSize));
            broadcastCost = BroadCaseSpeed * matrixSize(dc1) * Math.ceil(Math.log(r1));
            shuffleCost = ShuffleSpeed * matrixSize(dc3) * r1 / r2;
            computeCost = computeCostSPMM(dc1, dc2, dc3);
        } else if (method == AggBinaryOp.MMultMethod.MAPMM_R) {
            long r1 = Math.min(OptimizerUtils.getNumReducers(false), (long) Math.ceil((double) dc1.getCols() / defaultBlockSize));
            broadcastCost = BroadCaseSpeed * matrixSize(dc2) * Math.ceil(Math.log(r1));
            shuffleCost = ShuffleSpeed * matrixSize(dc3) * r1 / r2;
            computeCost = computeCostSPMM(dc1, dc2, dc3);
        }
        if (MMShowCostFlag) {
            LOG.info("begin<<< ");
            LOG.info("mapmm broadcast cost = " + broadcastCost);
            LOG.info("mapmm compute cost = " + computeCost);
            LOG.info("mapmm shuffle cost = " + shuffleCost);
            LOG.info("  end>>>");
        }
        return computeCost + shuffleCost + broadcastCost;
    }


    static double estimateCPMM(AggBinaryOp hop) {
        DataCharacteristics dc1 = estimator.estim(hop2mmnode.get(hop.getInput().get(0)));
        DataCharacteristics dc2 = estimator.estim(hop2mmnode.get(hop.getInput().get(1)));
        DataCharacteristics dc3 = estimator.estim(hop2mmnode.get(hop));


        long r = Math.min((long) Math.ceil((double) dc2.getRows() / defaultBlockSize), //max used reducers
                OptimizerUtils.getNumReducers(false)); //available reducer

        double shuffleCost1 = ShuffleSpeed * (matrixSize(dc1) + matrixSize(dc2)) / r;
        double shuffleCost2 = ShuffleSpeed * matrixSize(dc3) * r / reducerNumber(dc3.getRows(), dc3.getCols());
        double computeCost = computeCostSPMM(dc1, dc2, dc3);

        if (MMShowCostFlag) {
            LOG.info("begin<<< ");
            LOG.info("cpmm shuffle cost 1 = " + shuffleCost1);
            LOG.info("cpmm shuffle cost 2 = " + shuffleCost2);
            LOG.info("cpmm compute cost = " + computeCost);
            LOG.info("  end>>>");
        }
        return shuffleCost1 + shuffleCost2 + computeCost;

    }

    static double estimateRMM(AggBinaryOp hop) {

        DataCharacteristics dc1 = estimator.estim(hop2mmnode.get(hop.getInput().get(0)));
        DataCharacteristics dc2 = estimator.estim(hop2mmnode.get(hop.getInput().get(1)));
        DataCharacteristics dc3 = estimator.estim(hop2mmnode.get(hop));


        long m1_nrb = (long) Math.ceil((double) dc1.getRows() / defaultBlockSize); // number of row blocks in m1
        long m2_ncb = (long) Math.ceil((double) dc2.getCols() / defaultBlockSize); // number of column blocks in m2
        long k = (long) Math.ceil((double) dc2.getRows() / defaultBlockSize);

        double rmm_nred = Math.min((double) m1_nrb * m2_ncb, //max used reducers
                OptimizerUtils.getNumReducers(false)); //available reducers

        double shuffleCost1 = ShuffleSpeed * (m2_ncb * matrixSize(dc1) + m1_nrb * matrixSize(dc2)) / rmm_nred;
        double shuffleCost2 = ShuffleSpeed * (matrixSize(dc3) * k) / rmm_nred;
        double computeCost = computeCostSPMM(dc1, dc2, dc3);

        if (MMShowCostFlag) {
            LOG.info("begin<<< ");
            LOG.info("rmm shuffle cost 1 = " + shuffleCost1);
            LOG.info("rmm shuffle cost 2 = " + shuffleCost2);
            LOG.info("rmm compute cost = " + computeCost);
            LOG.info("  end>>>");
        }
        return shuffleCost1 + shuffleCost2 + computeCost;
    }

//    public static enum OpCode {
//        MM,
//        MULT, PLUS, EQZERO, NEQZERO,
//        CBIND, RBIND,
//        TRANS, DIAG, RESHAPE;
//    }


    public static HashMap<Hop, MMNode> hop2mmnode = new HashMap<>();

    public static MMNode buildMMNodeTree(Hop hop) {
        if (hop2mmnode.containsKey(hop)) return hop2mmnode.get(hop);
        MMNode node = null;
        // if (mapmm)
        if (Judge.isWrite(hop)) {
            node = buildMMNodeTree(hop.getInput().get(0));
            setMatrixHistogram(hop.getName(),(EstimatorMatrixHistogram.MatrixHistogram) node.getSynopsis());
        }else if (Judge.isLeafMatrix(hop)) {
            node = new MMNode(hop.getDataCharacteristics());
            if (estimator instanceof EstimatorMatrixHistogram) {
                DistributedScratch.ec = ec;
                EstimatorMatrixHistogram.MatrixHistogram histogram = getMatrixHistogram(hop.getName());
                node.setSynopsis(histogram);
            }
        } else if (HopRewriteUtils.isMatrixMultiply(hop)) {
            MMNode m0 = buildMMNodeTree(hop.getInput().get(0));
            MMNode m1 = buildMMNodeTree(hop.getInput().get(1));
            node = new MMNode(m0, m1, SparsityEstimator.OpCode.MM);
        } else if (HopRewriteUtils.isBinaryMatrixScalarOperation(hop)) {
            if (hop.getInput().get(0).isMatrix()) {
                node = buildMMNodeTree(hop.getInput().get(0));
                buildMMNodeTree(hop.getInput().get(1));
            } else {
                buildMMNodeTree(hop.getInput().get(0));
                node =  buildMMNodeTree(hop.getInput().get(1));
            }
        } else if (HopRewriteUtils.isBinaryMatrixMatrixOperation(hop)) {
            MMNode m0 = buildMMNodeTree(hop.getInput().get(0));
            MMNode m1 = buildMMNodeTree(hop.getInput().get(1));
            node = new MMNode(m0, m1, SparsityEstimator.OpCode.MULT);
        } else if (HopRewriteUtils.isTransposeOperation(hop)) {
            MMNode m0 = buildMMNodeTree(hop.getInput().get(0));
            node = new MMNode(m0, SparsityEstimator.OpCode.TRANS);
        }
        if (node!=null) {
           // System.out.println("put "+hop.getHopID());
            hop2mmnode.put(hop,node);
        }
        return node;
    }


}
