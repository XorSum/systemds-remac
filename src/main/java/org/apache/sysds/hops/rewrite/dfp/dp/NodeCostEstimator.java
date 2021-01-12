package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.BinaryOp;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.lops.Data;
import org.apache.sysds.lops.LopProperties;
import org.apache.sysds.runtime.meta.DataCharacteristics;

import java.util.HashMap;

import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.createFullHistogram;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.getMatrixHistogram;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2.*;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2.MMShowCostFlag;

public class NodeCostEstimator {

    protected static final Log LOG = LogFactory.getLog(NodeCostEstimator.class.getName());

    private static double CpuSpeed = 1.0;
    private static double ShuffleSpeed = 5.0;
    private static double BroadCaseSpeed = 3.0;

    private static long defaultBlockSize = 1000;


    // private static SparsityEstimator estimator = new EstimatorBasicAvg();
    private static EstimatorMatrixHistogram estimator = new EstimatorMatrixHistogram();

    static HashMap<Pair<Integer, Integer>, MMNode> range2mmnode = new HashMap<>();

    public static MMNode addOpnode2Mmnode(OperatorNode opnode) {
        if (opnode.mmNode != null) return opnode.mmNode;
        for (Pair<Integer, Integer> r : opnode.ranges) {
            if (range2mmnode.containsKey(r)) {
                opnode.mmNode = range2mmnode.get(r);
                return opnode.mmNode;
            }
        }
        MMNode ans = null;
        if (Judge.isLeafMatrix(opnode.hops.get(0))) {
            DataCharacteristics dc = opnode.hops.get(0).getDataCharacteristics();
            if (estimator instanceof EstimatorMatrixHistogram) {
                EstimatorMatrixHistogram.MatrixHistogram histogram = getMatrixHistogram(opnode.hops.get(0).getName());
                if (histogram == null) {
                    System.out.println("histogram=null " + opnode.hops.get(0).getOpString());
                    dc.setNonZeros(dc.getRows() * dc.getCols());
                    histogram = createFullHistogram((int) dc.getRows(), (int) dc.getCols());
                } else {
                    dc.setNonZeros(histogram.getNonZeros());
                }
                ans = new MMNode(dc);
                ans.setSynopsis(histogram);
            } else {
                if (dc.getNonZeros() < 0) {
                    dc.setNonZeros(dc.getRows() * dc.getCols());
                }
                ans = new MMNode(dc);
            }
        } else if (HopRewriteUtils.isMatrixMultiply(opnode.hops.get(0))) {
            MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
            MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));
            ans = new MMNode(m0, m1, SparsityEstimator.OpCode.MM);
        } else if (HopRewriteUtils.isBinaryMatrixScalarOperation(opnode.hops.get(0))) {
            if (opnode.hops.get(0).getInput().get(0).isMatrix() || opnode.inputs.size() < 2) {
                ans = addOpnode2Mmnode(opnode.inputs.get(0));
            } else {
                ans = addOpnode2Mmnode(opnode.inputs.get(1));
            }
        } else if (HopRewriteUtils.isBinaryMatrixMatrixOperation(opnode.hops.get(0))) {
            MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
            MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));
            ans = new MMNode(m0, m1, SparsityEstimator.OpCode.PLUS);
        } else {
            System.out.println("un handled hop type: " + opnode.hops.get(0).getOpString());
        }
        if (ans != null) {
            for (Pair<Integer, Integer> r : opnode.ranges) {
                if (!range2mmnode.containsKey(r)) {
                    range2mmnode.put(r, ans);
                }
            }
            opnode.mmNode = ans;
        } else {
            System.out.println("mmnode == null");
            System.exit(0);
        }
        // System.out.println("RANGE2NODE SIZE = " +range2mmnode.size());
        return ans;
    }


    public static double getNodeCost(OperatorNode opnode) {
        // MMNode mmnode = addOpnode2Mmnode(opnode);
        //System.out.println("hop " + opnode.hop.getOpString());
        double ans = Double.MAX_VALUE;
        if (HopRewriteUtils.isMatrixMultiply(opnode.hops.get(0))) {
            ans = eMatrixMultiply(opnode);
        } else if (opnode.hops.get(0) instanceof BinaryOp) {
            System.out.println("binary op" + opnode.hops.get(0).getOpString());
            if (HopRewriteUtils.isBinaryMatrixScalarOperation(opnode.hops.get(0))) {
                ans = eBinaryMatrixScalar(opnode);
            } else if (HopRewriteUtils.isBinaryMatrixMatrixOperation(opnode.hops.get(0))) {
                ans = eBinaryMatrixMatrix(opnode);
            } else {
                ans = 0;
            }
        } else if (Judge.isLeafMatrix(opnode.hops.get(0))) {
            ans = 0;
        } else {
            System.out.println("unhandled operator type " + opnode.hops.get(0).getOpString());
            System.out.println("x");
        }
        if (ans > Double.MAX_VALUE / 2) {
            System.out.println("x");
        }
        return ans;
    }


    static DataCharacteristics getDC(OperatorNode opNode) {
        DataCharacteristics dc = null;
        try {
            MMNode mmNode = addOpnode2Mmnode(opNode);
            dc = estimator.estim(mmNode, false);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("x");
        }
        return dc;
    }


    static double eMatrixMultiply(OperatorNode node) {
        DataCharacteristics dc1 = getDC(node.inputs.get(0));
        DataCharacteristics dc2 = getDC(node.inputs.get(1));
        DataCharacteristics dc3 = getDC(node);
        double ans = Double.MAX_VALUE;
        AggBinaryOp hop = (AggBinaryOp) node.hops.get(0);
        if (hop.getExecType2() == LopProperties.ExecType.SPARK) {
            hop.constructLops();
            AggBinaryOp.MMultMethod method = hop.getMMultMethod();
            switch (method) {
                case MAPMM_R:
                case MAPMM_L:
                    ans = eMapMM(hop, method, dc1, dc2, dc3);
                    break;
                case RMM:
                    ans = eRMM(hop, dc1, dc2, dc3);
                    break;
                case MAPMM_CHAIN:
                case TSMM:
                case CPMM:
                default:
                    ans = eCPMM(hop, dc1, dc2, dc3);
            }
        } else {
            ans = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols();
        }
        return ans;
    }

    static double eMapMM(AggBinaryOp hop, AggBinaryOp.MMultMethod method,
                         DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
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

    static double eCPMM(AggBinaryOp hop,
                        DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
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

    static double eRMM(AggBinaryOp hop,
                       DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
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

    static double eTSMM(AggBinaryOp hop,
                        DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {

        return Double.MAX_VALUE;
    }

    static double eMapMMChain(AggBinaryOp hop,
                              DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {

        return Double.MAX_VALUE;
    }

    static double eBinaryMatrixScalar(OperatorNode operatorNode) {
        DataCharacteristics dc;
        if (operatorNode.hops.get(0).getInput().get(0).isMatrix() || operatorNode.inputs.size() < 2) {
            dc = getDC(operatorNode.inputs.get(0));
        } else {
            dc = getDC(operatorNode.inputs.get(1));
        }
        // todo shuffle
        return dc.getNonZeros() * CpuSpeed;
    }

    static double eBinaryMatrixMatrix(OperatorNode operatorNode) {
        DataCharacteristics dc0 = getDC(operatorNode.inputs.get(0));
        DataCharacteristics dc1 = getDC(operatorNode.inputs.get(1));

        double ans = (dc0.getNonZeros() + dc1.getNonZeros()) * CpuSpeed;
        // todo shuffle
        return ans;
    }


}
