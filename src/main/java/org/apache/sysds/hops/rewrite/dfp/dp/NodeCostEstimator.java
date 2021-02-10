package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.BinaryOp;
import org.apache.sysds.hops.Hop;
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
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;

import java.util.HashMap;
import java.util.HashSet;

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
        if (range2mmnode.containsKey(opnode.range)) {
            opnode.mmNode = range2mmnode.get(opnode.range);
            return opnode.mmNode;
        }
        MMNode ans = null;
        Hop hop = opnode.hops.get(0);
        if (Judge.isLeafMatrix(hop)) {
            DataCharacteristics dc = hop.getDataCharacteristics();
//            if (estimator instanceof EstimatorMatrixHistogram) {
            EstimatorMatrixHistogram.MatrixHistogram histogram = getMatrixHistogram(hop.getName());
            if (histogram == null) {
                System.out.println("histogram=null " + hop.getOpString());
                dc.setNonZeros(dc.getRows() * dc.getCols());
                histogram = createFullHistogram((int) dc.getRows(), (int) dc.getCols());
            } else {
                dc.setNonZeros(histogram.getNonZeros());
            }
            ans = new MMNode(dc);
            ans.setSynopsis(histogram);
//            } else {
//                if (dc.getNonZeros() < 0) {
//                    dc.setNonZeros(dc.getRows() * dc.getCols());
//                }
//                ans = new MMNode(dc);
//            }
        } else if (HopRewriteUtils.isMatrixMultiply(hop)) {
            MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
            MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));
            ans = new MMNode(m0, m1, SparsityEstimator.OpCode.MM);
        } else if (hop instanceof BinaryOp) {
            if (HopRewriteUtils.isBinaryMatrixScalarOperation(hop)) {
                if (hop.getInput().get(0).isMatrix() || opnode.inputs.size() < 2) {
                    ans = addOpnode2Mmnode(opnode.inputs.get(0));
                } else {
                    ans = addOpnode2Mmnode(opnode.inputs.get(1));
                }
            }else if (hop.getInput().get(0).isScalar()&&hop.getInput().get(1).isScalar()){
                System.out.println("scalar scalar operation");
                ans = addOpnode2Mmnode(opnode.inputs.get(0));
                addOpnode2Mmnode(opnode.inputs.get(1));
            }else {
                MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
                MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));
                if (hop.getOpString().equals("b(+)")||hop.getOpString().equals("b(-)")) {
                    ans = new MMNode(m0, m1, SparsityEstimator.OpCode.PLUS);
                }else if (hop.getOpString().equals("b(*)")||hop.getOpString().equals("b(/)")) {
                    ans = new MMNode(m0, m1, SparsityEstimator.OpCode.MULT);
                }else {
                    System.out.println("un handled hop type: " + hop.getOpString());
                }
            }
        } else {
            System.out.println("un handled hop type: " + hop.getOpString());
            boolean tmp = HopRewriteUtils.isBinaryMatrixMatrixOperation(hop);
            System.out.println(tmp);
        }
        if (ans != null) {
            if (!range2mmnode.containsKey(opnode.range)) {
                range2mmnode.put(opnode.range, ans);
            }
            opnode.mmNode = ans;
        } else {
            System.out.println("mmnode == null");
            System.exit(0);
        }
        // System.out.println("RANGE2NODE SIZE = " +range2mmnode.size());
        return ans;
    }

/////////////////////////////////////////////////////////////////////////////////////////////////////

    public static double getNodeCost(OperatorNode opnode) {
        // MMNode mmnode = addOpnode2Mmnode(opnode);
        //System.out.println("hop " + opnode.hop.getOpString());
        double ans = Double.MAX_VALUE;
        Hop hop = opnode.hops.get(0);
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
            ans = eMatrixMultiply(opnode, (AggBinaryOp) hop);
        } else if (hop instanceof BinaryOp) {
            // System.out.println("binary op" + hop.getOpString());
            if (HopRewriteUtils.isBinaryMatrixScalarOperation(hop)) {
                ans = eBinaryMatrixScalar(opnode, hop);
            } else if (hop.getInput().get(0).isScalar() && hop.getInput().get(1).isScalar()) {// scalar scalar operation
                ans = 0;
            } else //if (HopRewriteUtils.isBinaryMatrixMatrixOperation(hop))
            {
                ans = eBinaryMatrixMatrix(opnode, hop);
            }
        } else if (Judge.isLeafMatrix(hop)) {
            ans = 0;
        } else {
            System.out.println("unhandled operator type " + hop.getOpString());
            //  System.out.println("x");
        }
        for (int i = 1; i < opnode.hops.size(); i++) {
            hop = opnode.hops.get(i);
            if (Judge.isWrite(hop)) {
                ans += 0;
            } else if (HopRewriteUtils.isUnary(hop, Types.OpOp1.CAST_AS_SCALAR)) {
                ans += 0;
            } else if (HopRewriteUtils.isTransposeOperation(hop)) {
                DataCharacteristics dc = getDC(opnode);
                ans += dc.getNonZeros() * CpuSpeed;
            } else if (hop instanceof BinaryOp) {
                DataCharacteristics dc = getDC(opnode);
                ans += dc.getNonZeros() * CpuSpeed;
            } else {
                System.out.println("unhandled operator type " + hop.getOpString());
            }
        }
        ans += eCollectCost(opnode);
        if (ans > Double.MAX_VALUE / 2) {
            LOG.error("cost infinate ");
        }
        return ans;
    }


    static DataCharacteristics getDC(OperatorNode opNode) {
        DataCharacteristics dc = null;
        MMNode mmNode = addOpnode2Mmnode(opNode);
        try {
            dc = estimator.estim(mmNode, false);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("get dc error" + opNode);
            dc = new MatrixCharacteristics(mmNode.getRows(), mmNode.getCols(), mmNode.getRows() * mmNode.getCols());
        }
        return dc;
    }


    static double eMatrixMultiply(OperatorNode node, AggBinaryOp hop) {
        DataCharacteristics dc1 = getDC(node.inputs.get(0));
        DataCharacteristics dc2 = getDC(node.inputs.get(1));
        DataCharacteristics dc3 = getDC(node);
        double ans = Double.MAX_VALUE;
        if (hop.getExecType() == LopProperties.ExecType.SPARK) {
//        if (true){
            hop.constructLops();
            AggBinaryOp.MMultMethod method = hop.getMMultMethod();
//            System.out.println(method);
            switch (method) {
                case MAPMM_R:
                case MAPMM_L:
                    ans = eMapMM(hop, method, dc1, dc2, dc3);
                    break;
                case RMM:
                    ans = eRMM(hop, dc1, dc2, dc3);
                    break;
                case MAPMM_CHAIN:
                    ans = eMapMMChain(hop, dc1, dc2, dc3);
                    break;
                case ZIPMM:
                    ans = eZipMM(hop, dc1, dc2, dc3);
                    break;
                case TSMM:
                    ans = eTSMM(hop, dc1, dc2, dc3);
                    break;
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
        double collectCost = 0;
        long r2 = reducerNumber(dc3.getRows(), dc3.getCols());
        if (method == AggBinaryOp.MMultMethod.MAPMM_L) {
            long r1 = Math.min(OptimizerUtils.getNumReducers(false), (long) Math.ceil((double) dc2.getRows() / defaultBlockSize));
            if (hop.getInput().get(0).getExecType() == LopProperties.ExecType.SPARK)
                collectCost = BroadCaseSpeed * matrixSize(dc1);
            broadcastCost = BroadCaseSpeed * matrixSize(dc1) * Math.ceil(Math.log(r1));
            shuffleCost = ShuffleSpeed * matrixSize(dc3) * r1 / r2;
            computeCost = computeCostSPMM(dc1, dc2, dc3);
        } else if (method == AggBinaryOp.MMultMethod.MAPMM_R) {
            long r1 = Math.min(OptimizerUtils.getNumReducers(false), (long) Math.ceil((double) dc1.getCols() / defaultBlockSize));
            if (hop.getInput().get(1).getExecType() == LopProperties.ExecType.SPARK)
                collectCost = BroadCaseSpeed * matrixSize(dc1);
            broadcastCost = BroadCaseSpeed * matrixSize(dc2) * Math.ceil(Math.log(r1));
            shuffleCost = ShuffleSpeed * matrixSize(dc3) * r1 / r2;
            computeCost = computeCostSPMM(dc1, dc2, dc3);
        }
        if (MMShowCostFlag) {
            LOG.info("begin<<< ");
            LOG.info("mapmm broadcast cost = " + broadcastCost);
            LOG.info("mapmm compute cost = " + computeCost);
            LOG.info("mapmm shuffle cost = " + shuffleCost);
            LOG.info("mapmm collect cost = " + collectCost);
            LOG.info("  end>>>");
        }
        return computeCost + shuffleCost + broadcastCost + collectCost;
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
        long reducer = (long) Math.ceil(Math.max(dc1.getRows(), dc1.getCols()) * 1.0 / defaultBlockSize);
        reducer = Math.min(reducer, OptimizerUtils.getNumReducers(false));
        double computeCost = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc1.getSparsity() / reducer;
        double shuffleCost = ShuffleSpeed * MatrixBlock.estimateSizeInMemory(dc3.getRows(), dc3.getRows(), dc3.getSparsity());
        return computeCost + shuffleCost;
    }

    static double eMapMMChain(AggBinaryOp hop,
                              DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        long reducer = (long) Math.ceil(Math.max(dc1.getRows(), dc1.getCols()) * 1.0 / defaultBlockSize);
        reducer = Math.min(reducer, OptimizerUtils.getNumReducers(false));

        double computeCost = 1.5 * CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * (dc1.getSparsity() * dc2.getSparsity() + dc1.getSparsity()) / reducer;
        double broadcastCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc2.getRows(), dc2.getCols(), dc2.getSparsity()) * Math.ceil(Math.log(reducer));
        double reduceCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc3.getRows(), dc3.getRows(), dc3.getSparsity());
        return computeCost + reduceCost + broadcastCost;
    }

    static double eZipMM(AggBinaryOp hop,
                         DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        long reducer = (long) Math.ceil(Math.max(dc1.getRows(), dc1.getCols()) * 1.0 / defaultBlockSize);
        reducer = Math.min(reducer, OptimizerUtils.getNumReducers(false));
        double computeCost = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc1.getSparsity() / reducer;
        double reduceCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc3.getRows(), dc3.getRows(), dc3.getSparsity()) * reducer;
        return computeCost + reduceCost;
    }


    static double eBinaryMatrixScalar(OperatorNode operatorNode, Hop hop) {
        DataCharacteristics dc;
        if (operatorNode.hops.get(0).getInput().get(0).isMatrix() || operatorNode.inputs.size() < 2) {
            dc = getDC(operatorNode.inputs.get(0));
        } else {
            dc = getDC(operatorNode.inputs.get(1));
        }
        double cost = dc.getNonZeros() * CpuSpeed;
        if (hop.getExecType() == LopProperties.ExecType.SPARK) {
            long exer = reducerNumber(dc.getRows(), dc.getCols());
            cost /= exer;
        }
        if (hop.getOpString().equals("b(*)") ||hop.getOpString().equals("b(/)")  ) {
            cost *= 2;
        }
        return cost;
    }

    static double eBinaryMatrixMatrix(OperatorNode operatorNode, Hop hop) {
        DataCharacteristics dc0 = getDC(operatorNode.inputs.get(0));
        DataCharacteristics dc1 = getDC(operatorNode.inputs.get(1));
        double computeCost = 0, shuffleCost = 0;
        if (hop.getExecType() == LopProperties.ExecType.SPARK) {
            long exer = reducerNumber(dc0.getRows(), dc0.getCols());
            long size0 = MatrixBlock.estimateSizeInMemory(dc0.getRows(), dc0.getCols(), dc0.getSparsity());
            long size1 = MatrixBlock.estimateSizeInMemory(dc1.getRows(), dc1.getCols(), dc1.getSparsity());
            computeCost = CpuSpeed * (dc0.getNonZeros() + dc1.getNonZeros()) / exer;
            if (hop.getOpString().equals("b(*)")) {
                computeCost *= 2;
            }
            shuffleCost = ShuffleSpeed * (size0 + size1) / exer;
        } else {
            computeCost = CpuSpeed * (dc0.getNonZeros() + dc1.getNonZeros());
        }
        return computeCost + shuffleCost;
    }

    static double eCollectCost(OperatorNode opnode) {
        if (opnode.hops.get(0).getExecType() != LopProperties.ExecType.CP) {
            return 0;
        }
        double cost = 0;
        for (int i = 0; i < opnode.inputs.size(); i++) {
            OperatorNode child = opnode.inputs.get(i);
            if (child.hops.get(child.hops.size() - 1).getExecType() == LopProperties.ExecType.SPARK) {
                DataCharacteristics dc = getDC(child);
                cost += BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc.getRows(), dc.getCols(), dc.getSparsity());
            }
        }
        return cost;
    }


}
