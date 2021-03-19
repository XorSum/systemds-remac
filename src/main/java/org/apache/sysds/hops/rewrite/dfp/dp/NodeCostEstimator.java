package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.*;
import org.apache.sysds.hops.estim.EstimatorBasicAvg;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.lops.LopProperties;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.utils.Explain;

import java.util.HashMap;

import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.createFullHistogram;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.getMatrixHistogram;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2.*;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2.MMShowCostFlag;

public class NodeCostEstimator {

    protected static final Log LOG = LogFactory.getLog(NodeCostEstimator.class.getName());

    private static SparsityEstimator metadataEstimator = new EstimatorBasicAvg();
    private static EstimatorMatrixHistogram mncEstimator = new EstimatorMatrixHistogram();

    public HashMap<Pair<Integer, Integer>, MMNode> range2mmnode = new HashMap<>();

    public MMNode addOpnode2Mmnode(OperatorNode opnode) {
        if (opnode.mmNode != null) return opnode.mmNode;
        if (range2mmnode.containsKey(opnode.range)) {
            opnode.mmNode = range2mmnode.get(opnode.range);
            return opnode.mmNode;
        }
        MMNode ans = null;
        Hop hop = opnode.hops.get(0);
        if (hop.isScalar()) return null;
        if (Judge.isLeafMatrix(hop)) {
            DataCharacteristics dc = hop.getDataCharacteristics();
            ans = new MMNode(dc);
            if (useMncEstimator) {
                EstimatorMatrixHistogram.MatrixHistogram histogram = getMatrixHistogram(hop.getName());
                if (histogram == null) {
                    dc.setNonZeros(dc.getRows() * dc.getCols());
                    histogram = createFullHistogram((int) dc.getRows(), (int) dc.getCols());
                }
                ans.setSynopsis(histogram);
            }
            if (opnode.isTranspose) ans = new MMNode(ans, SparsityEstimator.OpCode.TRANS);
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
            } else if (hop.getInput().get(0).isScalar() && hop.getInput().get(1).isScalar()) {
                //   System.out.println("scalar scalar operation");
                ans = addOpnode2Mmnode(opnode.inputs.get(0));
                addOpnode2Mmnode(opnode.inputs.get(1));
            } else {
                MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
                MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));
                if (hop.getOpString().equals("b(+)") || hop.getOpString().equals("b(-)")) {
                    ans = new MMNode(m0, m1, SparsityEstimator.OpCode.PLUS);
                } else if (hop.getOpString().equals("b(*)") || hop.getOpString().equals("b(/)")) {
                    ans = new MMNode(m0, m1, SparsityEstimator.OpCode.MULT);
                } else {
                    //   System.out.println("un handled hop type: " + hop.getOpString());
                }
            }
        } else if (hop instanceof NaryOp || hop instanceof TernaryOp) {
            MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
            MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));
            ans = new MMNode(m0, m1, SparsityEstimator.OpCode.PLUS);
        } else {
            // System.out.println("un handled hop type: " + hop.getOpString());
            // boolean tmp = HopRewriteUtils.isBinaryMatrixMatrixOperation(hop);
            //  System.out.println(tmp);
        }
        if (ans != null) {
//            if (!Judge.isLeafMatrix(hop)) {
//                for (int i = 1; i < opnode.hops.size(); i++) {
//                    if (HopRewriteUtils.isTransposeOperation(opnode.hops.get(i))) {
//                        if (ans.getOp() == SparsityEstimator.OpCode.TRANS) {
//                            ans = ans.getLeft();
//                        } else {
//                            ans = new MMNode(ans, SparsityEstimator.OpCode.TRANS);
//                        }
//                    }
//                }
//            }
            if (!range2mmnode.containsKey(opnode.range)) {
                range2mmnode.put(opnode.range, ans);
            }
            opnode.mmNode = ans;
        } else {
            //   System.out.println("mmnode == null");
            System.exit(0);
        }
        // System.out.println("RANGE2NODE SIZE = " +range2mmnode.size());
        return ans;
    }

    DataCharacteristics getDC(OperatorNode opNode) {
        DataCharacteristics dc = null;
        MMNode mmNode = addOpnode2Mmnode(opNode);
        try {
            if (useMncEstimator) {
                dc = mncEstimator.estim(mmNode, false);
            } else {
                dc = metadataEstimator.estim(mmNode);
            }
        } catch (Exception e) {
            e.printStackTrace();
//            CostGraph.explainOperatorNode(opNode, 0);
            LOG.error("get dc error" + opNode);
            //throw e;
            System.exit(-1);
            //  dc = new MatrixCharacteristics(mmNode.getRows(), mmNode.getCols(), mmNode.getRows() * mmNode.getCols());
        }
//        Hop h = opNode.hops.get(0);
//        boolean check = (h.getDim1()==dc.getRows()&&h.getDim2()==dc.getCols()) ||
//                (h.getDim2()==dc.getRows()&&h.getDim1()==dc.getCols());
//        if (!check) {
//            LOG.info(Explain.explain(h));
//            LOG.info(dc);
//            LOG.info(CostGraph.explainOpNode(opNode,0));
//            LOG.info("^^^^^^^^^^^");
//        }
        return dc;
    }

    public NodeCost getNodeCost(OperatorNode opnode) {
        NodeCost ans = NodeCost.INF();
        Hop hop = opnode.hops.get(0);
        if (HopRewriteUtils.isMatrixMultiply(hop)) {  // 矩阵乘法
            ans = eMatrixMultiply(opnode, (AggBinaryOp) hop);
        } else if (hop instanceof BinaryOp) {   // 二元操作
            // System.out.println("binary op" + hop.getOpString());
            if (HopRewriteUtils.isBinaryMatrixScalarOperation(hop)) {
                ans = eBinaryMatrixScalar(opnode, hop);
            } else if (hop.getInput().get(0).isScalar() && hop.getInput().get(1).isScalar()) {// scalar scalar operation
                ans = NodeCost.ZERO();
            } else //if (HopRewriteUtils.isBinaryMatrixMatrixOperation(hop))
            {
                ans = eBinaryMatrixMatrix(opnode, hop);
            }
        } else if (Judge.isLeafMatrix(hop)) {
            ans = NodeCost.ZERO();
        } else if (hop instanceof TernaryOp || hop instanceof NaryOp) { // 多元操作
            ans = eBinaryMatrixMatrix(opnode, hop);
        } else {
            // System.out.println("unhandled operator type " + hop.getOpString());
            //  System.out.println("x");
        }
        for (int i = 1; i < opnode.hops.size(); i++) {
            hop = opnode.hops.get(i);
            if (hop instanceof BinaryOp) {
                if (hop.isMatrix()) {
                    DataCharacteristics dc = getDC(opnode);
                    if (hop.optFindExecType() == LopProperties.ExecType.SPARK) {
                        ans.computeCost += dc.getNonZeros() * CpuSpeed / defaultWorkerNumber;
                    } else {
                        ans.computeCost += dc.getNonZeros() * CpuSpeed;
                    }
                }
            }
        }
        ans.collectCost += eCollectCost(opnode);
        if (ans.getSummary() >= Double.MAX_VALUE / 2 || ans.getSummary() < 0) {
            LOG.error("cost infinate " + opnode);
            System.exit(-1);
        }
        return ans;
    }


    NodeCost eMatrixMultiply(OperatorNode node, AggBinaryOp hop) {
        DataCharacteristics dc1 = getDC(node.inputs.get(0));
        DataCharacteristics dc2 = getDC(node.inputs.get(1));
        DataCharacteristics dc3 = getDC(node);
        //double ans = Double.MAX_VALUE;
        NodeCost ans = null;
        if (hop.optFindExecType() == LopProperties.ExecType.SPARK) {
//        if (true){
            hop.constructLops();
            AggBinaryOp.MMultMethod method = hop.getMMultMethod();
            node.method = method;
//            System.out.println(method);
            switch (method) {
                case MAPMM_R:
                case MAPMM_L:
                    ans = eMapMM(node, hop, method, dc1, dc2, dc3);
                    break;
                case RMM:
                    ans = eRMM(hop, dc1, dc2, dc3);
                    break;
                case MAPMM_CHAIN:
                    node.isXtXv = true;
                    ans = eMapMMChain(node,hop);
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
            double computeCost = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols();
            ans = new NodeCost(0, 0, computeCost, 0);
        }
        return ans;
    }


    public static void main(String[] args) {

        long dim1 = 58400000;
        long dim2 = 8692;
        long nnz = 2277598765l;

        DataOp a = new DataOp("a", Types.DataType.MATRIX, Types.ValueType.FP64,
                Types.OpOpData.TRANSIENTREAD, "", dim1, dim2, nnz, 1000);

        Hop b = HopRewriteUtils.createTranspose(a);

        AggBinaryOp c = HopRewriteUtils.createMatrixMultiply(b, a);

        DataCharacteristics dc1 = b.getDataCharacteristics();
        DataCharacteristics dc2 = a.getDataCharacteristics();
        DataCharacteristics dc3 = c.getDataCharacteristics();

        System.out.println(dc1);
        System.out.println(dc2);
        System.out.println(dc3);


        NodeCostEstimator costEstimator = new NodeCostEstimator();
        NodeCost costtsmm = costEstimator.eTSMM(c, dc1, dc2, dc3);
//        double costts2 =  costEstimator.eTSMM(c,dc2,dc1,dc3);
        NodeCost costcp = costEstimator.eCPMM(c, dc1, dc2, dc3);
        NodeCost costr = costEstimator.eRMM(c, dc1, dc2, dc3);
        NodeCost costzip = costEstimator.eZipMM(c, dc1, dc2, dc3);
        OperatorNode node = new OperatorNode();
        NodeCost costmapmm = costEstimator.eMapMM(node, c, AggBinaryOp.MMultMethod.MAPMM_R, dc1, dc2, dc3);
        double controlProgramMM = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols();

        c.constructLops();
        AggBinaryOp.MMultMethod method = c.getMMultMethod();

        System.out.println("method = " + method);
        System.out.println("reducers = " + getNumberReducers());
        System.out.println("cost tsmm = " + costtsmm);
        System.out.println("cost cpmm = " + costcp);
        System.out.println("cost rmm = " + costr);
        System.out.println("cost zipmm = " + costzip);
        System.out.println("cost mapmm = " + costmapmm);
        System.out.println("cost control program mm = " + controlProgramMM);

    }


    NodeCost eMapMM(OperatorNode node, AggBinaryOp hop, AggBinaryOp.MMultMethod method,
                    DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        double broadcastCost = Double.MAX_VALUE;
        double computeCost = Double.MAX_VALUE;
        double shuffleCost = Double.MAX_VALUE;
        double collectCost = 0;
        boolean isLeft = (!node.isTranspose && method == AggBinaryOp.MMultMethod.MAPMM_L)
                || (node.isTranspose && method == AggBinaryOp.MMultMethod.MAPMM_R);
//        if (node.isTranspose) {
//            System.out.println("x");
//        }
        long r2 = reducerNumber(dc3.getRows(), dc3.getCols());
        if (isLeft) {
            long r1 = Math.min(getNumberReducers(), (long) Math.ceil((double) dc2.getRows() / defaultBlockSize));
            double br = Math.ceil(Math.log(Math.min(r1, defaultWorkerNumber)));
            broadcastCost = BroadCaseSpeed * matrixSize(dc1) * br;
            shuffleCost = ShuffleSpeed * matrixSize(dc3) * r1 / r2;
            computeCost = computeCostSPMM(dc1, dc2, dc3);
        } else {
            long r1 = Math.min(getNumberReducers(), (long) Math.ceil((double) dc1.getCols() / defaultBlockSize));
            double br = Math.ceil(Math.log(Math.min(r1, defaultWorkerNumber)));
            broadcastCost = BroadCaseSpeed * matrixSize(dc2) * br;
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
//        return computeCost + shuffleCost + broadcastCost + collectCost;
        return new NodeCost(shuffleCost, broadcastCost, computeCost, collectCost);
    }

    NodeCost eCPMM(AggBinaryOp hop,
                   DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        long r = Math.min((long) Math.ceil((double) dc2.getRows() / defaultBlockSize), //max used reducers
                getNumberReducers()); //available reducer

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
//        return shuffleCost1 + shuffleCost2 + computeCost;
        return new NodeCost(shuffleCost1 + shuffleCost2, 0, computeCost, 0);
    }

    NodeCost eRMM(AggBinaryOp hop,
                  DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        long m1_nrb = (long) Math.ceil((double) dc1.getRows() / defaultBlockSize); // number of row blocks in m1
        long m2_ncb = (long) Math.ceil((double) dc2.getCols() / defaultBlockSize); // number of column blocks in m2
        long k = (long) Math.ceil((double) dc2.getRows() / defaultBlockSize);

        double rmm_nred = Math.min((double) m1_nrb * m2_ncb, //max used reducers
                getNumberReducers()); //available reducers

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
//        return shuffleCost1 + shuffleCost2 + computeCost;
        return new NodeCost(shuffleCost1 + shuffleCost2, 0, computeCost, 0);
    }

    NodeCost eTSMM(AggBinaryOp hop,
                   DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        long reducer = (long) Math.ceil(Math.max(dc1.getRows(), dc1.getCols()) * 1.0 / defaultBlockSize);
        reducer = Math.min(reducer, getNumberReducers());
        double computeCost = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc1.getSparsity() / defaultWorkerNumber;
        double shuffleCost = ShuffleSpeed * MatrixBlock.estimateSizeInMemory(dc3.getRows(), dc3.getRows(), dc3.getSparsity());
//        return computeCost + shuffleCost;
        return new NodeCost(shuffleCost, 0, computeCost, collectCostSummary);
    }

    NodeCost eMapMMChain(OperatorNode node,AggBinaryOp hop){
//        System.out.println("MAPMMCHAIN");
        DataCharacteristics dc1,dc2,dc3;
        dc3 = getDC(node);
        if (!node.isTranspose) {
            OperatorNode node2 = node.inputs.get(1);
            OperatorNode nodeX = node2.inputs.get(0);
            OperatorNode nodeV = node2.inputs.get(1);
            dc1 = getDC(nodeX);
            dc2 = getDC(nodeV);
        } else {
            OperatorNode node2 = node.inputs.get(0);
            OperatorNode nodeXt = node2.inputs.get(1);
            OperatorNode nodeVt = node2.inputs.get(0);
            DataCharacteristics  dcXt = getDC(nodeXt);
            DataCharacteristics dcVt = getDC(nodeVt);
            dc1 = new MatrixCharacteristics(dcXt.getCols(),dcXt.getRows(),dcXt.getNonZeros());
            dc2 = new MatrixCharacteristics(dcVt.getCols(),dcVt.getRows(),dcVt.getNonZeros());
        }
        return eMapMMChain(hop,dc1,dc2,dc3);
    }


    NodeCost eMapMMChain(AggBinaryOp hop,
                         DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        // todo transpose direction
        long reducer = reducerNumber(dc1.getRows(), dc1.getCols());
        reducer = Math.min(reducer, getNumberReducers());
        double computeCost = 1.5 * CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * (dc1.getSparsity() * dc2.getSparsity() + dc1.getSparsity()) / defaultWorkerNumber;
        double broadcastCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc2.getRows(), dc2.getCols(), dc2.getSparsity()) * Math.ceil(Math.log(defaultWorkerNumber));
        double reduceCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc3.getRows(), dc3.getRows(), dc3.getSparsity());
//        return computeCost + reduceCost + broadcastCost;
        return new NodeCost(0, broadcastCost, computeCost, reduceCost);
    }

    NodeCost eZipMM(AggBinaryOp hop,
                    DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        long reducer = (long) Math.ceil(Math.max(dc1.getRows(), dc1.getCols()) * 1.0 / defaultBlockSize);
        reducer = Math.min(reducer, getNumberReducers());
        double computeCost = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc2.getSparsity() / defaultWorkerNumber;
        double reduceCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc3.getRows(), dc3.getRows(), dc3.getSparsity()) * reducer;
//        return computeCost + reduceCost;
        return new NodeCost(0, 0, computeCost, reduceCost);
    }


    NodeCost eBinaryMatrixScalar(OperatorNode operatorNode, Hop hop) {
        DataCharacteristics dc;
        if (operatorNode.hops.get(0).getInput().get(0).isMatrix() || operatorNode.inputs.size() < 2) {
            dc = getDC(operatorNode.inputs.get(0));
        } else {
            dc = getDC(operatorNode.inputs.get(1));
        }
        double cost = dc.getNonZeros() * CpuSpeed;
        if (hop.optFindExecType() == LopProperties.ExecType.SPARK) {
            cost /= defaultWorkerNumber;
        }
        if (hop.getOpString().equals("b(*)") || hop.getOpString().equals("b(/)")) {
            cost *= 2;
        }
        return new NodeCost(0, 0, cost, 0);
    }

    NodeCost eBinaryMatrixMatrix(OperatorNode operatorNode, Hop hop) {
        DataCharacteristics dc0 = getDC(operatorNode.inputs.get(0));
        DataCharacteristics dc1 = getDC(operatorNode.inputs.get(1));
        double computeCost = 0, shuffleCost = 0;
        if (hop.optFindExecType() == LopProperties.ExecType.SPARK) {
            long reducerNumber = reducerNumber(dc0.getRows(), dc0.getCols());
            long size0 = MatrixBlock.estimateSizeInMemory(dc0.getRows(), dc0.getCols(), dc0.getSparsity());
            long size1 = MatrixBlock.estimateSizeInMemory(dc1.getRows(), dc1.getCols(), dc1.getSparsity());
            computeCost = CpuSpeed * (dc0.getNonZeros() + dc1.getNonZeros()) / defaultWorkerNumber;
            shuffleCost = ShuffleSpeed * (size0 + size1) / reducerNumber;
        } else {
            computeCost = CpuSpeed * (dc0.getNonZeros() + dc1.getNonZeros());
        }
        if (hop.getOpString().equals("b(*)")) {
            computeCost *= 2;
        } else if (hop.getOpString().equals("b(/)")) {
            computeCost *= 4;
        }
        return new NodeCost(shuffleCost, 0, computeCost, 0);
//        return computeCost + shuffleCost;
    }

    double eCollectCost(OperatorNode opnode) {
        if (opnode.hops.get(0).optFindExecType() != LopProperties.ExecType.CP) {
            return 0;
        }
        double cost = 0;
        for (int i = 0; i < opnode.inputs.size(); i++) {
            OperatorNode child = opnode.inputs.get(i);
            if (child.hops.get(child.hops.size() - 1).optFindExecType() == LopProperties.ExecType.SPARK) {
                DataCharacteristics dc = getDC(child);
                cost += BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc.getRows(), dc.getCols(), dc.getSparsity());
            }
        }
        return cost;
    }


}
