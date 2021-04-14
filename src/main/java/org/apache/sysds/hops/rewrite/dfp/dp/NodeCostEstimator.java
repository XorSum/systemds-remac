package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sysds.hops.*;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.GenericDisjointSet;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.lops.LopProperties;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.spark.CpmmSPInstruction;
import org.apache.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.data.MatrixIndexes;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;
import org.apache.sysds.utils.Explain;

import java.util.HashMap;

import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.createFullHistogram;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.getMatrixHistogram;
import static org.apache.sysds.hops.rewrite.dfp.costmodel.CostModelCommon.*;

public class NodeCostEstimator {

    private SparkExecutionContext sec;
    protected static final Log LOG = LogFactory.getLog(NodeCostEstimator.class.getName());

    public static long estimateTime = 0;

    public HashMap<Pair<Integer, Integer>, MMNode> range2mmnodeCache = new HashMap<>();
    public HashMap<DRange, NodeCost> drange2multiplycostCache = new HashMap<>();

    GenericDisjointSet<DRange> dRangeDisjointSet = new GenericDisjointSet<>();
    GenericDisjointSet<Pair<Integer, Integer>> rangeDisjointSet = new GenericDisjointSet<>();

    boolean useCommonCostCache = true;

    public NodeCostEstimator(SparkExecutionContext sec) {
        this.sec = sec;
    }

    int build_mmnode_counter = 0;
    int estimate_matrix_multiply_counter = 0;

    public void resetCacheCounter() {
        build_mmnode_counter = 0;
        estimate_matrix_multiply_counter = 0;
    }

    public void printCacheStats() {
        LOG.info("range2mmnode size = " + range2mmnodeCache.size());
        LOG.info("drange2multiplycost size = " + drange2multiplycostCache.size());
        LOG.info("build mmnode count = " + build_mmnode_counter);
        LOG.info("estimate matrix multiply count = " + estimate_matrix_multiply_counter);
        LOG.info(range2mmnodeCache);
        LOG.info(drange2multiplycostCache);
    }

    public MMNode getMmnode(OperatorNode opnode) {

        //  查找缓存
        if (opnode.mmNode != null) return opnode.mmNode;
        if (range2mmnodeCache.containsKey(opnode.dRange.getRange())) {
            opnode.mmNode = range2mmnodeCache.get(opnode.dRange.getRange());
            return opnode.mmNode;
        }

        // 如果缓存中没有，就去计算
        build_mmnode_counter++;
        MMNode ans = addOpnode2Mmnode(opnode);

        // 更新缓存
        // todo: update co-cse dranges
        // todo: transpose

        if (useCommonCostCache) {
//            for (Pair<Integer, Integer> range : rangeDisjointSet.elements(opnode.dRange.range)) {
//                if (!range2mmnodeCache.containsKey(range)) {
//                    range2mmnodeCache.put(range, ans);
//                }
//            }
        }
        if (!range2mmnodeCache.containsKey(opnode.dRange.getRange())) {
            range2mmnodeCache.put(opnode.dRange.getRange(), ans);
        }
        opnode.mmNode = ans;
        return ans;
    }


    public MMNode addOpnode2Mmnode(OperatorNode opnode) {

//        LOG.info("start add opnode to mmnode");
        MMNode ans = null;
        Hop hop = opnode.hops.get(0);
        if (hop.isScalar()) return null;
        if (Judge.isLeafMatrix(hop)) {
            if (useMncEstimator) {
                EstimatorMatrixHistogram.MatrixHistogram histogram = getMatrixHistogram(hop.getName());
                DataCharacteristics dc = hop.getDataCharacteristics();
                if (histogram == null) {
                    dc.setNonZeros(dc.getRows() * dc.getCols());
                    histogram = createFullHistogram((int) dc.getRows(), (int) dc.getCols());
                    LOG.info("get by mnc null "+hop.getName()+" "+dc);
                } else {
                    dc.setNonZeros(histogram.getNonZeros());
                    LOG.info("get by mnc not null "+hop.getName()+" "+histogram.getNonZeros());
                }
                ans = new MMNode(dc);
                ans.setSynopsis(histogram);
            } else {
                MatrixObject matrixBlock = (MatrixObject) sec.getVariable(hop.getName());
                DataCharacteristics dc;
                if (matrixBlock != null) {
                    dc = matrixBlock.getDataCharacteristics();
//                    LOG.info("get by metadata not null "+hop.getName()+" "+dc);
                } else {
                    dc = hop.getDataCharacteristics();
//                    LOG.info("get by metadata null "+hop.getName()+" "+dc);
                }
                ans = new MMNode(dc);
                ans.setDataCharacteristics(dc);
            }
            if (opnode.isTranspose) ans = new MMNode(ans, SparsityEstimator.OpCode.TRANS);
        } else if (HopRewriteUtils.isMatrixMultiply(hop)) {
            MMNode m0 = getMmnode(opnode.inputs.get(0));
            MMNode m1 = getMmnode(opnode.inputs.get(1));
            ans = new MMNode(m0, m1, SparsityEstimator.OpCode.MM);
        } else if (hop instanceof BinaryOp) {
            if (HopRewriteUtils.isBinaryMatrixScalarOperation(hop)) {
                if (hop.getInput().get(0).isMatrix() || opnode.inputs.size() < 2) {
                    ans = getMmnode(opnode.inputs.get(0));
                } else {
                    ans = getMmnode(opnode.inputs.get(1));
                }
            } else if (hop.getInput().get(0).isScalar() && hop.getInput().get(1).isScalar()) {
                //   System.out.println("scalar scalar operation");
                ans = getMmnode(opnode.inputs.get(0));
                getMmnode(opnode.inputs.get(1));
            } else {
                MMNode m0 = getMmnode(opnode.inputs.get(0));
                MMNode m1 = getMmnode(opnode.inputs.get(1));
                if (hop.getOpString().equals("b(+)") || hop.getOpString().equals("b(-)")) {
                    ans = new MMNode(m0, m1, SparsityEstimator.OpCode.PLUS);
                } else if (hop.getOpString().equals("b(*)") || hop.getOpString().equals("b(/)")) {
                    ans = new MMNode(m0, m1, SparsityEstimator.OpCode.MULT);
                } else {
                    //   System.out.println("un handled hop type: " + hop.getOpString());
                }
            }
        } else if (hop instanceof NaryOp || hop instanceof TernaryOp) {
            MMNode m0 = getMmnode(opnode.inputs.get(0));
            MMNode m1 = getMmnode(opnode.inputs.get(1));
            ans = new MMNode(m0, m1, SparsityEstimator.OpCode.PLUS);
        }
        if (ans == null) {
            System.out.println("mmnode == null");
            System.exit(-1);
        }
//        LOG.info("end add opnode to mmnode");
        return ans;
    }

    int getPartition(OperatorNode opNode) {
//        LOG.info("start get partition");
        String name = opNode.hops.get(0).getName();
        if (opNode.inputs.size() == 0 && sec.containsVariable(name)) {
            JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = sec.getBinaryMatrixBlockRDDHandleForVariable(name);
            LOG.info("get partitions by rdd: " + name + ", partitions: " + in1.getNumPartitions());
            opNode.partitionNumber = in1.getNumPartitions();
        } else {
            DataCharacteristics dc = getDC(opNode);
            int p1 = SparkUtils.getNumPreferredPartitions(dc);
            LOG.info("get partitions by util" + ", partitions: " + p1 + ", dc: " + dc);
            opNode.partitionNumber = p1;
        }
        LOG.info(Explain.explain(opNode.hops.get(0)));
//        LOG.info("end get partition");
        return opNode.partitionNumber;
    }

    public static double CPMM_INTERN_SPARSITY = -1;

    double getMmInternSparsity(OperatorNode opNode, long c1, double sp1, double sp2, long partJoin) {
//        LOG.info("start get mm intern sparsity");
        assert opNode.mmNode != null;
//        if (opNode.mmNode.mm_intern_sparsity >= 0) {
//            LOG.info("end get mm intern sparsity");
//            return opNode.mmNode.mm_intern_sparsity;
//        }


        long blocks = (long) Math.ceil((double) c1 / defaultBlockSize);
        long layerNum1 = (blocks / partJoin) * defaultBlockSize;
        long parNum2 = blocks % partJoin;
        long parNum1 = partJoin - parNum2;
        long layerNum2 = layerNum1 + defaultBlockSize;
        LOG.info("getMmInternSparsity");
        LOG.info("partJoin " + partJoin);
        LOG.info("sp1 " + sp1);
        LOG.info("sp2 " + sp2);
        LOG.info("blocks " + blocks);
        LOG.info("layerNum1 " + layerNum1);
        LOG.info("parNum1 " + parNum1);
        LOG.info("layerNum2 " + layerNum2);
        LOG.info("parNum2 " + parNum2);
        double sp;
        if (useMncEstimator) {
            LOG.info("mnc");
            MMNode mn0 = opNode.inputs.get(0).mmNode;
            MMNode mn1 = opNode.inputs.get(1).mmNode;
            MMNode mmNode = new MMNode(mn0, mn1, SparsityEstimator.OpCode.MM);
            mncEstimator.estim(mmNode, false);
            sp = mncEstimator.estimInternSparsity(mmNode, layerNum1, parNum1, layerNum2, parNum2);
        } else {
            LOG.info("metadata");
            double tmp1 = 1.0 - Math.pow(1.0 - sp1 * sp2, layerNum1);
            double tmp2 = 1.0 - Math.pow(1.0 - sp1 * sp2, layerNum2);
            sp = (tmp1 * parNum1 / (parNum1 + parNum2)) + (tmp2 * parNum2 / (parNum1 + parNum2));
        }
        LOG.info("get mm intern sparsity " + sp);
//        opNode.mmNode.mm_intern_sparsity = sp;
//        LOG.info("end get mm intern sparsity");
        return sp;
    }

    DataCharacteristics getDC(OperatorNode opNode) {
//        LOG.info("start get dc");
        DataCharacteristics dc = null;
        MMNode mmNode = getMmnode(opNode);
//        Hop hop = opNode.hops.get(0);
//        if(Judge.isLeafMatrix(hop) && hop.getName().equals("h") ) {
//            System.out.println("h");
//        }
        try {
            if (useMncEstimator) {
                dc = mncEstimator.estim(mmNode, false);
            } else {
                dc = metadataEstimator.estim(mmNode);
            }
            if (mmNode.getDataCharacteristics().getBlocksize() < 0) {
                mmNode.getDataCharacteristics().setBlocksize(defaultBlockSize);
            }
            if (dc.getBlocksize() < 0) {
                dc.setBlocksize(defaultBlockSize);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
//        if (dc.getNonZeros()==-1) {
//            System.out.println("x");
//        }
        return dc;
    }

//    public  NodeCost getNodeCost(OperatorNode opnode) {
//        return NodeCost.ZERO();
//    }

    public NodeCost getNodeCost(OperatorNode opnode) {
//        LOG.info("start get node cost");
        long start = System.nanoTime();
        NodeCost ans;
        Hop hop = opnode.hops.get(0);
        // recurse top-bottom to determine weather a opnode is used by driver
        if (hop.optFindExecType() == LopProperties.ExecType.CP) {
            for (OperatorNode input : opnode.inputs) {
                input.isUsedByCp = true;
            }
        }
        for (int i = 0; i < opnode.hops.size(); i++) {
            if (opnode.hops.get(i).optFindExecType() == LopProperties.ExecType.CP) {
                opnode.isUsedByCp = true;
            }
        }
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
            ans = NodeCost.INF();
        }
        for (int i = 1; i < opnode.hops.size(); i++) {
            hop = opnode.hops.get(i);
            if (hop instanceof BinaryOp) {
                if (hop.isMatrix()) {
                    DataCharacteristics dc = getDC(opnode);
                    if (hop.optFindExecType() == LopProperties.ExecType.SPARK) {
                        ans.computeCost += dc.getNonZeros() * CpuSpeed / defaultWorkerNumber;
                        opnode.isSpark = true;
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
//        LOG.info("end get node cost");
        long end = System.nanoTime();
        estimateTime += end - start;
        return ans.clone();
    }


    NodeCost eMatrixMultiply(OperatorNode node, AggBinaryOp hop) {
//        LOG.info("start estimate matrix multiply");

        // 查缓存
        if (drange2multiplycostCache.containsKey(node.dRange)) {
            NodeCost nodeCost = drange2multiplycostCache.get(node.dRange);
//            LOG.info("end estimate matrix multiply");
            return nodeCost.clone();
        }

        // 计算
        estimate_matrix_multiply_counter++;
        DataCharacteristics dc1 = getDC(node.inputs.get(0));
        DataCharacteristics dc2 = getDC(node.inputs.get(1));
        DataCharacteristics dc3 = getDC(node);
        NodeCost ans = null;
        if (hop.optFindExecType() == LopProperties.ExecType.SPARK) {
            hop.constructLops();
            AggBinaryOp.MMultMethod method = hop.getMMultMethod();
//            method = AggBinaryOp.MMultMethod.CPMM;
            node.method = method;
            switch (method) {
                case MAPMM_R:
                case MAPMM_L:
                    ans = eMapMM(node, hop, method, dc1, dc2, dc3);
                    break;
                case RMM:
                    ans = eRMM(node, hop, dc1, dc2, dc3);
                    break;
                case MAPMM_CHAIN:
                    node.isXtXv = true;
                    ans = eMapMMChain(node, hop);
                    break;
                case ZIPMM:
                    ans = eZipMM(node, hop, dc1, dc2, dc3);
                    break;
                case TSMM:
                    ans = eTSMM(node, hop, dc1, dc2, dc3);
                    break;
                case CPMM:
                default:
                    ans = eCPMM(node, hop, dc1, dc2, dc3);
            }
        } else { // LopProperties.ExecType.CP
            double computeCost = CpuSpeed * 3 * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc2.getSparsity();
            ans = new NodeCost(0, 0, computeCost, 0);
        }

        // 更新缓存
        // todo: update co-cse dranges

        if (useCommonCostCache && dRangeDisjointSet.exist(node.dRange) ) {
            DRange dRange1 = dRangeDisjointSet.find(node.dRange);
            for (DRange dRange2 : dRangeDisjointSet.elements(node.dRange)) {
                DRange targetDrange;
                if (dRange2.cseRangeTransposeType.equals(dRange1.cseRangeTransposeType)) {
                    targetDrange = dRange2;
                } else {
                    targetDrange = dRange2.revverse(); //
                }
                if (!drange2multiplycostCache.containsKey(targetDrange)) {
                    drange2multiplycostCache.put(targetDrange, ans.clone());
                }
//                    if (drange2multiplycostCache.containsKey(targetDrange)) {
//                        NodeCost cost2 = drange2multiplycostCache.get(targetDrange);
//                        if ( Math.abs(ans.getSummary()-cost2.getSummary())>1e4 ) {
//                            System.out.println("x");
//                        }
//                    }
            }
        } else {
            if (!drange2multiplycostCache.containsKey(node.dRange)) {
                drange2multiplycostCache.put(node.dRange, ans.clone());
            }
        }
        //        LOG.info("end estimate matrix multiply");
        return ans;
    }


    public static void main(String[] args) {


        DataCharacteristics dc1_meta = new MatrixCharacteristics(58400000, 14955, 1000, 2277596265l);
        DataCharacteristics dc1_t_meta = new MatrixCharacteristics(14955, 58400000, 1000, 2277596265l);

        DataCharacteristics dc2_meta = new MatrixCharacteristics(14955, 1, 1000, 9454);
        DataCharacteristics dc3_meta = new MatrixCharacteristics(58400000, 1, 1000, 58400000);

        DataCharacteristics dc1_mnc = new MatrixCharacteristics(58400000, 14955, 1000, 2277596265l);
        DataCharacteristics dc1_t_mnc = new MatrixCharacteristics(14955, 58400000, 1000, 2277596265l);

        DataCharacteristics dc2_mnc = new MatrixCharacteristics(14955, 1, 1000, 14955);
        DataCharacteristics dc3_mnc = new MatrixCharacteristics(58400000, 1, 1000, 58400000);

        DataCharacteristics dc_ata = new MatrixCharacteristics(14955, 14955, 1000, 223652025);


        OperatorNode node = new OperatorNode();
        node.isTranspose = false;
        MMShowCostFlag = true;
        NodeCostEstimator estimator = new NodeCostEstimator(null);
        useMncEstimator = false;
        estimator.eCPMM(node, null, dc1_t_meta, dc1_meta, dc_ata);
        useMncEstimator = true;
        estimator.eCPMM(node, null, dc1_t_mnc, dc1_mnc, dc_ata);


//        estimator.eMapMM(node,null, AggBinaryOp.MMultMethod.MAPMM_R,dc1_meta,dc2_meta,dc3_meta);
//        estimator.eMapMM(node,null, AggBinaryOp.MMultMethod.MAPMM_R,dc1_mnc,dc2_mnc,dc3_mnc);

//        long dim1 = 58400000;
//        long dim2 = 8692;
//        long nnz = 2277598765l;

//        DataOp a = new DataOp("a", Types.DataType.MATRIX, Types.ValueType.FP64,
//                Types.OpOpData.TRANSIENTREAD, "", dim1, dim2, nnz, 1000);
//
//        Hop b = HopRewriteUtils.createTranspose(a);
//
//        AggBinaryOp c = HopRewriteUtils.createMatrixMultiply(b, a);
//
//        DataCharacteristics dc1 = b.getDataCharacteristics();
//        DataCharacteristics dc2 = a.getDataCharacteristics();
//        DataCharacteristics dc3 = c.getDataCharacteristics();
//
//        System.out.println(dc1);
//        System.out.println(dc2);
//        System.out.println(dc3);
//
//
//        NodeCostEstimator costEstimator = new NodeCostEstimator();
//        NodeCost costtsmm = costEstimator.eTSMM(c, dc1, dc2, dc3);
////        double costts2 =  costEstimator.eTSMM(c,dc2,dc1,dc3);
//        NodeCost costcp = costEstimator.eCPMM(c, dc1, dc2, dc3);
//        NodeCost costr = costEstimator.eRMM(c, dc1, dc2, dc3);
//        NodeCost costzip = costEstimator.eZipMM(c, dc1, dc2, dc3);
//        OperatorNode node = new OperatorNode();
//        NodeCost costmapmm = costEstimator.eMapMM(node, c, AggBinaryOp.MMultMethod.MAPMM_R, dc1, dc2, dc3);
//        double controlProgramMM = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols();
//
//        c.constructLops();
//        AggBinaryOp.MMultMethod method = c.getMMultMethod();
//
//        System.out.println("method = " + method);
//        System.out.println("reducers = " + getNumberReducers());
//        System.out.println("cost tsmm = " + costtsmm);
//        System.out.println("cost cpmm = " + costcp);
//        System.out.println("cost rmm = " + costr);
//        System.out.println("cost zipmm = " + costzip);
//        System.out.println("cost mapmm = " + costmapmm);
//        System.out.println("cost control program mm = " + controlProgramMM);

    }


    public static double sparkmmComputeCost(DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        double result = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc2.getSparsity() * 3 / defaultWorkerNumber;
        return result;
    }


    NodeCost eMapMM(OperatorNode node, AggBinaryOp hop, AggBinaryOp.MMultMethod method,
                    DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        LOG.info("begin<<<  eMapMM");
        double broadcastCost;
        double computeCost;
        double shuffleCost = 0;
        double foldCost = 0;

        boolean isLeft = (!node.isTranspose && method == AggBinaryOp.MMultMethod.MAPMM_L)
                || (node.isTranspose && method == AggBinaryOp.MMultMethod.MAPMM_R);
        LOG.info("isLeft: " + isLeft);
        long partitionRdd;
        double br = Math.ceil(Math.log(defaultWorkerNumber));

        if (isLeft) {
            partitionRdd = getPartition(node.inputs.get(1));
            broadcastCost = BroadCaseSpeed * matrixSize(dc1) * br;
        } else {
            partitionRdd = getPartition(node.inputs.get(0));
            broadcastCost = BroadCaseSpeed * matrixSize(dc2) * br;
        }
        LOG.info("partitionRdd: " + partitionRdd);
        LOG.info("br: " + br);
        LOG.info("broadcastCost: " + broadcastCost);

        //double middle_sparsity = getMmInternSparsity(node, dc1.getCols(), dc1.getSparsity(), dc2.getSparsity(), partitionRdd);
        double middle_sparsity = dc3.getSparsity();

        AggBinaryOp.SparkAggType aggType = PartitionUtil.getSparkMMAggregationType(dc3);

        LOG.info("middle_sparsity: " + middle_sparsity);
        LOG.info("aggType: " + aggType);


        if (!PartitionUtil.requiresAggregation(isLeft, dc1, dc2)) { // AggBinaryOp.SparkAggType.NONE
            shuffleCost = 0;
            LOG.info("SparkAggType.NONE");
        } else {
            if (aggType == AggBinaryOp.SparkAggType.SINGLE_BLOCK) {
                LOG.info("SparkAggType.SINGLE_BLOCK");
                double middle_size = matrixSize(dc3.getRows(), dc3.getCols(), middle_sparsity);
                foldCost = BroadCaseSpeed * middle_size * partitionRdd;
                LOG.info("middle_size: " + middle_size);
            } else { //AggBinaryOp.SparkAggType.MULTI_BLOCK
                LOG.info("SparkAggType.MULTI_BLOCK");
                double matrix_block_size; //= matrixSize(defaultBlockSize, defaultBlockSize, middle_sparsity);
                double matrix_block_number_per_partition;
                if (isLeft) {
                    double avg_matrix_block_number_per_partition = (double) matrixBlocks(dc2) / partitionRdd;
                    LOG.info("avg_matrix_block_number_per_partition: " + avg_matrix_block_number_per_partition);
                    matrix_block_number_per_partition = box(colBlocks(dc2), avg_matrix_block_number_per_partition);
                    long cols = (long) Math.ceil(matrix_block_number_per_partition * defaultBlockSize);
                    LOG.info("middle rows: " + dc1.getRows());
                    LOG.info("middle cols: " + cols);
                    matrix_block_size = matrixSize(dc1.getRows(), cols, middle_sparsity);
                } else {
                    double avg_matrix_block_number_per_partition = (double) matrixBlocks(dc1) / partitionRdd;
                    LOG.info("avg_matrix_block_number_per_partition: " + avg_matrix_block_number_per_partition);
                    matrix_block_number_per_partition = box(rowBlocks(dc1), avg_matrix_block_number_per_partition);
                    long rows = (long) Math.ceil(matrix_block_number_per_partition * defaultBlockSize);
                    LOG.info("middle rows: " + rows);
                    LOG.info("middle cols: " + dc2.getCols());
                    matrix_block_size = matrixSize(rows, dc2.getCols(), middle_sparsity);
                }
                LOG.info("matrix_block_size: " + matrix_block_size);
                LOG.info("matrix_block_number_per_partition: " + matrix_block_number_per_partition);
                shuffleCost = ShuffleSpeed * matrix_block_size * partitionRdd / defaultWorkerNumber;
                node.isSpark = true;
            }
        }
//        shuffleCost = ShuffleSpeed * matrixSize(dc3) * partitionRdd / w2;

        computeCost = sparkmmComputeCost(dc1, dc2, dc3);

        if (MMShowCostFlag) {
//            LOG.info("begin<<< ");
            LOG.info("mapmm broadcast cost = " + broadcastCost);
            LOG.info("mapmm compute cost = " + computeCost);
            LOG.info("mapmm shuffle cost = " + shuffleCost);
            LOG.info("mapmm fold cost = " + foldCost);
            LOG.info("end>>>  eMapMM");
        }
//        return computeCost + shuffleCost + broadcastCost + collectCost;
        return new NodeCost(shuffleCost, broadcastCost, computeCost, foldCost);
    }


    NodeCost eCPMM(OperatorNode operatorNode, AggBinaryOp hop,
                   DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        LOG.info("begin<<<  eCPMM");
        int numPreferred = CpmmSPInstruction.getPreferredParJoin(dc1, dc2,
                getPartition(operatorNode.inputs.get(0)),
                getPartition(operatorNode.inputs.get(1)));
        int numMax = (int) Math.ceil((double) dc1.getCols() / defaultBlockSize);
        int numPartJoin = Math.min(numMax, numPreferred);
        LOG.info("rdd par 0 = " + getPartition(operatorNode.inputs.get(0)));
        LOG.info("rdd par 1 = " + getPartition(operatorNode.inputs.get(1)));
        LOG.info("numPrefered = " + numPreferred);
        LOG.info("numMax = " + numMax);
        LOG.info("numPartJoin = " + numPartJoin);
        double computeCost = sparkmmComputeCost(dc1, dc2, dc3);
        double joinCost1 = JoinSpeed * (matrixSize(dc1) + matrixSize(dc2)) / defaultWorkerNumber;
        double middle_sparsity = getMmInternSparsity(operatorNode, dc1.getCols(), dc1.getSparsity(), dc2.getSparsity(), numPartJoin);
        double shuffleCost2 = 0;
        double foldCost = 0;
        double middle_size;
        AggBinaryOp.SparkAggType aggType = PartitionUtil.getSparkMMAggregationType(dc3);
        middle_size = matrixSize(dc3.getRows(), dc3.getCols(), middle_sparsity);
        if (aggType == AggBinaryOp.SparkAggType.SINGLE_BLOCK) {
            foldCost = BroadCaseSpeed * middle_size * numPartJoin;
        } else {
            shuffleCost2 = ShuffleSpeed * middle_size * numPartJoin / defaultWorkerNumber;
            operatorNode.isSpark = true;
        }
        if (MMShowCostFlag) {
//            LOG.info("begin<<< ");
            LOG.info("dcA: " + dc1);
            LOG.info("dcB: " + dc2);
            LOG.info("dcC: " + dc3);
            LOG.info("numPartJoin: " + numPartJoin);
            LOG.info("sparsity of middle: " + middle_sparsity);
            LOG.info("matrix size of middle: " + middle_size);
            LOG.info("aggType: " + aggType);
            LOG.info("cpmm join cost = " + joinCost1);
            LOG.info("cpmm shuffle cost = " + shuffleCost2);
            LOG.info("cpmm compute cost = " + computeCost);
            LOG.info("end>>>  eCPMM");
        }
//        return shuffleCost1 + shuffleCost2 + computeCost;
        return new NodeCost(shuffleCost2, 0, computeCost, foldCost, joinCost1);
    }

    NodeCost eTSMM(OperatorNode node, AggBinaryOp hop,
                   DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        LOG.info("begin<<<  eTSMM");
        long partition = getPartition(node.inputs.get(0));
        double intern_sp = getMmInternSparsity(node, dc1.getCols(), dc1.getSparsity(), dc2.getSparsity(), partition);
        double computeCost = CpuSpeed * 3 * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc1.getSparsity() / defaultWorkerNumber;
        double matrix_size = matrixSize(dc3.getRows(), dc3.getCols(), intern_sp);
        double foldCost = ShuffleSpeed * matrix_size * partition;
        LOG.info("partition: " + partition);
        LOG.info("intern_sp: " + intern_sp);
        LOG.info("computeCost: " + computeCost);
        LOG.info("matrix_size: " + matrix_size);
        LOG.info("foldCost: " + foldCost);
        LOG.info("end>>>  eTSMM");
        return new NodeCost(0, 0, computeCost, foldCost);
    }

    NodeCost eRMM(OperatorNode node, AggBinaryOp hop,
                  DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        LOG.info("begin<<<  RMM");
        long m1_nrb = rowBlocks(dc1);
        LOG.info("m1_nrb: " + m1_nrb);
        long m2_ncb = colBlocks(dc2);
        LOG.info("m2_ncb: " + m2_ncb);
        long k = rowBlocks(dc2);
        LOG.info("k: " + k);
        double joinCost1 = ShuffleSpeed * (m2_ncb * matrixSize(dc1) + m1_nrb * matrixSize(dc2)) / defaultWorkerNumber;
        double shuffleCost2 = ShuffleSpeed * (matrixSize(dc3) * k) / defaultWorkerNumber;
        double computeCost = sparkmmComputeCost(dc1, dc2, dc3);
        node.isSpark = true;
        if (MMShowCostFlag) {
//            LOG.info("begin<<< ");
            LOG.info("rmm join cost 1 = " + joinCost1);
            LOG.info("rmm shuffle cost 2 = " + shuffleCost2);
            LOG.info("rmm compute cost = " + computeCost);
            LOG.info("end>>>  RMM");
        }
//        return joinCost1 + shuffleCost2 + computeCost;
        return new NodeCost(shuffleCost2, 0, computeCost, 0, joinCost1);
    }

    NodeCost eMapMMChain(OperatorNode node, AggBinaryOp hop) {
        LOG.info("begin<<<  eMapMMChain");
        DataCharacteristics dc1, dc2, dc3;
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
            DataCharacteristics dcXt = getDC(nodeXt);
            DataCharacteristics dcVt = getDC(nodeVt);
            dc1 = new MatrixCharacteristics(dcXt.getCols(), dcXt.getRows(), defaultBlockSize, dcXt.getNonZeros());
            dc2 = new MatrixCharacteristics(dcVt.getCols(), dcVt.getRows(), defaultBlockSize, dcVt.getNonZeros());
        }
        LOG.info("dc1: " + dc1);
        LOG.info("dc2: " + dc2);
        LOG.info("dc3: " + dc3);
        return eMapMMChain(node, hop, dc1, dc2, dc3);
    }


    NodeCost eMapMMChain(OperatorNode node, AggBinaryOp hop,
                         DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        long partition = getPartition(node.inputs.get(0));
        LOG.info("partition: " + partition);
        double inter_sp = getMmInternSparsity(node, dc1.getCols(), dc1.getSparsity(), dc1.getSparsity(), partition);
        LOG.info("inter_sp: " + inter_sp);
        long br = (long) Math.ceil(Math.log(defaultWorkerNumber));
        LOG.info("br: " + br);
        double computeCost = CpuSpeed * 3 * dc1.getRows() * dc1.getCols() * dc2.getCols() * (dc1.getSparsity() * dc2.getSparsity() + dc1.getSparsity()) / defaultWorkerNumber;
        LOG.info("computeCost: " + computeCost);
        double broadcastCost = BroadCaseSpeed * matrixSize(dc2) * br;
        LOG.info("broadcastCost: " + broadcastCost);
        double matrix_size = matrixSize(dc3.getRows(), dc3.getCols(), inter_sp);
        LOG.info("matrix_size: " + matrix_size);
        double foldCost = BroadCaseSpeed * matrix_size * partition;
        LOG.info("foldCost: " + foldCost);
        //        return computeCost + foldCost + broadcastCost;
        LOG.info("end>>>  eMapMMChain");
        return new NodeCost(0, broadcastCost, computeCost, foldCost);
    }

    NodeCost eZipMM(OperatorNode node, AggBinaryOp hop,
                    DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        LOG.info("begin<<<  eZipMM");
        long partition = getPartition(node.inputs.get(0));
        LOG.info("partition: " + partition);
        double computeCost = CpuSpeed * 3 * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc2.getSparsity() / defaultWorkerNumber;
        LOG.info("computeCost: " + computeCost);
        double joinCost = ShuffleSpeed * (matrixSize(dc1) + matrixSize(dc2)) / defaultWorkerNumber;
        LOG.info("joinCost: " + joinCost);
        double inter_sp = getMmInternSparsity(node, dc1.getCols(), dc1.getSparsity(), dc2.getSparsity(), partition);
        LOG.info("inter_sp: " + inter_sp);
        double matrix_size = matrixSize(dc3.getRows(), dc3.getCols(), inter_sp);
        LOG.info("matrix_size: " + matrix_size);
        double foldCost = BroadCaseSpeed * matrix_size * partition;
        LOG.info("foldCost: " + foldCost);
        LOG.info("end>>>  eZipMM");
//        return computeCost + foldCost;
        return new NodeCost(0, 0, computeCost, foldCost, joinCost);
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
            operatorNode.isSpark = true;
        }
        if (hop.getOpString().equals("b(*)") || hop.getOpString().equals("b(/)")) {
            cost *= 2;
        }
        return new NodeCost(0, 0, cost, 0);
    }

    NodeCost eBinaryMatrixMatrix(OperatorNode operatorNode, Hop hop) {
        double nnz = 0, matrix_size = 0;
        for (int i = 0; i < operatorNode.inputs.size(); i++) {
            DataCharacteristics dci = getDC(operatorNode.inputs.get(i));
            nnz += dci.getNonZeros();
            matrix_size += matrixSize(dci);
        }
        double computeCost;
        double joinCost = 0;
        if (hop.optFindExecType() == LopProperties.ExecType.SPARK) {
            DataCharacteristics dc0 = getDC(operatorNode.inputs.get(0));
            computeCost = CpuSpeed * nnz / defaultWorkerNumber;
            joinCost = ShuffleSpeed * matrix_size / defaultWorkerNumber;
            operatorNode.isSpark = true;
        } else {
            computeCost = CpuSpeed * nnz;
        }
        if (hop.getOpString().equals("b(*)")) {
            computeCost *= 2;
        } else if (hop.getOpString().equals("b(/)")) {
            computeCost *= 4;
        }
        return new NodeCost(0, 0, computeCost, 0, joinCost);
//        return computeCost + joinCost;
    }

    double eCollectCost(OperatorNode opnode) {
        double cost = 0;
        if (opnode.isUsedByCp && opnode.isSpark) {
            opnode.shouldCollect = true;
            DataCharacteristics dc = getDC(opnode);
            cost += BroadCaseSpeed * matrixSize(dc);
        }
        return cost;
    }


}
