package org.apache.sysds.hops.rewrite.dfp.costmodel;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.*;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.lops.MMTSJ;
import org.apache.sysds.lops.MapMult;
import org.apache.sysds.lops.MapMultChain;
import org.apache.sysds.runtime.controlprogram.*;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.instructions.cp.*;
import org.apache.sysds.runtime.instructions.spark.*;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;


import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch.*;

public class FakeCostEstimator2 {
    protected static final Log LOG = LogFactory.getLog(FakeCostEstimator2.class.getName());

    public static double CpuSpeed = 1.0;
    public static double ShuffleSpeed = 5.0;
    public static double BroadCaseSpeed = 3.0;

    public static long defaultBlockSize = 1000;
    public static ExecutionContext ec = null;

    //            private static SparsityEstimator estimator = new EstimatorBasicAvg();
    private static EstimatorMatrixHistogram estimator = new EstimatorMatrixHistogram();

    //public static double miniumCostBoundery = Double.MAX_VALUE;

    //////////////////////
    // varius of cost

    public static double shuffleCostSummary = 0;
    public static double broadcastCostSummary = 0;
    public static double computeCostSummary = 0;
    public static double collectCostSummary = 0;

    //
    //////////////////////

    public static double estimate(Program rtprog) {

        shuffleCostSummary = 0;
        broadcastCostSummary = 0;
        computeCostSummary = 0;
        collectCostSummary = 0;

        //  System.out.println(name2MMNode.keySet());
        double cost = 0;
        //  LOG.debug(Explain.explain(rtprog));
        for (ProgramBlock pb : rtprog.getProgramBlocks()) {
            try {
                double delta = rEstimate(pb);
                //   LOG.debug("pb cost = "+delta);
                if (delta < 0 || delta >= Double.MAX_VALUE) {
                    //      LOG.error("delta error");
                    cost = Double.MAX_VALUE;
                    break;
                }
                cost += delta;
//                if (cost > miniumCostBoundery) {
//                    cost = Double.MAX_VALUE;
//                    break;
//                }
            } catch (Exception e) {
                //  LOG.error("delta error");
                e.printStackTrace();
                cost = Double.MAX_VALUE;
                break;
            }
        }
        //  LOG.error("for end");
        name2MMNode.entrySet().removeIf(e -> e.getKey().startsWith("_Var") || e.getKey().startsWith("_mVar"));

        //   LOG.info("Name Size = "+name2MMNode.size());
        //     LOG.info("Names = "+name2MMNode.keySet());
        //  LOG.debug("rtprog cost = "+cost);
        //  return cost;

        return cost;
    }

    public static void cleanUnusedMMNode() {
        name2MMNode.entrySet().removeIf(e -> e.getKey().startsWith("_Var") || e.getKey().startsWith("_mVar") || e.getKey().startsWith("_conVar"));
    }

    public static void cleanUnusedScratchMMNode() {
        name2MMNode.entrySet().removeIf(e -> e.getKey().startsWith("_Var") || e.getKey().startsWith("_mVar") || e.getKey().startsWith("_conVar") || e.getKey().startsWith("_sVar"));
    }

    private static double rEstimate(ProgramBlock pb) throws Exception {
        double cost = 0;
        double delta;
        if (pb instanceof IfProgramBlock) {
            IfProgramBlock ipb = (IfProgramBlock) pb;
            delta = processInstructions(ipb.getPredicate());
            if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
            cost += delta;
            for (ProgramBlock p : ipb.getChildBlocksIfBody()) {
                delta = rEstimate(p);
                if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
                cost += delta;
            }
            for (ProgramBlock p : ipb.getChildBlocksElseBody()) {
                delta = rEstimate(p);
                if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
                cost += delta;
            }
        } else if (pb instanceof WhileProgramBlock) {
            WhileProgramBlock wpb = (WhileProgramBlock) pb;
            delta = processInstructions(wpb.getPredicate());
            if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
            cost += delta;
            for (ProgramBlock p : wpb.getChildBlocks()) {
                delta = rEstimate(p);
                if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
                cost += delta;
            }
        } else if (pb instanceof BasicProgramBlock) {
            BasicProgramBlock bpb = (BasicProgramBlock) pb;
            delta = processInstructions(bpb.getInstructions());
            if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
            cost += delta;
        }
        return cost;
    }


//    private static double rEstimate(ArrayList<Instruction> instructions) throws Exception {
////        System.out.println("<<<explain instructions");
////        printInstruction(instructions, 0);
////        System.out.println("explain instructions>>>");
//        // step 1. according to Instructions, build MMNode Tree and topology rank
//        // step 2. traverse all the nodes by topo rank, get the operator type and sparsity of each node
//        // step 3. traverse all the nodes by topo rank , calculate the cost of each node
//        names = new ArrayList<>();
//        double cost = 0;
//        cost = processInstructions(instructions);
//        //   System.out.println("Summary Cost = " + cost);
////        System.out.println("name size = " + name2MMNode.size());
////        System.out.println(name2MMNode.keySet());
////        System.out.println("# names:");
////        for (String name : names) {
////            MMNode mmNode = null;
////            mmNode = getMMNode(name);
////            System.out.println("# " + name + ((mmNode != null) ? (" " + mmNode) : ""));
////        }
////        if (map.containsKey("_mVar1515")) {
////            System.out.println("x");
////        }
//        return cost;
//    }

    private static class Node {
        MMNode mmNode;
        Types.ExecType execType;

        public Node(MMNode mmNode, Types.ExecType execType) {
            this.mmNode = mmNode;
            this.execType = execType;
        }
    }


    private static HashMap<String, Node> name2MMNode = new HashMap<>();
    // private static ArrayList<String> names = new ArrayList<>();
    private static HashMap<String, DataCharacteristics> name2DataCharacteristics = new HashMap<>();

    private static Types.ExecType getExecType(String name) {
        if (name2MMNode.containsKey(name)) return name2MMNode.get(name).execType;
        else return null;
    }


    ////////////////////////////////////

    private static HashMap<Triple<MMNode, MMNode, SparsityEstimator.OpCode>, MMNode> binaryOpMmnode = new HashMap<>();
    private static HashMap<Pair<MMNode, SparsityEstimator.OpCode>, MMNode> unaryOpMmnode = new HashMap<>();


    private static MMNode createMMNode(DataCharacteristics dc) {
        MMNode mmNode = new MMNode(dc);
        if (MMShowCostFlag)  LOG.info("create new mmnode by dc " + mmNode.id);
        return mmNode;
    }

    private static MMNode createMMNode(MMNode left, MMNode right, SparsityEstimator.OpCode op) {
        Triple<MMNode, MMNode, SparsityEstimator.OpCode> key = Triple.of(left, right, op);
        if (binaryOpMmnode.containsKey(key)) {
            MMNode mmNode = binaryOpMmnode.get(key);
            if (MMShowCostFlag)  LOG.info("reuse old mmnode by triple " + left.id + " " + op + " " + right.id + " -> " + mmNode.id);
            return mmNode;
        } else {
            MMNode mmNode = new MMNode(left, right, op);
            if (MMShowCostFlag)  LOG.info("create new mmnode by triple " + left.id + " " + op + " " + right.id + " -> " + mmNode.id);
            binaryOpMmnode.put(key, mmNode);
            return mmNode;
        }
    }

    private static MMNode createMMNode(MMNode left, SparsityEstimator.OpCode op) {
        Pair<MMNode, SparsityEstimator.OpCode> key = Pair.of(left, op);
        if (unaryOpMmnode.containsKey(key)) {
            MMNode mmNode = unaryOpMmnode.get(key);
            if (MMShowCostFlag)   LOG.info("reuse old mmnode by pair " + left.id + " " + op + " -> " + mmNode.id);
            return mmNode;
        } else {
            MMNode mmNode = new MMNode(left, op);
            if (MMShowCostFlag)   LOG.info("create new mmnode by pair " + left.id + " " + op + " -> " + mmNode.id);
            unaryOpMmnode.put(key, mmNode);
            return mmNode;
        }
    }


    /////////////////////////////////////////

    private static MMNode getMMNode(String name) {
        //   LOG.trace("get mmnode " + name);
        // 先不获取matrix block, 直接用data characristic
        MMNode ans = null;
        if (name == null || name.startsWith("_Var")) {
            ans = null;
        } else if (name2MMNode.containsKey(name)) {
            ans = name2MMNode.get(name).mmNode;
            if (MMShowCostFlag) LOG.info("get mmnode by old name " + name + " -> " + ans.id);
        } else if (ec != null && ec.containsVariable(name)) {
            try {
                Data data = ec.getVariable(name);
                if (data.getDataType() == Types.DataType.MATRIX) {
                    //     System.out.println("metadata " + name + " " + data.getMetaData());
                    DataCharacteristics characteristics = data.getMetaData().getDataCharacteristics();
                    MMNode mmNode = createMMNode(characteristics);
                    if (estimator instanceof EstimatorMatrixHistogram) {
                        DistributedScratch.ec = ec;
                        EstimatorMatrixHistogram.MatrixHistogram histogram = getMatrixHistogram(name);
                        if (histogram == null) {
                            characteristics.setNonZeros(characteristics.getRows() * characteristics.getCols());
                            histogram = createFullHistogram((int) characteristics.getRows(), (int) characteristics.getCols());
                        }
                        mmNode.setSynopsis(histogram);
                    }
                    setMMNode(name, mmNode, Types.ExecType.SPARK);
                    ans = mmNode;
                }
                if (MMShowCostFlag)   LOG.info("get mmnode by create " + name + " -> " + ans.id);
            } catch (Exception e) {
//                e.printStackTrace();
            }
        }
//            Data dat = ec.getVariables().get(name);
//            MatrixObject mobj = (MatrixObject) dat;
//            System.out.println( name + ": nnz=" + mobj.getNnz()+ " metadata=" + mobj.getMetaData());
//            if (!(dat instanceof MatrixObject))
//                throw new HopsException("Input '" + name + "' not a matrix: " + dat.getDataType());
//            try {
//                MatrixBlock matrixBlock = ((MatrixObject) dat).acquireReadAndRelease();
//                return createMMNode(matrixBlock);
//            } catch (Exception e) {
//                System.out.println("getMMNode "+name);
//                e.printStackTrace();
//            }
//        }
        if (MMShowCostFlag) {
            LOG.info("get mmnode " + name + " -> " + ans);
            if (ans == null && !name.startsWith("_Var"))
                LOG.error("get mmnode failed " + name);
        }
        return ans;
    }

    private static void setMMNode(String name, MMNode mmNode, Types.ExecType execType) {
        if (name2MMNode.containsKey(name)) return;
        //  LOG.trace("set mmnode " + name);
        if ("g".equals(name)) {
            if (MMShowCostFlag)   LOG.debug("set g = " + mmNode.getDataCharacteristics() + " " + execType);
        }
//         if (execType== Types.ExecType.SPARK)
        if (MMShowCostFlag)  LOG.info("set mmnode " + name + " -> " + mmNode);
        name2MMNode.put(name, new Node(mmNode, execType));
        //names.add(name);
    }

    private static DataCharacteristics getDC(MMNode mmNode) throws Exception {
        DataCharacteristics dc = estimator.estim(mmNode, false);
//        DataCharacteristics dc = estimator.estim(mmNode);
        if (dc.getRows() < 0 || dc.getCols() < 0) throw new Exception("dc<0");
        if (MMShowCostFlag) {
            LOG.info("dc " + dc);
        }
        return dc;
    }

    private static double processInstructions(ArrayList<Instruction> instructions) throws Exception {
        double cost = 0;
        for (Instruction inst : instructions) {
            //  System.out.println(inst);
            if (MMShowCostFlag)   LOG.trace(inst);
            double delta = 0;
            try {
                if (inst instanceof CPInstruction) {
                    delta = eCPInstruction((CPInstruction) inst);
                } else if (inst instanceof SPInstruction) {
                    delta = eSPInstruction((SPInstruction) inst);
                }
            } catch (Exception e) {
                //   System.out.println(e);
                e.printStackTrace();
                delta = Double.MAX_VALUE;
            }
//            if (cost > miniumCostBoundery) {
//                cost = Double.MAX_VALUE;
//                break;
//            }
            if (delta < 0 || delta >= Double.MAX_VALUE) {
                LOG.error("error instruction: " + inst.toString());
                return Double.MAX_VALUE;
            }
            //   LOG.trace(inst+" |||| delta = "+ delta);
            cost += delta;
        }
        //   LOG.debug("process cost " + cost);
        return cost;
    }

    //////////////////////////////
    //以下是处理各种instruction的函数

    private static double eCPInstruction(CPInstruction inst) throws Exception {
        double cost = 0;
        if (inst instanceof ComputationCPInstruction) {
            cost += eComputationCPInstruction((ComputationCPInstruction) inst);
        } else if (inst instanceof VariableCPInstruction) {
            cost += eVariableCPInstruction((VariableCPInstruction) inst);
        } else if (inst instanceof FunctionCallCPInstruction) {
            cost += eFunctionCallCPInstruction((FunctionCallCPInstruction) inst);
        } else if (inst instanceof BuiltinNaryCPInstruction) {
            cost += eBuiltinNaryCPInstruction((BuiltinNaryCPInstruction) inst);
        } else {
            throw new UnhandledOperatorException(inst);
        }
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eBuiltinNaryCPInstruction(BuiltinNaryCPInstruction inst) throws Exception {
        MMNode summary = getMMNode(inst.inputs[0].getName());
        for (int i = 1; i < inst.inputs.length; i++) {
            MMNode tmp = getMMNode(inst.inputs[i].getName());
            summary = createMMNode(summary, tmp, SparsityEstimator.OpCode.PLUS);
        }
        setMMNode(inst.output.getName(), summary, Types.ExecType.SPARK);
        DataCharacteristics dc = getDC(summary);
        double cost = CpuSpeed * dc.getNonZeros() * (inst.inputs.length - 1);
        computeCostSummary += cost;
        for (int i = 0; i < inst.inputs.length; i++) {
            cost += getCollectCost(inst.inputs[i].getName());
        }
        return cost;
    }

    private static double eComputationCPInstruction(ComputationCPInstruction inst) throws Exception {
        double cost = 0;
        if (inst instanceof UnaryCPInstruction) {
            cost += eUnaryCPInstruction((UnaryCPInstruction) inst);
        } else if (inst instanceof BinaryCPInstruction) {
            cost += eBinaryCPInstruction((BinaryCPInstruction) inst);
        } else if (inst instanceof TernaryCPInstruction) {
            cost += eTernaryCPInstruction((TernaryCPInstruction) inst);
        } else {
            throw new UnhandledOperatorException(inst);
        }
        return cost;
    }

    private static double eUnaryCPInstruction(UnaryCPInstruction inst) throws Exception {  // pass through
        //    System.out.println("UnaryCP " + inst.getOutput().getName());
        double cost = 0;
        if (inst instanceof MMChainCPInstruction) {
            cost += eMMChainCPInstruction((MMChainCPInstruction) inst);
        } else if (inst instanceof DataGenCPInstruction) {

        } else if (inst instanceof ReorgCPInstruction) {
            cost += eReorgCPInstruction((ReorgCPInstruction) inst);
        } else if (inst instanceof MMTSJCPInstruction) {
            cost += eMMTSJCPInstruction((MMTSJCPInstruction) inst);
        } else if (inst instanceof UnaryScalarCPInstruction) {

        } else {
            throw new UnhandledOperatorException(inst);
        }

        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eMMChainCPInstruction(MMChainCPInstruction inst) throws Exception {
        MMNode out;
        double cost = 0;
        if (inst.getMMChainType() == MapMultChain.ChainType.XtXv) {
            if (MMShowCostFlag) {
                LOG.info("begin<<< " + inst);
            }
            MMNode X = getMMNode(inst.input1.getName());
            DataCharacteristics dc1 = getDC(X);
            MMNode v = getMMNode(inst.input2.getName());
            MMNode Xv = createMMNode(X, v, SparsityEstimator.OpCode.MM);
            MMNode Xt = createMMNode(X, SparsityEstimator.OpCode.TRANS);
            out = createMMNode(Xt, Xv, SparsityEstimator.OpCode.MM);
            double d1m = dc1.getRows(), d1n = dc1.getCols(), d1s = dc1.getSparsity();
            boolean leftSparse = MatrixBlock.evalSparseFormatInMemory(dc1);
            if (!leftSparse)
                cost = CpuSpeed * (2 + 2) * ((double) d1m * d1n) / 2;
            else
                cost = CpuSpeed * (2 + 2) * ((double) d1m * d1n * d1s) / 2;
            computeCostSummary += cost;
            if (MMShowCostFlag) {
                LOG.info("end " + inst + " >>>");
            }
        } else {
            LOG.error(inst);
            throw new UnhandledOperatorException(inst);
        }
        setMMNode(inst.output.getName(), out, Types.ExecType.CP);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eReorgCPInstruction(ReorgCPInstruction inst) throws Exception {
//        System.out.println(inst.getOpcode());
        double cost = 0;
        if ("r'".equals(inst.getOpcode())) {
            MMNode in = getMMNode(inst.input1.getName());
            DataCharacteristics dc = getDC(in);
            boolean sparse = MatrixBlock.evalSparseFormatInMemory(dc);
            if (sparse) {
                cost = CpuSpeed * dc.getRows() * dc.getCols() * dc.getSparsity();
            } else {
                cost = CpuSpeed * dc.getRows() * dc.getCols();
            }
            computeCostSummary += cost;
            cost += getCollectCost(inst.input1.getName());

            MMNode out = createMMNode(in, SparsityEstimator.OpCode.TRANS);
            setMMNode(inst.output.getName(), out, Types.ExecType.CP);
        }
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eMMTSJCPInstruction(MMTSJCPInstruction inst) throws Exception { // tsmm
        MMNode in = getMMNode(inst.input1.getName());
        DataCharacteristics dcin = getDC(in);
        double d1m = dcin.getRows(), d1n = dcin.getCols(), d1s = dcin.getSparsity();
        boolean sparse = MatrixBlock.evalSparseFormatInMemory(dcin);
        MMNode tmp = createMMNode(in, SparsityEstimator.OpCode.TRANS);
        MMNode out = null;
        double cost = 0;
        if (inst.getMMTSJType() == MMTSJ.MMTSJType.LEFT) {
            out = createMMNode(tmp, in, SparsityEstimator.OpCode.MM);
            if (sparse) {
                cost = CpuSpeed * d1m * d1n * d1s * d1n / 2;
            } else {
                cost = CpuSpeed * d1m * d1n * d1s * d1n * d1s / 2;
            }
        } else if (inst.getMMTSJType() == MMTSJ.MMTSJType.RIGHT) {
            out = createMMNode(in, tmp, SparsityEstimator.OpCode.MM);
            if (sparse) {
                cost = CpuSpeed * d1m * d1n * d1s + d1m * d1n * d1s * d1n * d1s / 2;
            } else {
                cost = CpuSpeed * d1m * d1n * d1m / 2;
            }
        }
        computeCostSummary += cost;
        cost += getCollectCost(inst.input1.getName());

        setMMNode(inst.output.getName(), out, Types.ExecType.CP);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eBinaryCPInstruction(BinaryCPInstruction inst) throws Exception {
        double cost = 0;
        if (inst instanceof AggregateBinaryCPInstruction) { // matrix mult
            cost += eAggregateBinaryCPInstruction((AggregateBinaryCPInstruction) inst);
        } else if (inst instanceof BinaryScalarScalarCPInstruction) {// pass through
//            BinaryScalarScalarCPInstruction bcp = (BinaryScalarScalarCPInstruction) inst;
//            System.out.println("BinaryScalarScalarCP " + bcp.input1.getName() + " " + bcp.getOpcode() + " " + bcp.input2.getName() + " -> " + bcp.output.getName());
        } else if (inst instanceof BinaryMatrixMatrixCPInstruction) {// pass through
            cost += eBinaryMatrixMatrixCPInstruction((BinaryMatrixMatrixCPInstruction) inst);
        } else if (inst instanceof BinaryMatrixScalarCPInstruction) {
            cost += eBinaryMatrixScalarCPInstruction((BinaryMatrixScalarCPInstruction) inst);
        } else {
            throw new UnhandledOperatorException(inst);
        }
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eTernaryCPInstruction(TernaryCPInstruction inst) throws Exception { // Matrix Multiply
        //  System.out.println("TernaryCP " + inst.output.getName());
        double cost = 0;
        MMNode out = null;
        ArrayList<MMNode> inputs = new ArrayList<>();
        if (inst.input1.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input1.getName()));
        if (inst.input2.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input2.getName()));
        if (inst.input3.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input3.getName()));
        if (inputs.size() == 1) {
            out = inputs.get(0);
            cost += getCollectCost(inst.input1.getName());
        } else if (inputs.size() == 2) {
            out = createMMNode(inputs.get(0), inputs.get(1), SparsityEstimator.OpCode.MM);
            cost += getCollectCost(inst.input1.getName());
            cost += getCollectCost(inst.input2.getName());
        } else if (inputs.size() == 3) {
            MMNode tmp = createMMNode(inputs.get(0), inputs.get(1), SparsityEstimator.OpCode.MM);
            out = createMMNode(tmp, inputs.get(2), SparsityEstimator.OpCode.MM);
            cost += getCollectCost(inst.input1.getName());
            cost += getCollectCost(inst.input2.getName());
            cost += getCollectCost(inst.input3.getName());
        }
        double computeCost = CpuSpeed * 2 * inputs.get(0).getRows() * inputs.get(0).getCols();
        computeCostSummary += computeCost;
        cost += computeCost;
        try {
            DataCharacteristics dc = getDC(out);
            //   System.out.println("data characteristics " + inst.output.getName() + " " + dc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        setMMNode(inst.output.getName(), out, Types.ExecType.CP);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eAggregateBinaryCPInstruction(AggregateBinaryCPInstruction inst) throws Exception {
        //   System.out.println("AggregateBinaryCP " + inst.input1.getName() + " %*% " + inst.input2.getName() + " -> " + inst.output.getName());
        MMNode m1 = getMMNode(inst.input1.getName());
        MMNode m2 = getMMNode(inst.input2.getName());
        MMNode out = createMMNode(m1, m2, SparsityEstimator.OpCode.MM);

        double cost = 0;
        try {
            DataCharacteristics m1DC = getDC(m1);
            double d1m = m1DC.getRows(), d1n = m1DC.getCols(), d1s = m1DC.getSparsity();
            boolean leftSparse = MatrixBlock.evalSparseFormatInMemory(m1DC);
            DataCharacteristics m2DC = getDC(m2);
            double d2m = m2DC.getRows(), d2n = m2DC.getCols(), d2s = m2DC.getSparsity();
            boolean rightSparse = MatrixBlock.evalSparseFormatInMemory(m2DC);
            DataCharacteristics outDC = getDC(out);
            double d3m = outDC.getRows(), d3n = outDC.getCols(), d3s = outDC.getSparsity();

            if (!leftSparse && !rightSparse)
                cost = CpuSpeed * 2 * (d1m * d1n * ((d2n > 1) ? d1s : 1.0) * d2n) / 2;
            else if (!leftSparse && rightSparse)
                cost = CpuSpeed * 2 * (d1m * d1n * d1s * d2n * d2s) / 2;
            else if (leftSparse && !rightSparse)
                cost = CpuSpeed * 2 * (d1m * d1n * d1s * d2n) / 2;
            else //leftSparse && rightSparse
                cost = CpuSpeed * 2 * (d1m * d1n * d1s * d2n * d2s) / 2;

        } catch (Exception e) {
            e.printStackTrace();
        }
        computeCostSummary += cost;
        cost += getCollectCost(inst.input1.getName());
        cost += getCollectCost(inst.input2.getName());
        setMMNode(inst.output.getName(), out, Types.ExecType.CP);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eBinaryMatrixMatrixCPInstruction(BinaryMatrixMatrixCPInstruction inst) throws Exception {
        //  System.out.println("BinaryMatrixMatrixSP " + inst.input1.getName() + " " + inst.getOpcode() + " " + inst.input2.getName() + " -> " + inst.output.getName());
        MMNode m1 = getMMNode(inst.input1.getName());
        MMNode m2 = getMMNode(inst.input2.getName());
        MMNode out = null;
        double cost = 0;
        try {
            DataCharacteristics m1DC = getDC(m1);
            double d1m = m1DC.getRows(), d1n = m1DC.getCols(), d1s = m1DC.getSparsity();
            boolean leftSparse = MatrixBlock.evalSparseFormatInMemory(m1DC);
            DataCharacteristics m2DC = getDC(m2);
            double d2m = m2DC.getRows(), d2n = m2DC.getCols(), d2s = m2DC.getSparsity();
            boolean rightSparse = MatrixBlock.evalSparseFormatInMemory(m2DC);
            if ("*".equals(inst.getOpcode())) {
                out = createMMNode(m1, m2, SparsityEstimator.OpCode.MULT);
                DataCharacteristics outDC = getDC(out);
                double d3m = outDC.getRows(), d3n = outDC.getCols(), d3s = outDC.getSparsity();
                cost = CpuSpeed * d3m * d3n;
            } else {
                out = createMMNode(m1, m2, SparsityEstimator.OpCode.PLUS);
                DataCharacteristics outDC = getDC(out);
                double d3m = outDC.getRows(), d3n = outDC.getCols(), d3s = outDC.getSparsity();
                if (leftSparse || rightSparse) cost = CpuSpeed * (d1m * d1n * d1s + d2m * d2n * d2s);
                else cost = CpuSpeed * d3m * d3n;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        computeCostSummary += cost;
        cost += getCollectCost(inst.input1.getName());
        cost += getCollectCost(inst.input2.getName());
        setMMNode(inst.output.getName(), out, Types.ExecType.CP);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eBinaryMatrixScalarCPInstruction(BinaryMatrixScalarCPInstruction inst) throws Exception {
        // PLUT, MULT
        //  System.out.println("BinaryMatrixScalarSP " + inst.input1.getName() + " " + inst.getOpcode() + " " + inst.input2.getName() + " -> " + inst.output.getName());
        MMNode out = null;
        double cost = 0;
        if (inst.input1.getDataType() == Types.DataType.MATRIX) {
            out = getMMNode(inst.input1.getName());
            cost += getCollectCost(inst.input1.getName());
        } else {
            out = getMMNode(inst.input2.getName());
            cost += getCollectCost(inst.input2.getName());
        }
        double computeCost = 0;
        try {
            DataCharacteristics outDC = getDC(out);
            double d3m = outDC.getRows(), d3n = outDC.getCols(), d3s = outDC.getSparsity();
            boolean sparse = MatrixBlock.evalSparseFormatInMemory(outDC);
            if (sparse) {
                computeCost = CpuSpeed * d3m * d3n * d3s;
            } else {
                computeCost = CpuSpeed * d3m * d3n;
            } //  System.out.println("data characteristics " + inst.output.getName() + " " + dc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        computeCostSummary += computeCost;
        cost += computeCost;
        setMMNode(inst.output.getName(), out, Types.ExecType.CP);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eVariableCPInstruction(VariableCPInstruction inst) throws Exception {
        CPOperand input1 = inst.getInput1();
        CPOperand input2 = inst.getInput2();
        CPOperand output = inst.getOutput();
        if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.CreateVariable) {
            //   System.out.println("creatvar " + input1.getName());
//            MMNode mmNode = createMMNode(inst.getMetadata().getDataCharacteristics());
//            setMMNode(input1.getName(), mmNode, Types.ExecType.CP);
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.RemoveVariable) {
            //    System.out.println("rmvar " + input1.getName());
//            for (CPOperand operand : inst.getInputs()) {
//                name2MMNode.remove(operand.getName());
//            }
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.AssignVariable) {
            //    System.out.println("assignvar " + input1.getName() + " -> " + input2.getName());
            MMNode mmNode = getMMNode(input1.getName());
            setMMNode(input2.getName(), mmNode, Types.ExecType.CP);
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.CastAsScalarVariable) {
            //   System.out.println("castdts " + input1.getName() + " -> " + output.getName());
            //    map.put(output.getName(), null);
            //  names.add(output.getName());
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.CopyVariable) {
            //   LOG.debug("cpvar " + input1.getName() + " -> " + input2.getName());
            if (!input1.getName().startsWith("_Var")) {
                MMNode mmNode = getMMNode(input1.getName());
                if (mmNode != null)
                    setMMNode(input2.getName(), mmNode, Types.ExecType.CP);
            }
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.MoveVariable) {
            //  System.out.println("mvvar " + input1.getName() + " -> " + input2.getName());
            if (!input1.getName().startsWith("_Var")) {
                MMNode mmNode = getMMNode(input1.getName());
                setMMNode(input2.getName(), mmNode, Types.ExecType.CP);
                name2MMNode.remove(input1.getName());
            }
        } else {
            throw new UnhandledOperatorException(inst);
        }
        return 0;
    }

    private static double eFunctionCallCPInstruction(FunctionCallCPInstruction fcp) throws Exception {
//        System.out.print("FunctionCallCP " + fcp.getFunctionName());
//        System.out.print(" [");
//        for (CPOperand cpOperand : fcp.getInputs()) {
//            System.out.print(cpOperand.getName() + ",");
//        }
//        System.out.print("] -> [");
        for (String name : fcp.getBoundOutputParamNames()) {
//            System.out.print(name + ",");
            setMMNode(name, null, Types.ExecType.CP);
        }
//        System.out.println("]");
        return 0;
    }

    private static double eSPInstruction(SPInstruction inst) throws Exception {
        double cost = 0;
        if (inst instanceof ComputationSPInstruction) {
            cost += eComputationSPInstruction((ComputationSPInstruction) inst);
        } else if (inst instanceof MapmmChainSPInstruction) {
            cost += eMapmmChainSPInstruction((MapmmChainSPInstruction) inst);
        } else if (inst instanceof BuiltinNarySPInstruction) {
            cost += eBuiltinNarySPInstruction((BuiltinNarySPInstruction) inst);
        } else {
            throw new UnhandledOperatorException(inst);
        }
        return cost;
    }

    private static double eComputationSPInstruction(ComputationSPInstruction inst) throws Exception {
        double cost = 0;
        if (inst instanceof UnarySPInstruction) {
            cost += eUnarySPInstruction((UnarySPInstruction) inst);
        } else if (inst instanceof BinarySPInstruction) {
            cost += eBinarySPInstruction((BinarySPInstruction) inst);
        } else if (inst instanceof TernarySPInstruction) {
            cost += eTernarySPInstruction((TernarySPInstruction) inst);
        } else {
            throw new UnhandledOperatorException(inst);
        }
        return cost;
    }

    private static double eUnarySPInstruction(UnarySPInstruction inst) throws Exception {
        double cost = 0;
        if (inst instanceof TsmmSPInstruction) {// matrix multiply
            cost += eTsmmSPInstruction((TsmmSPInstruction) inst);
        } else if (inst instanceof ReorgSPInstruction) {   // TRcost
            cost += eReorgSPInstruction((ReorgSPInstruction) inst);
        } else if (inst instanceof CSVReblockSPInstruction) {

        } else if (inst instanceof CheckpointSPInstruction) {

        } else if (inst instanceof RandSPInstruction) {

        } else {
            throw new UnhandledOperatorException(inst);
        }
        return cost;
    }

    private static double eBuiltinNarySPInstruction(BuiltinNarySPInstruction inst) throws Exception {
        MMNode summary = getMMNode(inst.inputs[0].getName());
        for (int i = 1; i < inst.inputs.length; i++) {
            summary = createMMNode(summary, getMMNode(inst.inputs[i].getName()), SparsityEstimator.OpCode.PLUS);
        }
        setMMNode(inst.output.getName(), summary, Types.ExecType.SPARK);
        DataCharacteristics dc = getDC(summary);
        double cost = CpuSpeed * dc.getNonZeros() * (inst.inputs.length - 1) / SparkExecutionContext.getNumExecutors();
        computeCostSummary += cost;
        return cost;
    }

    private static double eTsmmSPInstruction(TsmmSPInstruction inst) throws Exception {
        // System.out.println("TSMM " + inst.input1.getName() + " -> " + inst.output.getName());
        MMNode in = getMMNode(inst.input1.getName());
        DataCharacteristics dcin = getDC(in);
        long d1m = dcin.getRows(), d1n = dcin.getCols();
        double d1s = dcin.getSparsity();
        boolean sparse = MatrixBlock.evalSparseFormatInMemory(dcin);
        MMNode tmp = createMMNode(in, SparsityEstimator.OpCode.TRANS);
        MMNode out = null;
        double computeCost = Double.MAX_VALUE;
        double shuffleCost = Double.MAX_VALUE;
        long executor = reducerNumber(d1m, d1n);
        if (inst.getMMTSJType() == MMTSJ.MMTSJType.LEFT) {
            out = createMMNode(tmp, in, SparsityEstimator.OpCode.MM);
            DataCharacteristics dcout = getDC(out);
            if (sparse) {
                computeCost = CpuSpeed * d1m * d1n * d1s * d1n / (2.0 * executor);
                shuffleCost = ShuffleSpeed * MatrixBlock.estimateSizeInMemory(d1n, d1n, dcout.getSparsity()) * executor;
            } else {
                computeCost = CpuSpeed * d1m * d1n * d1s * d1n * d1s / (2.0 * executor);
                shuffleCost = ShuffleSpeed * MatrixBlock.estimateSizeInMemory(d1n, d1n, dcout.getSparsity()) * executor;
            }
        } else if (inst.getMMTSJType() == MMTSJ.MMTSJType.RIGHT) {
            out = createMMNode(in, tmp, SparsityEstimator.OpCode.MM);
            DataCharacteristics dcout = getDC(out);
            if (sparse) {
                computeCost = CpuSpeed * ((double) d1m * d1n * d1s + d1m * d1n * d1s * d1n * d1s / (2.0 * executor));
                shuffleCost = ShuffleSpeed * MatrixBlock.estimateSizeInMemory(d1n, d1n, dcout.getSparsity()) * executor;
            } else {
                computeCost = CpuSpeed * d1m * d1n * d1m / (2.0 * executor);
                shuffleCost = ShuffleSpeed * MatrixBlock.estimateSizeInMemory(d1n, d1n, dcout.getSparsity()) * executor;
            }
        }
        setMMNode(inst.output.getName(), out, Types.ExecType.SPARK);
        computeCostSummary += computeCost;
        shuffleCostSummary += shuffleCost;
        return shuffleCost + computeCost;
    }

    private static double eReorgSPInstruction(ReorgSPInstruction t) throws Exception {
        //   System.out.println("ReorgSP " + t.input1.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode out = createMMNode(m1, SparsityEstimator.OpCode.TRANS);
        setMMNode(t.output.getName(), out, Types.ExecType.SPARK);
        DataCharacteristics dc = getDC(m1);
        double cost = CpuSpeed * dc.getCols() * dc.getRows() / reducerNumber(m1.getRows(), m1.getCols());
        computeCostSummary += cost;
        return cost;
    }

    private static double eBinarySPInstruction(BinarySPInstruction inst) throws Exception {
        double cost = 0;
//        if (inst.getOpcode().equals("map-")) {
//            System.out.println("x");
//        }
        if (inst instanceof MapmmSPInstruction) {// matrix multiply
            cost += eMapmmSPInstruction((MapmmSPInstruction) inst);
        } else if (inst instanceof CpmmSPInstruction) { // matrix multiply
            cost += eCpmmSPInstruction((CpmmSPInstruction) inst);
        } else if (inst instanceof BinaryMatrixBVectorSPInstruction) { // minus: ax - b
            cost += eBinaryMatrixBVectorSPInstruction((BinaryMatrixBVectorSPInstruction) inst);
        } else if (inst instanceof BinaryMatrixScalarSPInstruction) { // PLUT,MULT
            cost += eBinaryMatrixScalarSPInstruction((BinaryMatrixScalarSPInstruction) inst);
        } else if (inst instanceof BinaryMatrixMatrixSPInstruction) {// PLUS,MULT
            cost += eBinaryMatrixMatrixSPInstruction((BinaryMatrixMatrixSPInstruction) inst);
        } else if (inst instanceof RmmSPInstruction) {
            cost += eRmmSPInstruction((RmmSPInstruction) inst);
        } else if (inst instanceof ZipmmSPInstruction) {
            cost += eZipmmSPInstruction((ZipmmSPInstruction) inst);
        } else {
            throw new UnhandledOperatorException(inst);
        }
        return cost;
    }


    public static double computeCostSPMM(DataCharacteristics dc1, DataCharacteristics dc2, DataCharacteristics dc3) {
        double result = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc2.getSparsity() * 1.5 / OptimizerUtils.getNumReducers(false);
        return result;
    }


    public static double matrixSize(DataCharacteristics dc) {
        return MatrixBlock.estimateSizeInMemory(dc.getRows(), dc.getCols(), dc.getSparsity());
    }

    public static double matrixSize(MatrixBlock mb) {
        return MatrixBlock.estimateSizeInMemory(mb.getNumRows(), mb.getNumColumns(), mb.getSparsity());
    }

    private static double eMapmmSPInstruction(MapmmSPInstruction t) throws Exception {
        //  System.out.println("MPMM " + t.input1.getName() + " %*% " + t.input2.getName() + " -> " + t.output.getName());
        if (MMShowCostFlag) {
            LOG.info("begin<<< " + t);
        }
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = createMMNode(m1, m2, SparsityEstimator.OpCode.MM);
        setMMNode(t.output.getName(), out, Types.ExecType.SPARK);
        DataCharacteristics dc1 = getDC(m1);
        DataCharacteristics dc2 = getDC(m2);
        DataCharacteristics dc3 = getDC(out);
        double broadcastCost = Double.MAX_VALUE;
        double computeCost = Double.MAX_VALUE;
        double shuffleCost = Double.MAX_VALUE;
        long r2 = reducerNumber(dc3.getRows(), dc3.getCols());
        if (t.getCacheType() == MapMult.CacheType.LEFT) {
            long r1 = Math.min(OptimizerUtils.getNumReducers(false), (long) Math.ceil((double) dc2.getRows() / defaultBlockSize));
            broadcastCost = BroadCaseSpeed * matrixSize(dc1) * Math.ceil(Math.log(Math.min(r1, SparkExecutionContext.getNumExecutors())));
            shuffleCost = ShuffleSpeed * matrixSize(dc3) * r1 / r2;
            computeCost = computeCostSPMM(dc1, dc2, dc3);
        } else if (t.getCacheType() == MapMult.CacheType.RIGHT) {
            long r1 = Math.min(OptimizerUtils.getNumReducers(false), (long) Math.ceil((double) dc1.getCols() / defaultBlockSize));
            broadcastCost = BroadCaseSpeed * matrixSize(dc2) * Math.ceil(Math.log(Math.min(r1, SparkExecutionContext.getNumExecutors())));
            shuffleCost = ShuffleSpeed * matrixSize(dc3) * r1 / r2;
            computeCost = computeCostSPMM(dc1, dc2, dc3);
        }
        if (MMShowCostFlag) {
            LOG.info("mapmm broadcast cost = " + broadcastCost);
            LOG.info("mapmm compute cost = " + computeCost);
            LOG.info("mapmm shuffle cost = " + shuffleCost);
            LOG.info(t + "  end>>>");
        }
        computeCostSummary += computeCost;
        shuffleCostSummary += shuffleCost;
        broadcastCostSummary += broadcastCost;
        return computeCost + shuffleCost + broadcastCost;
    }

    public static boolean MMShowCostFlag = false;

    private static double eCpmmSPInstruction(CpmmSPInstruction t) throws Exception {
        //  System.out.println("CPMM " + t.input1.getName() + " %*% " + t.input2.getName() + " -> " + t.output.getName());
        if (MMShowCostFlag) {
            LOG.info("begin<<< " + t);
        }
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = createMMNode(m1, m2, SparsityEstimator.OpCode.MM);
        setMMNode(t.output.getName(), out, Types.ExecType.SPARK);
        DataCharacteristics dc1 = getDC(m1);
        DataCharacteristics dc2 = getDC(m2);
        DataCharacteristics dc3 = getDC(out);

        long r = Math.min((long) Math.ceil((double) dc2.getRows() / defaultBlockSize), //max used reducers
                OptimizerUtils.getNumReducers(false)); //available reducer

        double shuffleCost1 = ShuffleSpeed * (matrixSize(dc1) + matrixSize(dc2)) / r;
        double shuffleCost2 = ShuffleSpeed * matrixSize(dc3) * r / reducerNumber(dc3.getRows(), dc3.getCols());
        double computeCost = computeCostSPMM(dc1, dc2, dc3);

        if (MMShowCostFlag) {
            LOG.info("cpmm shuffle cost 1 = " + shuffleCost1);
            LOG.info("cpmm shuffle cost 2 = " + shuffleCost2);
            LOG.info("cpmm compute cost = " + computeCost);
            LOG.info(t + "  end>>>");
        }
        computeCostSummary += computeCost;
        shuffleCostSummary += shuffleCost1 + shuffleCost2;
        return shuffleCost1 + shuffleCost2 + computeCost;
    }

    private static double eRmmSPInstruction(RmmSPInstruction t) throws Exception {
        if (MMShowCostFlag) {
            LOG.info("begin<<< " + t);
        }
        //  System.out.println("CPMM " + t.input1.getName() + " %*% " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = createMMNode(m1, m2, SparsityEstimator.OpCode.MM);
        setMMNode(t.output.getName(), out, Types.ExecType.SPARK);
        DataCharacteristics dc1 = getDC(m1);
        DataCharacteristics dc2 = getDC(m2);
        DataCharacteristics dc3 = getDC(out);

        long m1_nrb = (long) Math.ceil((double) dc1.getRows() / defaultBlockSize); // number of row blocks in m1
        long m2_ncb = (long) Math.ceil((double) dc2.getCols() / defaultBlockSize); // number of column blocks in m2
        long k = (long) Math.ceil((double) dc2.getRows() / defaultBlockSize);

        double rmm_nred = Math.min((double) m1_nrb * m2_ncb, //max used reducers
                OptimizerUtils.getNumReducers(false)); //available reducers

        double shuffleCost1 = ShuffleSpeed * (m2_ncb * matrixSize(dc1) + m1_nrb * matrixSize(dc2)) / rmm_nred;
        double shuffleCost2 = ShuffleSpeed * (matrixSize(dc3) * k) / rmm_nred;
        double computeCost = computeCostSPMM(dc1, dc2, dc3);

        if (MMShowCostFlag) {
            LOG.info("rmm shuffle cost 1 = " + shuffleCost1);
            LOG.info("rmm shuffle cost 2 = " + shuffleCost2);
            LOG.info("rmm compute cost = " + computeCost);
            LOG.info(t + "  end>>>");
        }
        shuffleCostSummary += shuffleCost1 + shuffleCost2;
        computeCostSummary += computeCost;
        return shuffleCost1 + shuffleCost2 + computeCost;

    }

    private static double eZipmmSPInstruction(ZipmmSPInstruction t) throws Exception {
        if (MMShowCostFlag) {
            LOG.info("begin<<< " + t);
        }
        //  System.out.println("CPMM " + t.input1.getName() + " %*% " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = createMMNode(m1, m2, SparsityEstimator.OpCode.MM);
        setMMNode(t.output.getName(), out, Types.ExecType.SPARK);
        DataCharacteristics dc1 = getDC(m1);
        DataCharacteristics dc2 = getDC(m2);
        DataCharacteristics dc3 = getDC(out);

        long reducer = (long) Math.ceil(Math.max(dc1.getRows(), dc1.getCols()) * 1.0 / defaultBlockSize);
        reducer = Math.min(reducer, OptimizerUtils.getNumReducers(false));
        reducer = Math.max(reducer, 1);
        double computeCost = CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * dc1.getSparsity() * dc1.getSparsity() / reducer;
        double reduceCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc3.getRows(), dc3.getRows(), dc3.getSparsity()) * reducer;

        if (MMShowCostFlag) {
            LOG.info("zipmm reduce cost = " + reduceCost);
            LOG.info("zipmm compute cost = " + computeCost);
            LOG.info(t + "  end>>>");
        }
        collectCostSummary += reduceCost;
        computeCostSummary += computeCost;
        return reduceCost + computeCost;
    }

    private static double eBinaryMatrixBVectorSPInstruction(BinaryMatrixBVectorSPInstruction t) throws Exception {
        //   System.out.println("BinaryMatrixBVectorSP " + t.input1.getName() + " " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = createMMNode(m1, m2, SparsityEstimator.OpCode.PLUS);
        double cost = 0;
        try {
            DataCharacteristics dc = getDC(out);
            //   System.out.println(" ed dc = " + dc);
            cost = CpuSpeed * dc.getNonZeros();
        } catch (Exception e) {
            e.printStackTrace();
        }
        computeCostSummary += cost;
        setMMNode(t.output.getName(), out, Types.ExecType.SPARK);
        return cost;
    }

    private static double eBinaryMatrixScalarSPInstruction(BinaryMatrixScalarSPInstruction inst) throws Exception {
        //  System.out.println("BinaryMatrixScalarSP " + inst.input1.getName() + " " + inst.input2.getName() + " -> " + inst.output.getName());
        if (inst.input1.getDataType() == Types.DataType.MATRIX) {
            MMNode tmp = getMMNode(inst.input1.getName());
            setMMNode(inst.output.getName(), tmp, Types.ExecType.SPARK);
        } else {
            MMNode tmp = getMMNode(inst.input2.getName());
            setMMNode(inst.output.getName(), tmp, Types.ExecType.SPARK);
        }
        return 0;
    }

    private static double eBinaryMatrixMatrixSPInstruction(BinaryMatrixMatrixSPInstruction t) throws Exception {
        //    System.out.println("BinaryMatrixMatrixSP " + t.input1.getName() + " " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        //   System.out.println(t.getOpcode());
        MMNode out = createMMNode(m1, m2, SparsityEstimator.OpCode.PLUS);
        setMMNode(t.output.getName(), out, Types.ExecType.SPARK);
        DataCharacteristics dc1 = getDC(m1);
        DataCharacteristics dc2 = getDC(m2);
        DataCharacteristics dc3 = getDC(out);
        boolean sparse = MatrixBlock.evalSparseFormatInMemory(dc3);
        long reducerNum = reducerNumber(dc3.getRows(), dc3.getCols());
        double shuffle = 0;
        double compute = 0;
        if (sparse) {
            compute = dc1.getNonZeros() + dc2.getNonZeros();
            shuffle = MatrixBlock.estimateSizeSparseInMemory(dc1.getRows(), dc1.getCols(), dc1.getSparsity())
                    + MatrixBlock.estimateSizeSparseInMemory(dc2.getRows(), dc2.getCols(), dc2.getSparsity());
        } else {
            compute = dc1.getRows() * dc1.getCols() + dc2.getRows() * dc2.getCols();
            shuffle = MatrixBlock.estimateSizeDenseInMemory(dc1.getRows(), dc1.getCols())
                    + MatrixBlock.estimateSizeDenseInMemory(dc2.getRows(), dc2.getCols());
        }
        double computeCost = CpuSpeed * compute/reducerNum;
        double shuffleCost = ShuffleSpeed * shuffle/reducerNum;
        computeCostSummary += computeCost;
        shuffleCostSummary += shuffleCost;
        return computeCost + shuffleCost;
    }


    private static double eTernarySPInstruction(TernarySPInstruction inst) throws Exception {
        // System.out.println("ternarySP " + inst.input1.getName() + " " + inst.input2.getName() + " " + inst.input3.getName() + " -> " + inst.output.getName());
        MMNode out = null;
        double shuffle = 0;
        double computation = 0;
        ArrayList<MMNode> inputs = new ArrayList<>();
        if (inst.input1.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input1.getName()));
        if (inst.input2.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input2.getName()));
        if (inst.input3.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input3.getName()));
        if (inputs.size() == 1) {
            out = inputs.get(0);
            computation = (double) out.getCols() * out.getRows();
            shuffle = 0;
        } else if (inputs.size() == 2) {
            MMNode m1 = inputs.get(0);
            MMNode m2 = inputs.get(1);
            out = createMMNode(m1, m2, SparsityEstimator.OpCode.MM);
            computation = (double) m1.getRows() * m1.getCols() * m2.getCols();
            shuffle = MatrixBlock.estimateSizeDenseInMemory(m1.getRows(), m1.getCols()) +
                    MatrixBlock.estimateSizeDenseInMemory(m2.getRows(), m2.getCols());
        } else if (inputs.size() == 3) {
            MMNode m1 = inputs.get(0);
            MMNode m2 = inputs.get(1);
            MMNode m3 = inputs.get(2);
            MMNode tmp = createMMNode(m1, m2, SparsityEstimator.OpCode.MM);
            out = createMMNode(tmp, m3, SparsityEstimator.OpCode.MM);
            computation = (double) m1.getRows() * m1.getCols() * m2.getCols() + (double) m1.getRows() * m2.getCols() * m3.getCols();
            shuffle = MatrixBlock.estimateSizeDenseInMemory(m1.getRows(), m1.getCols()) +
                    MatrixBlock.estimateSizeDenseInMemory(m2.getRows(), m2.getCols()) +
                    MatrixBlock.estimateSizeDenseInMemory(m3.getRows(), m3.getCols());
        }
        DataCharacteristics dc = getDC(out);
        setMMNode(inst.output.getName(), out, Types.ExecType.SPARK);

        long r = reducerNumber(dc.getRows(), dc.getCols());
        double computeCost = CpuSpeed * computation/r;
        double shuffleCost = ShuffleSpeed * shuffle/r;
        computeCostSummary += computeCost;
        shuffleCostSummary += shuffleCost;
        return computeCost + shuffleCost;
    }

    private static double eMapmmChainSPInstruction(MapmmChainSPInstruction inst) throws Exception {
        // System.out.println("mapmm chain sp ");
        MMNode out = null;
        double cost = 0;
        // todo: estimate the cost of MapmmChainSPInst
        if (inst.get_chainType() == MapMultChain.ChainType.XtXvy) {
            MMNode X = getMMNode(inst.get_input1().getName());
            DataCharacteristics dcX = getDC(X);
            MMNode v = getMMNode(inst.get_input2().getName());
            MMNode y = getMMNode(inst.get_input3().getName());
            MMNode Xv = createMMNode(X, v, SparsityEstimator.OpCode.MM);
            MMNode Xvy = createMMNode(Xv, y, SparsityEstimator.OpCode.PLUS);
            MMNode Xt = createMMNode(X, SparsityEstimator.OpCode.TRANS);
            out = createMMNode(Xt, Xvy, SparsityEstimator.OpCode.MM);
            DataCharacteristics dc = getDC(out);
//            long r = (long) Math.ceil((double) dcX.getNonZeros() / defaultBlockSize);
//            r = Math.max(r, 1);
            //     cost = sumTableCost(dc.getRows(), dc.getCols(), defaultBlockSize, r, dc.getSparsity());
        } else if (inst.get_chainType() == MapMultChain.ChainType.XtXv) {
            if (MMShowCostFlag) {
                LOG.info("begin<<< " + inst);
            }
            MMNode X = getMMNode(inst.get_input1().getName());
            DataCharacteristics dc1 = getDC(X);
            MMNode v = getMMNode(inst.get_input2().getName());
            DataCharacteristics dc2 = getDC(v);
            MMNode Xv = createMMNode(X, v, SparsityEstimator.OpCode.MM);
            MMNode Xt = createMMNode(X, SparsityEstimator.OpCode.TRANS);
            out = createMMNode(Xt, Xv, SparsityEstimator.OpCode.MM);
            DataCharacteristics dc3 = getDC(out);
            long reducer = reducerNumber(dc1.getRows(), dc1.getCols());
            reducer = Math.min(reducer, OptimizerUtils.getNumReducers(false));
            long executor = Math.min(reducer, SparkExecutionContext.getNumExecutors());
            double computeCost = 1.5 * CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() * (dc1.getSparsity() * dc2.getSparsity() + dc1.getSparsity()) / reducer;
            double broadcastCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc2.getRows(), dc2.getCols(), dc2.getSparsity()) * Math.ceil(Math.log(executor));
            double reduceCost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc3.getRows(), dc3.getRows(), dc3.getSparsity());
            if (MMShowCostFlag) {
                LOG.info("mapmmchain broad cost = " + broadcastCost);
                LOG.info("mapmmchain reduce cost = " + reduceCost);
                LOG.info("mapmmchain compute cost = " + computeCost);
                LOG.info(inst + "  end>>>");
            }
            cost = computeCost + reduceCost + broadcastCost;
            computeCostSummary += computeCost;
            broadcastCostSummary += broadcastCost;
            collectCostSummary += reduceCost;
        } else {
            //  System.out.println(inst.get_chainType());
            throw new UnhandledOperatorException(inst);
        }
        setMMNode(inst.get_output().getName(), out, Types.ExecType.SPARK);
        return cost;
    }

//    private static double e( inst) {
//
//    }


    //以上是处理各种instruction的函数
    //////////////////////////////

    private static double sumTableCost(long rows, long cols, long replicate, long numReducer, double sparsity, boolean show) {
        double size, compute;
        if (MatrixBlock.evalSparseFormatInMemory(rows, cols, (long) sparsity * rows * cols)) {
            size = MatrixBlock.estimateSizeSparseInMemory(rows, cols, sparsity);
            compute = sparsity * rows * cols * replicate;
        } else {
            size = MatrixBlock.estimateSizeDenseInMemory(rows, cols);
            compute = rows * cols * replicate;
        }
        //long numReducer = reducerNumber(rows, cols, blen);
        double shuffle = size * replicate;
        //  double io = (replicate + 1) * size * sparsity;
        if (show) {
            LOG.info("sum table shuffle = " + shuffle);
            LOG.info("sum table compute = " + compute);
            LOG.info("sum table numReducer = " + numReducer);
        }

        double shuffleCost = ShuffleSpeed * shuffle / numReducer;
        double computeCost = CpuSpeed * compute / numReducer;
        if (show) {
            LOG.info("sum table shuffle cost = " + shuffleCost);
            LOG.info("sum table compute cost = " + computeCost);
        }
        return shuffleCost + computeCost;
    }

    public static long reducerNumber(long rows, long cols) {
        if (defaultBlockSize <= 0) return OptimizerUtils.getNumReducers(false);
        long nrb = (long) Math.ceil((double) rows / defaultBlockSize);
        long ncb = (long) Math.ceil((double) cols / defaultBlockSize);
        long numReducer = Math.min(nrb * ncb, OptimizerUtils.getNumReducers(false));
        numReducer = Math.max(numReducer, 1);
        return numReducer;
    }

    private static double getCollectCost(String name) throws Exception {
        //   LOG.info("get collect cost "+name);
        Types.ExecType type = getExecType(name);
        if (type == null || type == Types.ExecType.SPARK) {
            MMNode mmNode = getMMNode(name);
            if (mmNode != null) {
                DataCharacteristics dc = getDC(mmNode);
                double cost = BroadCaseSpeed * MatrixBlock.estimateSizeInMemory(dc.getRows(), dc.getCols(), dc.getSparsity());
                cost = Math.max(cost, 0);
                //   LOG.info(name + " collect cost = " + cost);
                collectCostSummary += cost;
                return cost;
            }
        }
        return 0;
    }


////////////////////////////////////////////////////////////////////
//                   print instructions                           //
////////////////////////////////////////////////////////////////////


    public static void printInstructions(Program rtprog) {
        for (ProgramBlock pb : rtprog.getProgramBlocks()) {
            rPrintInstrunctions(pb, 0);
        }
    }

    private static void rPrintInstrunctions(ProgramBlock pb, int depth) {
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < depth; i++) prefix.append(" ");
        if (pb instanceof IfProgramBlock) {
            IfProgramBlock ipb = (IfProgramBlock) pb;
            LOG.info(prefix + "if begin");
            LOG.info(prefix + " predicate:");
            printInstruction(ipb.getPredicate(), depth + 2);
            LOG.info(prefix + " if body");
            for (ProgramBlock p : ipb.getChildBlocksIfBody()) {
                rPrintInstrunctions(p, depth + 2);
            }
            LOG.info(prefix + " else body");
            for (ProgramBlock p : ipb.getChildBlocksElseBody()) {
                rPrintInstrunctions(p, depth + 2);
            }
            LOG.info(prefix + "if end");
        } else if (pb instanceof WhileProgramBlock) {
            WhileProgramBlock wpb = (WhileProgramBlock) pb;
            LOG.info(prefix + "while begin");
            LOG.info(prefix + " predicate:");
            printInstruction(wpb.getPredicate(), depth + 2);
            LOG.info(prefix + " while body");
            for (ProgramBlock p : wpb.getChildBlocks()) {
                rPrintInstrunctions(p, depth + 2);
            }
            LOG.info(prefix + "while end");
        } else if (pb instanceof BasicProgramBlock) {
            LOG.info(prefix + "basic begin");
            BasicProgramBlock bpb = (BasicProgramBlock) pb;
            printInstruction(bpb.getInstructions(), depth + 1);
            LOG.info(prefix + "basic end");
        }
    }

    private static void printInstruction(ArrayList<Instruction> instructions, int depth) {
        String prefix = "";
        for (int i = 0; i < depth; i++) prefix = prefix + " ";
        for (Instruction instruction : instructions) {
            LOG.info(prefix +
                    instruction.getClass().getName().substring(38) +
                    "  " + instruction.getOpcode() +
                    "  " + instruction.getInstructionString());
        }
    }

}
