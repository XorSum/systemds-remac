package org.apache.sysds.hops.rewrite.dfp.costmodel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.hops.estim.EstimatorBasicAvg;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.lops.MMTSJ;
import org.apache.sysds.lops.MapMult;
import org.apache.sysds.lops.MapMultChain;
import org.apache.sysds.runtime.controlprogram.*;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.Instruction;
import org.apache.sysds.runtime.instructions.cp.*;
import org.apache.sysds.runtime.instructions.spark.*;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class FakeCostEstimator2 {
    protected static final Log LOG = LogFactory.getLog(FakeCostEstimator2.class.getName());

    private static double CpuSpeed = 1.0;
    private static double ShuffleSpeed = 3.0;
    private static long defaultBlockSize = 1000;
    public static ExecutionContext ec = null;

    public static double estimate(Hop root) {
        root.resetVisitStatusForced(new HashSet<>());
        double cost = rEatimate(root);
        root.resetVisitStatus();
        return cost;
    }

    private static double rEatimate(Hop hop) {
        double cost = 0;
        if (Judge.isLeafMatrix(hop)) {
            long nnz = hop.getNnz();
            cost = CpuSpeed * nnz;
        } else if (HopRewriteUtils.isTransposeOperation(hop)) {
            cost = CpuSpeed * hop.getNnz();
        } else if (HopRewriteUtils.isMatrixMultiply(hop)) {
            AggBinaryOp aggBinaryOp = (AggBinaryOp) hop;
            aggBinaryOp.constructLops();
            AggBinaryOp.MMultMethod mMultMethod = aggBinaryOp.getMMultMethod();
            Hop input1 = aggBinaryOp.getInput().get(0);
            Hop input2 = aggBinaryOp.getInput().get(1);
            if (mMultMethod == AggBinaryOp.MMultMethod.MM) {

            } else if (mMultMethod == AggBinaryOp.MMultMethod.CPMM) {

            } else if (mMultMethod == AggBinaryOp.MMultMethod.MAPMM_L) {

            } else if (mMultMethod == AggBinaryOp.MMultMethod.MAPMM_R) {

            } else {

            }
        } else if (HopRewriteUtils.isBinaryMatrixMatrixOperation(hop)) {
            if (HopRewriteUtils.isBinary(hop, Types.OpOp2.MULT)) {

            } else if (HopRewriteUtils.isBinary(hop, Types.OpOp2.PLUS)) {

            } else if (HopRewriteUtils.isBinary(hop, Types.OpOp2.MINUS)) {

            } else if (HopRewriteUtils.isBinary(hop, Types.OpOp2.DIV)) {

            }
        } else if (HopRewriteUtils.isBinaryMatrixScalarOperation(hop)) {
            long nnz = hop.getNnz();
            cost = CpuSpeed * nnz;
        }
        return cost;
    }


    public static double estimate(Program rtprog) {
        double cost = 0;
        for (ProgramBlock pb : rtprog.getProgramBlocks()) {
            double delta = rEstimate(pb);
            if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
            cost += delta;
        }
        return cost;
    }

    private static double rEstimate(ProgramBlock pb) {
        double cost = 0;
        double delta;
        if (pb instanceof IfProgramBlock) {
            IfProgramBlock ipb = (IfProgramBlock) pb;
            delta = rEstimate(ipb.getPredicate());
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
            delta = rEstimate(wpb.getPredicate());
            if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
            cost += delta;
            for (ProgramBlock p : wpb.getChildBlocks()) {
                delta = rEstimate(p);
                if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
                cost += delta;
            }
        } else if (pb instanceof BasicProgramBlock) {
            BasicProgramBlock bpb = (BasicProgramBlock) pb;
            delta = rEstimate(bpb.getInstructions());
            if (delta < 0 || delta >= Double.MAX_VALUE) return Double.MAX_VALUE;
            cost += delta;
        }
        return cost;
    }

    public static double rEstimate(ArrayList<Instruction> instructions) {
//        System.out.println("<<<explain instructions");
//        printInstruction(instructions, 0);
//        System.out.println("explain instructions>>>");
        // step 1. according to Instructions, build MMNode Tree and topology rank
        // step 2. traverse all the nodes by topo rank, get the operator type and sparsity of each node
        // step 3. traverse all the nodes by topo rank , calculate the cost of each node
        names = new ArrayList<>();
        double cost = 0;
        try {
            cost = processInstructions(instructions);
        } catch (Exception e) {
            e.printStackTrace();
            return Double.MAX_VALUE;
        }
        //   System.out.println("Summary Cost = " + cost);
//        System.out.println("name size = " + name2MMNode.size());
//        System.out.println(name2MMNode.keySet());
//        System.out.println("# names:");
//        for (String name : names) {
//            MMNode mmNode = null;
//            mmNode = getMMNode(name);
//            System.out.println("# " + name + ((mmNode != null) ? (" " + mmNode) : ""));
//        }
//        if (map.containsKey("_mVar1515")) {
//            System.out.println("x");
//        }
        return cost;
    }

    private static EstimatorBasicAvg estimator = new EstimatorBasicAvg();
    private static HashMap<String, MMNode> name2MMNode = new HashMap<>();
    private static ArrayList<String> names = new ArrayList<>();
    private static HashMap<String, DataCharacteristics> name2DataCharacteristics = new HashMap<>();

    private static MMNode getMMNode(String name) {
        // 先不获取matrix block, 直接用data characristic
        if (ec != null && ec.containsVariable(name)) {
            try {
                Data data = ec.getVariable(name);
                if (data.getDataType() == Types.DataType.MATRIX) {
                    //     System.out.println("metadata " + name + " " + data.getMetaData());
                    DataCharacteristics characteristics = data.getMetaData().getDataCharacteristics();
                    MMNode mmNode = new MMNode(characteristics);
                    return mmNode;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (name2MMNode.containsKey(name)) {
            return name2MMNode.get(name);
        }
//            Data dat = ec.getVariables().get(name);
//            MatrixObject mobj = (MatrixObject) dat;
//            System.out.println( name + ": nnz=" + mobj.getNnz()+ " metadata=" + mobj.getMetaData());
//            if (!(dat instanceof MatrixObject))
//                throw new HopsException("Input '" + name + "' not a matrix: " + dat.getDataType());
//            try {
//                MatrixBlock matrixBlock = ((MatrixObject) dat).acquireReadAndRelease();
//                return new MMNode(matrixBlock);
//            } catch (Exception e) {
//                System.out.println("getMMNode "+name);
//                e.printStackTrace();
//            }
//        }
        //   System.out.println("warn: get mmnode failed " + name);
        return null;
    }

    private static void setMMNode(String name, MMNode mmNode) {
//        if ("x".equals(name)) {
//            System.out.println("x");
//        }
//        System.out.println("set mmnode " + name + " -> " + mmNode);
        name2MMNode.put(name, mmNode);
        names.add(name);
    }


    private static double processInstructions(ArrayList<Instruction> instructions) throws Exception {
        double cost = 0;
        for (Instruction inst : instructions) {
            double delta = 0;
            try {
                if (inst instanceof CPInstruction) {
                    delta = eCPInstruction((CPInstruction) inst);
                } else if (inst instanceof SPInstruction) {
                    delta = eSPInstruction((SPInstruction) inst);
                } else {
                    throw new UnhandledOperatorException(inst);
                }
            } catch (UnhandledOperatorException e) {
                System.out.println(e);
                e.printStackTrace();
            }
            if (delta < 0) {
                //  LOG.error("ERROR NEGATIVE COST " + inst.toString());
                //  LOG.error(inst.getClass().getName());
                return Double.MAX_VALUE;
            }
            cost += delta;
        }
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
        } else {
            throw new UnhandledOperatorException(inst);
        }
        //System.out.println(inst.toString() + cost);
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

    private static double eUnaryCPInstruction(UnaryCPInstruction inst) throws UnhandledOperatorException {  // pass through
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

    private static double eMMChainCPInstruction(MMChainCPInstruction inst) {
        MMNode out;
        double cost = 0;
        MMNode m1 = getMMNode(inst.input1.getName());
        MMNode m2 = getMMNode(inst.input2.getName());
        MMNode tmp = new MMNode(m1, m2, SparsityEstimator.OpCode.MM);
        if (inst.input3 != null) {
            out = new MMNode(tmp, getMMNode(inst.input3.getName()), SparsityEstimator.OpCode.MM);
        } else out = tmp;
        DataCharacteristics m1DC = estimator.estim(m1);
        double d1m = m1DC.getRows(), d1n = m1DC.getCols(), d1s = m1DC.getSparsity();
        boolean leftSparse = MatrixBlock.evalSparseFormatInMemory(m1DC);
        if (!leftSparse)
            cost = CpuSpeed * (2 + 2) * ((double) d1m * d1n) / 2;
        else
            cost = CpuSpeed * (2 + 2) * ((double) d1m * d1n * d1s) / 2;
        setMMNode(inst.output.getName(), out);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eReorgCPInstruction(ReorgCPInstruction inst) {
//        System.out.println(inst.getOpcode());
        double cost = 0;
        if ("r'".equals(inst.getOpcode())) {
            MMNode in = getMMNode(inst.input1.getName());
            DataCharacteristics dc = estimator.estim(in);
            boolean sparse = MatrixBlock.evalSparseFormatInMemory(dc);
            if (sparse) {
                cost = CpuSpeed * dc.getRows() * dc.getCols() * dc.getSparsity();
            } else {
                cost = CpuSpeed * dc.getRows() * dc.getCols();
            }
            MMNode out = new MMNode(in, SparsityEstimator.OpCode.TRANS);
            setMMNode(inst.output.getName(), out);
        }
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eMMTSJCPInstruction(MMTSJCPInstruction inst) { // tsmm
        MMNode in = getMMNode(inst.input1.getName());
        DataCharacteristics dcin = estimator.estim(in);
        double d1m = dcin.getRows(), d1n = dcin.getCols(), d1s = dcin.getSparsity();
        boolean sparse = MatrixBlock.evalSparseFormatInMemory(dcin);
        MMNode tmp = new MMNode(in, SparsityEstimator.OpCode.TRANS);
        MMNode out = null;
        double cost = 0;
        if (inst.getMMTSJType() == MMTSJ.MMTSJType.LEFT) {
            out = new MMNode(tmp, in, SparsityEstimator.OpCode.MM);
            if (sparse) {
                cost = CpuSpeed * d1m * d1n * d1s * d1n / 2;
            } else {
                cost = CpuSpeed * d1m * d1n * d1s * d1n * d1s / 2;
            }
        } else if (inst.getMMTSJType() == MMTSJ.MMTSJType.RIGHT) {
            out = new MMNode(in, tmp, SparsityEstimator.OpCode.MM);
            if (sparse) {
                cost = CpuSpeed * d1m * d1n * d1s + d1m * d1n * d1s * d1n * d1s / 2;
            } else {
                cost = CpuSpeed * d1m * d1n * d1m / 2;
            }
        }
        setMMNode(inst.output.getName(), out);
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

    private static double eTernaryCPInstruction(TernaryCPInstruction inst) { // Matrix Multiply
        //  System.out.println("TernaryCP " + inst.output.getName());
        double cost = 0;
        MMNode out = null;
        ArrayList<MMNode> inputs = new ArrayList<>();
        if (inst.input1.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input1.getName()));
        if (inst.input2.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input2.getName()));
        if (inst.input3.getDataType() == Types.DataType.MATRIX) inputs.add(getMMNode(inst.input3.getName()));
        if (inputs.size() == 1) {
            out = inputs.get(0);
        } else if (inputs.size() == 2) {
            out = new MMNode(inputs.get(0), inputs.get(1), SparsityEstimator.OpCode.MM);
        } else if (inputs.size() == 3) {
            MMNode tmp = new MMNode(inputs.get(0), inputs.get(1), SparsityEstimator.OpCode.MM);
            out = new MMNode(tmp, inputs.get(2), SparsityEstimator.OpCode.MM);
        }
        cost = CpuSpeed * 2 * inputs.get(0).getRows() * inputs.get(0).getCols();
        try {
            DataCharacteristics dc = estimator.estim(out);
            //   System.out.println("data characteristics " + inst.output.getName() + " " + dc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        setMMNode(inst.output.getName(), out);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eAggregateBinaryCPInstruction(AggregateBinaryCPInstruction inst) {
        //   System.out.println("AggregateBinaryCP " + inst.input1.getName() + " %*% " + inst.input2.getName() + " -> " + inst.output.getName());
        MMNode m1 = getMMNode(inst.input1.getName());
        MMNode m2 = getMMNode(inst.input2.getName());
        MMNode out = new MMNode(m1, m2, SparsityEstimator.OpCode.MM);

        double cost = 0;
        try {
            DataCharacteristics m1DC = estimator.estim(m1);
            double d1m = m1DC.getRows(), d1n = m1DC.getCols(), d1s = m1DC.getSparsity();
            boolean leftSparse = MatrixBlock.evalSparseFormatInMemory(m1DC);
            DataCharacteristics m2DC = estimator.estim(m2);
            double d2m = m2DC.getRows(), d2n = m2DC.getCols(), d2s = m2DC.getSparsity();
            boolean rightSparse = MatrixBlock.evalSparseFormatInMemory(m2DC);
            DataCharacteristics outDC = estimator.estim(out);
            double d3m = outDC.getRows(), d3n = outDC.getCols(), d3s = outDC.getSparsity();

            if (!leftSparse && !rightSparse)
                cost = CpuSpeed * 2 * ((double) d1m * d1n * ((d2n > 1) ? d1s : 1.0) * d2n) / 2;
            else if (!leftSparse && rightSparse)
                cost = CpuSpeed * 2 * (d1m * d1n * d1s * d2n * d2s) / 2;
            else if (leftSparse && !rightSparse)
                cost = CpuSpeed * 2 * (d1m * d1n * d1s * d2n) / 2;
            else //leftSparse && rightSparse
                cost = CpuSpeed * 2 * (d1m * d1n * d1s * d2n * d2s) / 2;

        } catch (Exception e) {
            e.printStackTrace();
        }
        setMMNode(inst.output.getName(), out);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eBinaryMatrixMatrixCPInstruction(BinaryMatrixMatrixCPInstruction inst) {
        //  System.out.println("BinaryMatrixMatrixSP " + inst.input1.getName() + " " + inst.getOpcode() + " " + inst.input2.getName() + " -> " + inst.output.getName());
        MMNode m1 = getMMNode(inst.input1.getName());
        MMNode m2 = getMMNode(inst.input2.getName());
        MMNode out = null;
        double cost = 0;
        try {
            DataCharacteristics m1DC = estimator.estim(m1);
            double d1m = m1DC.getRows(), d1n = m1DC.getCols(), d1s = m1DC.getSparsity();
            boolean leftSparse = MatrixBlock.evalSparseFormatInMemory(m1DC);
            DataCharacteristics m2DC = estimator.estim(m2);
            double d2m = m2DC.getRows(), d2n = m2DC.getCols(), d2s = m2DC.getSparsity();
            boolean rightSparse = MatrixBlock.evalSparseFormatInMemory(m2DC);
            if ("*".equals(inst.getOpcode())) {
                out = new MMNode(m1, m2, SparsityEstimator.OpCode.MULT);
                DataCharacteristics outDC = estimator.estim(out);
                double d3m = outDC.getRows(), d3n = outDC.getCols(), d3s = outDC.getSparsity();
                cost = CpuSpeed * d3m * d3n;
            } else {
                out = new MMNode(m1, m2, SparsityEstimator.OpCode.PLUS);
                DataCharacteristics outDC = estimator.estim(out);
                double d3m = outDC.getRows(), d3n = outDC.getCols(), d3s = outDC.getSparsity();
                if (leftSparse || rightSparse) cost = CpuSpeed * (d1m * d1n * d1s + d2m * d2n * d2s);
                else cost = CpuSpeed * d3m * d3n;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        setMMNode(inst.output.getName(), out);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eBinaryMatrixScalarCPInstruction(BinaryMatrixScalarCPInstruction inst) {
        // PLUT, MULT
        //  System.out.println("BinaryMatrixScalarSP " + inst.input1.getName() + " " + inst.getOpcode() + " " + inst.input2.getName() + " -> " + inst.output.getName());
        MMNode out = null;
        double cost = 0;
        if (inst.input1.getDataType() == Types.DataType.MATRIX) {
            out = getMMNode(inst.input1.getName());
        } else {
            out = getMMNode(inst.input2.getName());
        }
        try {
            DataCharacteristics outDC = estimator.estim(out);
            double d3m = outDC.getRows(), d3n = outDC.getCols(), d3s = outDC.getSparsity();
            boolean sparse = MatrixBlock.evalSparseFormatInMemory(outDC);
            if (sparse) {
                cost = CpuSpeed * d3m * d3n * d3s;
            } else {
                cost = CpuSpeed * d3m * d3n;
            } //  System.out.println("data characteristics " + inst.output.getName() + " " + dc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        setMMNode(inst.output.getName(), out);
        //System.out.println(inst.toString() + cost);
        return cost;
    }

    private static double eVariableCPInstruction(VariableCPInstruction inst) throws UnhandledOperatorException {
        CPOperand input1 = inst.getInput1();
        CPOperand input2 = inst.getInput2();
        CPOperand output = inst.getOutput();
        if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.CreateVariable) {
            //   System.out.println("creatvar " + input1.getName());
            MMNode mmNode = new MMNode(inst.getMetadata().getDataCharacteristics());
            setMMNode(input1.getName(), mmNode);
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.RemoveVariable) {
            //    System.out.println("rmvar " + input1.getName());
            for (CPOperand operand : inst.getInputs()) {
//                try {
//                    MMNode mmNode = getMMNode(operand.getName());
//                    if (mmNode != null) {
//                        DataCharacteristics dc = estimator.estim(mmNode);
//                        LOG.info(dc);
//                    }
//                } catch (Exception e) {
//                }
                name2MMNode.remove(operand.getName());
            }
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.AssignVariable) {
            //    System.out.println("assignvar " + input1.getName() + " -> " + input2.getName());
            MMNode mmNode = name2MMNode.get(input1.getName());
            setMMNode(input2.getName(), mmNode);
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.CastAsScalarVariable) {
            //   System.out.println("castdts " + input1.getName() + " -> " + output.getName());
            //    map.put(output.getName(), null);
            names.add(output.getName());
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.CopyVariable) {
            //   System.out.println("cpvar " + input1.getName() + " -> " + input2.getName());
            MMNode mmNode = name2MMNode.get(input1.getName());
            setMMNode(input2.getName(), mmNode);
        } else if (inst.getVariableOpcode() == VariableCPInstruction.VariableOperationCode.MoveVariable) {
            //  System.out.println("mvvar " + input1.getName() + " -> " + input2.getName());
            MMNode mmNode = name2MMNode.get(input1.getName());
            setMMNode(input2.getName(), mmNode);
            name2MMNode.remove(input1.getName());
        } else {
            throw new UnhandledOperatorException(inst);
        }
        return 0;
    }

    private static double eFunctionCallCPInstruction(FunctionCallCPInstruction fcp) {
        System.out.print("FunctionCallCP " + fcp.getFunctionName());
        System.out.print(" [");
        for (CPOperand cpOperand : fcp.getInputs()) {
            System.out.print(cpOperand.getName() + ",");
        }
        System.out.print("] -> [");
        for (String name : fcp.getBoundOutputParamNames()) {
            System.out.print(name + ",");
            setMMNode(name, null);
        }
        System.out.println("]");
        return 0;
    }

    private static double eSPInstruction(SPInstruction inst) throws Exception {
        double cost = 0;
        if (inst instanceof ComputationSPInstruction) {
            cost += eComputationSPInstruction((ComputationSPInstruction) inst);
        } else if (inst instanceof MapmmChainSPInstruction) {
            cost += eMapmmChainSPInstruction((MapmmChainSPInstruction) inst);
        } else {
            throw new UnhandledOperatorException(inst);
        }
        return cost;
    }

    private static double eComputationSPInstruction(ComputationSPInstruction inst) throws UnhandledOperatorException {
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

    private static double eUnarySPInstruction(UnarySPInstruction inst) throws UnhandledOperatorException {
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


    private static double eTsmmSPInstruction(TsmmSPInstruction inst) {
        // System.out.println("TSMM " + inst.input1.getName() + " -> " + inst.output.getName());
        MMNode in = getMMNode(inst.input1.getName());
        DataCharacteristics dcin = estimator.estim(in);
        long d1m = dcin.getRows(), d1n = dcin.getCols();
        double d1s = dcin.getSparsity();
        long blen = defaultBlockSize;
        boolean sparse = MatrixBlock.evalSparseFormatInMemory(dcin);
        MMNode tmp = new MMNode(in, SparsityEstimator.OpCode.TRANS);
        MMNode out = null;
        double cost = 0;
        if (inst.getMMTSJType() == MMTSJ.MMTSJType.LEFT) {
            out = new MMNode(tmp, in, SparsityEstimator.OpCode.MM);
            long r = Math.max((long) Math.ceil((double) d1m / blen), 1);
            if (sparse) {
                cost += CpuSpeed * d1m * d1n * d1s * d1n / 2;
                cost += sumTableCost(d1n, d1n, blen, r, dcin.getSparsity());
            } else {
                cost += CpuSpeed * d1m * d1n * d1s * d1n * d1s / 2;
                cost += sumTableCost(d1n, d1n, blen, r, 1.0);
            }
        } else if (inst.getMMTSJType() == MMTSJ.MMTSJType.RIGHT) {
            out = new MMNode(in, tmp, SparsityEstimator.OpCode.MM);
            long r = Math.max((long) Math.ceil((double) d1m / blen), 1);
            if (sparse) {
                cost += CpuSpeed * ((double) d1m * d1n * d1s + d1m * d1n * d1s * d1n * d1s / 2);
                cost += sumTableCost(d1n, d1n, blen, r, dcin.getSparsity());
            } else {
                cost += CpuSpeed * d1m * d1n * d1m / 2;
                cost += sumTableCost(d1n, d1n, blen, 1, 1.0);
            }
        }
        setMMNode(inst.output.getName(), out);
        return cost;
    }

    private static double eReorgSPInstruction(ReorgSPInstruction t) {
        //   System.out.println("ReorgSP " + t.input1.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode out = new MMNode(m1, SparsityEstimator.OpCode.TRANS);
        setMMNode(t.output.getName(), out);
        DataCharacteristics dc = estimator.estim(m1);
        double cost = CpuSpeed * dc.getCols() * dc.getCols();
        return cost;
    }

    private static double eBinarySPInstruction(BinarySPInstruction inst) throws UnhandledOperatorException {
        double cost = 0;
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
        } else {
            throw new UnhandledOperatorException(inst);
        }
        return cost;
    }

    private static double eMapmmSPInstruction(MapmmSPInstruction t) {
        //  System.out.println("MPMM " + t.input1.getName() + " %*% " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = new MMNode(m1, m2, SparsityEstimator.OpCode.MM);
        setMMNode(t.output.getName(), out);
        DataCharacteristics dc1 = estimator.estim(m1);
        DataCharacteristics dc2 = estimator.estim(m2);
        DataCharacteristics dc3 = estimator.estim(out);
        double cost = 0;
        if (t.getCacheType() == MapMult.CacheType.LEFT) {
            long r = reducerNumber(dc2.getRows(), dc2.getCols(), defaultBlockSize);
            cost += sumTableCost(dc3.getRows(), dc3.getCols(), defaultBlockSize, r, dc3.getSparsity());
            cost += CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() / r;
        } else if (t.getCacheType() == MapMult.CacheType.RIGHT) {
            long r = reducerNumber(dc1.getRows(), dc1.getCols(), defaultBlockSize);
            cost += sumTableCost(dc3.getRows(), dc3.getCols(), defaultBlockSize, r, dc3.getSparsity());
            cost += CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() / r;
        }
        return cost;
    }

    private static double eCpmmSPInstruction(CpmmSPInstruction t) {
        //  System.out.println("CPMM " + t.input1.getName() + " %*% " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = new MMNode(m1, m2, SparsityEstimator.OpCode.MM);
        setMMNode(t.output.getName(), out);
        DataCharacteristics dc1 = estimator.estim(m1);
        DataCharacteristics dc2 = estimator.estim(m2);
        DataCharacteristics dc3 = estimator.estim(out);
        double cost = 0;
        // stage1
        double m1_size = (double) dc1.getRows() * dc1.getCols();
        double m2_size = (double) dc2.getRows() * dc2.getCols();
        double m3_size = (double) dc3.getRows() * dc3.getCols();
        long m1_ncb = (long) Math.ceil((double) dc1.getCols() / defaultBlockSize); // number of column blocks in m1
        m1_ncb = Math.max(m1_ncb, 1);
        long cpmm_nred1 = Math.min(m1_ncb, //max used reducers
                OptimizerUtils.getNumReducers(false)); //available reducer

        double shuffle1 = m1_size + m2_size;
        double io1 = m1_size + m2_size + cpmm_nred1 * m3_size;
        cost += ShuffleSpeed * (shuffle1 + io1) / cpmm_nred1;
        cost += CpuSpeed * dc1.getRows() * dc1.getCols() * dc2.getCols() / cpmm_nred1;
        // stage 2
        cost += sumTableCost(dc3.getRows(), dc3.getCols(), defaultBlockSize, cpmm_nred1, dc3.getSparsity());

        return cost;
    }

    private static double eRmmSPInstruction(RmmSPInstruction t) {

        //  System.out.println("CPMM " + t.input1.getName() + " %*% " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = new MMNode(m1, m2, SparsityEstimator.OpCode.MM);
        setMMNode(t.output.getName(), out);
        DataCharacteristics dc1 = estimator.estim(m1);
        DataCharacteristics dc2 = estimator.estim(m2);
        DataCharacteristics dc3 = estimator.estim(out);
        double cost = 0;

        long m1_nrb = (long) Math.ceil((double) dc1.getRows() / defaultBlockSize); // number of row blocks in m1
        long m2_ncb = (long) Math.ceil((double) dc2.getCols() / defaultBlockSize); // number of column blocks in m2

        double m1_size = (double) dc1.getRows() * dc1.getCols();
        double m2_size = (double) dc2.getRows() * dc2.getCols();
        double result_size = (double) dc3.getRows() * dc3.getCols();
        long m1_ncb = (long) Math.ceil((double) dc1.getCols() / defaultBlockSize); // number of column blocks in m1
        m1_ncb = Math.max(m1_ncb, 1);
        int numReducersRMM = OptimizerUtils.getNumReducers(true);
        numReducersRMM = Math.max(numReducersRMM, 1);
        // Estimate the cost of RMM
        // RMM phase 1
        double rmm_shuffle = ((double) m2_ncb * m1_size) + ((double) m1_nrb * m2_size);
        double rmm_io = m1_size + m2_size + result_size;
        double rmm_nred = Math.min((double) m1_nrb * m2_ncb, //max used reducers
                numReducersRMM); //available reducers
        // RMM total costs
        double rmm_costs = ((double) rmm_shuffle + rmm_io) / rmm_nred;

        cost += ShuffleSpeed * rmm_costs;
        if (cost < 0) {
            cost = Double.MAX_VALUE;
            LOG.error("RMM error");
            LOG.error("dc1=" + dc1);
            LOG.error("dc2=" + dc2);
            LOG.error("dc3=" + dc3);
        }
        return cost;
    }


    private static double eBinaryMatrixBVectorSPInstruction(BinaryMatrixBVectorSPInstruction t) {
        //   System.out.println("BinaryMatrixBVectorSP " + t.input1.getName() + " " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        MMNode out = new MMNode(m1, m2, SparsityEstimator.OpCode.PLUS);
        try {
            DataCharacteristics dc = estimator.estim(out);
            //   System.out.println(" ed dc = " + dc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        setMMNode(t.output.getName(), out);
        return 0;
    }

    private static double eBinaryMatrixScalarSPInstruction(BinaryMatrixScalarSPInstruction inst) {
        //  System.out.println("BinaryMatrixScalarSP " + inst.input1.getName() + " " + inst.input2.getName() + " -> " + inst.output.getName());
        if (inst.input1.getDataType() == Types.DataType.MATRIX) {
            MMNode tmp = getMMNode(inst.input1.getName());
            setMMNode(inst.output.getName(), tmp);
        } else {
            MMNode tmp = getMMNode(inst.input2.getName());
            setMMNode(inst.output.getName(), tmp);
        }
        return 0;
    }

    private static double eBinaryMatrixMatrixSPInstruction(BinaryMatrixMatrixSPInstruction t) {
        //    System.out.println("BinaryMatrixMatrixSP " + t.input1.getName() + " " + t.input2.getName() + " -> " + t.output.getName());
        MMNode m1 = getMMNode(t.input1.getName());
        MMNode m2 = getMMNode(t.input2.getName());
        //   System.out.println(t.getOpcode());
        MMNode out = new MMNode(m1, m2, SparsityEstimator.OpCode.PLUS);
        setMMNode(t.output.getName(), out);
        DataCharacteristics dc1 = estimator.estim(m1);
        DataCharacteristics dc2 = estimator.estim(m2);
        DataCharacteristics dc3 = estimator.estim(out);
        boolean sparse = MatrixBlock.evalSparseFormatInMemory(dc3);
        long reducerNum = reducerNumber(dc3.getRows(), dc3.getCols(), defaultBlockSize);
        double size = 0;
        double cost = 0;
        if (sparse) {
            size = dc1.getNonZeros() + dc2.getNonZeros();
        } else {
            size = (double) dc1.getRows() * dc1.getCols() + (double) dc2.getRows() * dc2.getCols();
        }
        cost += (CpuSpeed * size + ShuffleSpeed * size) / reducerNum;
        return cost;
    }


    private static double eTernarySPInstruction(TernarySPInstruction inst) {
        // System.out.println("ternarySP " + inst.input1.getName() + " " + inst.input2.getName() + " " + inst.input3.getName() + " -> " + inst.output.getName());
        MMNode out = null;
        double shuffle = 0;
        double cost = 0;
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
            out = new MMNode(m1, m2, SparsityEstimator.OpCode.MM);
            computation = (double) m1.getRows() * m1.getCols() * m2.getCols();
            shuffle = (double) m1.getCols() * m1.getRows() + (double) m2.getCols() * m2.getRows();
        } else if (inputs.size() == 3) {
            MMNode m1 = inputs.get(0);
            MMNode m2 = inputs.get(1);
            MMNode m3 = inputs.get(2);
            MMNode tmp = new MMNode(m1, m2, SparsityEstimator.OpCode.MM);
            out = new MMNode(tmp, m3, SparsityEstimator.OpCode.MM);
            computation = (double) m1.getRows() * m1.getCols() * m2.getCols() + (double) m1.getRows() * m2.getCols() * m3.getCols();
            shuffle = (double) m1.getCols() * m1.getRows() + (double) m2.getCols() * m2.getRows() + (double) m3.getCols() * m3.getRows();
        }
        DataCharacteristics dc = estimator.estim(out);
        long r = reducerNumber(dc.getRows(), dc.getCols(), defaultBlockSize);
        cost = (ShuffleSpeed * shuffle + CpuSpeed * computation) / r;
        setMMNode(inst.output.getName(), out);
        return cost;
    }

    private static double eMapmmChainSPInstruction(MapmmChainSPInstruction inst) throws UnhandledOperatorException {
        // System.out.println("mapmm chain sp ");
        MMNode out = null;
        double cost = 0;
        if (inst.get_chainType() == MapMultChain.ChainType.XtXvy) {
            MMNode X = getMMNode(inst.get_input1().getName());
            DataCharacteristics dcX = estimator.estim(X);
            MMNode v = getMMNode(inst.get_input2().getName());
            MMNode y = getMMNode(inst.get_input3().getName());
            MMNode Xv = new MMNode(X, v, SparsityEstimator.OpCode.MM);
            MMNode Xvy = new MMNode(Xv, y, SparsityEstimator.OpCode.PLUS);
            MMNode Xt = new MMNode(X, SparsityEstimator.OpCode.TRANS);
            out = new MMNode(Xt, Xvy, SparsityEstimator.OpCode.MM);
            DataCharacteristics dc = estimator.estim(out);
            long r = (long) Math.ceil((double) dcX.getRows() * dcX.getCols() / defaultBlockSize);
            r = Math.max(r, 1);
            cost = sumTableCost(dc.getRows(), dc.getCols(), defaultBlockSize, r, dc.getSparsity());
        } else if (inst.get_chainType() == MapMultChain.ChainType.XtXv) {
            MMNode X = getMMNode(inst.get_input1().getName());
            DataCharacteristics dcX = estimator.estim(X);
            MMNode v = getMMNode(inst.get_input2().getName());
            MMNode Xv = new MMNode(X, v, SparsityEstimator.OpCode.MM);
            MMNode Xt = new MMNode(X, SparsityEstimator.OpCode.TRANS);
            out = new MMNode(Xt, Xv, SparsityEstimator.OpCode.MM);
            DataCharacteristics dc = estimator.estim(out);
            long r = (long) Math.ceil((double) dcX.getRows() * dcX.getCols() / defaultBlockSize);
            r = Math.max(r, 1);
            cost = sumTableCost(dc.getRows(), dc.getCols(), defaultBlockSize, r, dc.getSparsity());
        } else {
            System.out.println(inst.get_chainType());
            throw new UnhandledOperatorException(inst);
        }
        setMMNode(inst.get_output().getName(), out);
        return cost;
    }

//    private static double e( inst) {
//
//    }


    //以上是处理各种instruction的函数
    //////////////////////////////

    private static double sumTableCost(long rows, long cols, long blen, long r, double sparsity) {
        double size = (double) rows * cols;
        long numReducer = reducerNumber(rows, cols, blen);
        double shuffle = size * r * sparsity;
        double io = (r + 1) * size * sparsity;
        double cost = 0;
        cost += ShuffleSpeed * (shuffle + io) / numReducer;
        cost += CpuSpeed * size * r / numReducer;
        return cost;
    }

    private static long reducerNumber(long rows, long cols, long blen) {
        if (blen <= 0) return OptimizerUtils.getNumReducers(false);
        long nrb = (long) Math.ceil((double) rows / blen);
        long ncb = (long) Math.ceil((double) cols / blen);
        long numReducer = Math.min(nrb * ncb, OptimizerUtils.getNumReducers(false));
        numReducer = Math.max(numReducer, 1);
        return numReducer;
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
            System.out.println(prefix + "if begin");
            System.out.println(prefix + " predicate:");
            printInstruction(ipb.getPredicate(), depth + 2);
            System.out.println(prefix + " if body");
            for (ProgramBlock p : ipb.getChildBlocksIfBody()) {
                rPrintInstrunctions(p, depth + 2);
            }
            System.out.println(prefix + " else body");
            for (ProgramBlock p : ipb.getChildBlocksElseBody()) {
                rPrintInstrunctions(p, depth + 2);
            }
            System.out.println(prefix + "if end");
        } else if (pb instanceof WhileProgramBlock) {
            WhileProgramBlock wpb = (WhileProgramBlock) pb;
            System.out.println(prefix + "while begin");
            System.out.println(prefix + " predicate:");
            printInstruction(wpb.getPredicate(), depth + 2);
            System.out.println(prefix + " while body");
            for (ProgramBlock p : wpb.getChildBlocks()) {
                rPrintInstrunctions(p, depth + 2);
            }
            System.out.println(prefix + "while end");
        } else if (pb instanceof BasicProgramBlock) {
            System.out.println(prefix + "basic begin");
            BasicProgramBlock bpb = (BasicProgramBlock) pb;
            printInstruction(bpb.getInstructions(), depth + 1);
            System.out.println(prefix + "basic end");
        }
    }

    private static void printInstruction(ArrayList<Instruction> instructions, int depth) {
        String prefix = "";
        for (int i = 0; i < depth; i++) prefix = prefix + " ";
        for (Instruction instruction : instructions) {
            System.out.println(prefix +
                    instruction.getClass().getName().substring(38) +
                    "  " + instruction.getOpcode() +
                    "  " + instruction.getInstructionString());
        }
    }

}
