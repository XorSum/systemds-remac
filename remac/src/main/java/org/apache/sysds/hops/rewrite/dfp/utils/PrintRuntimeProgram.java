package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.runtime.controlprogram.*;
import org.apache.sysds.runtime.instructions.Instruction;

import java.util.ArrayList;

public class PrintRuntimeProgram {

    protected static final Log LOG = LogFactory.getLog(PrintRuntimeProgram.class.getName());

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
