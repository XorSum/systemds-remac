package org.apache.sysds.hops.rewrite.dfp.costmodel;

import org.apache.sysds.runtime.instructions.Instruction;

public class UnhandledOperatorException extends Exception {
    public UnhandledOperatorException( Instruction inst) {
        super("This instruction is not handled: " + inst.getClass().getName());
    }
}
