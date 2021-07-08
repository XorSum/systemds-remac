package org.apache.sysds.hops.rewrite.dfp.costmodel;

import org.apache.sysds.runtime.instructions.cp.CPOperand;

import java.util.ArrayList;

public class CostNode {

    public ArrayList<CostNode> children;
    public OpType opType;
    CPOperand cpOperand;


    CostNode() {}


}
