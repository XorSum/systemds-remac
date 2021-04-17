package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

import static org.apache.sysds.hops.rewrite.dfp.dp.NodeCost.dts;

public class OperatorNode {
    //    ArrayList<RangeNode> operands = new ArrayList<>();
//    Pair<Integer, Integer> range = null;
    DRange dRange = null;
    //    ArrayList<Pair<Integer, Integer>> ranges = new ArrayList<>();
    public HashSet<SingleCse> dependencies = new HashSet<>();
    public HashSet<SingleCse> oldDependencies = new HashSet<>();
    //Hop hop = null;
    ArrayList<Hop> hops = new ArrayList<>();
    double thisCost = Double.MAX_VALUE;
    public double accCost = Double.MAX_VALUE;
    public NodeCost thisCostDetails = NodeCost.INF();
    public NodeCost accCostDetails = NodeCost.INF();
    ArrayList<OperatorNode> inputs = new ArrayList<>();
    boolean isConstant = false;
    boolean isTranspose = false;
    boolean isXtXv = false;
    boolean shouldCollect = false;
    boolean isSpark = false;
    boolean isUsedByCp = false;
    AggBinaryOp.MMultMethod method = null;
    MMNode mmNode = null;
//    SparsityEstimator.OpCode opCode = null;
    int partitionNumber = -1;
    boolean cseTransposeType = false;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ON{");
        sb.append("[");
        for (Hop h : hops) {
            sb.append(h.getOpString());
            sb.append(" ");
            sb.append(h.getHopID());
            sb.append(",");
        }
        sb.append("],");
        if (mmNode != null) {
            sb.append("sparsity=");
            sb.append(mmNode.getDataCharacteristics().getSparsity());
            sb.append(",");
            sb.append(mmNode.getDataCharacteristics());
            sb.append(",cis="+ mmNode.mm_intern_sparsity);
            sb.append(",");
        }
        sb.append(dts(thisCost));
        sb.append(",");
//        sb.append(dts(accCost));
//        sb.append(",");
        sb.append(thisCostDetails);
        sb.append(",");
//        sb.append(accCostDetails);
//        sb.append(",");
//        sb.append(range);
        sb.append(dRange);
        if (method != null) {
            sb.append("," + method);
        }
        if (isConstant) sb.append(",constant");
        if (isTranspose) sb.append(",transpose");
        if (shouldCollect) sb.append(",collect");
        if (isUsedByCp) sb.append(",usedByCp");
        if (isSpark) sb.append(",isSpark");
        sb.append(",[");
        for (SingleCse singleCse : dependencies) {
            sb.append(singleCse.name);
            sb.append(",");
            sb.append(singleCse.ranges);
        }
        sb.append("]");

        sb.append(",[");
        for (SingleCse singleCse : oldDependencies) {
            sb.append(singleCse.name);
            sb.append(",");
            sb.append(singleCse.ranges);
        }
        sb.append("],");

//            sb.append(dependencies);
//            sb.append("[");
//            for (OperatorNode on : inputs) {
//                sb.append(",");
//                sb.append(on.ranges);
//            }
//            sb.append("]");
        sb.append("}");
        return sb.toString();
    }


    protected OperatorNode copy()  {
        OperatorNode node = new OperatorNode();
        node.dRange = this.dRange;
        node.dependencies = (HashSet<SingleCse>) this.dependencies.clone();
        node.oldDependencies = (HashSet<SingleCse>) this.oldDependencies.clone();
        node.hops = this.hops;
        node.thisCost = this.thisCost;
        node. accCost = this.accCost;
        node. thisCostDetails = this.thisCostDetails.clone();
        node.accCostDetails = this.accCostDetails.clone();
        node.inputs = this.inputs;
        node. isConstant = this.isConstant;
        node.isTranspose = this.isTranspose;
        node.isXtXv = this.isXtXv;
        node.shouldCollect = this.shouldCollect;
        node.isSpark = this.isSpark;
        node.isUsedByCp = this.isUsedByCp;
        node. method = this.method;
        node. mmNode = this.mmNode;
        node. partitionNumber = this.partitionNumber;
        node. cseTransposeType = this.cseTransposeType;
        return node;
    }

    boolean lessThan(OperatorNode that) {
        if (accCost+1e-3<that.accCost) return true;
        if (dependencies.size()+oldDependencies.size()<that.dependencies.size()+that.oldDependencies.size()) return true;
        return false;
    }

}
