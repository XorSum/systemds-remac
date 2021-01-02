package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.expressions.Sin;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.BinaryOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.MySolution;
import org.apache.sysds.hops.rewrite.dfp.coordinate.Range;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.runtime.matrix.operators.Operator;
import org.apache.sysds.utils.Explain;
import org.mortbay.io.nio.SelectorManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.OptionalInt;

public class CostTree {


    public void testCostTree(ArrayList<Pair<Hop, SingleCse>> pairs) {
        for (Pair<Hop, SingleCse> p : pairs) {
            addHop(p.getLeft(), p.getRight());
//            System.out.println("added");
//            System.out.println("range2Node size = "+range2Node.size());
//            System.out.println(range2Node.keySet());
//            OptionalInt min = range2Node.keySet().stream().mapToInt(p->p.getLeft()).min();
//            OptionalInt max = range2Node.keySet().stream().mapToInt(p->p.getRight()).max();
//            System.out.println(min+"  "+max);
        }
        for (HashMap.Entry<Pair<Integer, Integer>, RangeNode> entry : range2Node.entrySet()) {
            System.out.println("range: " + entry.getKey().getLeft() + " " + entry.getKey().getRight());
            System.out.println(entry.getValue().options);
        }
        System.out.println("done");
    }

    public static class RangeNode {
        Pair<Integer, Integer> range;
        ArrayList<OperatorNode> options = new ArrayList<>();
        ArrayList<OperatorNode> bestOptions = new ArrayList<>();
    }

    public static class OperatorNode {
        //        ArrayList<RangeNode> operands = new ArrayList<>();
        Pair<Integer, Integer> range = null;
        HashSet<SingleCse> dependencies = new HashSet<>();
        Hop hop = null;
        double thisCost = 1000000;
        double accCost = 0;
        ArrayList<OperatorNode> inputs = new ArrayList<>();

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("ON{");
            sb.append(hop.getOpString());
            sb.append(",");
            sb.append(thisCost);
            sb.append(",");
            sb.append(accCost);
            sb.append(",[");
            for (SingleCse singleCse:dependencies){
                sb.append(singleCse.name);
            }
            sb.append("],");
//            sb.append(dependencies);
            sb.append("[");
            for (OperatorNode on : inputs) {
                sb.append(",");
                sb.append(on.range);
            }
            sb.append("]");
            sb.append("}\n");
            return sb.toString();
        }
    }

    public HashMap<Pair<Integer, Integer>, RangeNode> range2Node = new HashMap<>();

    public CostTree() {
    }

    int count = 0;

    public void addHop(Hop hop, SingleCse singleCse) {
        System.out.println("single cse: " + singleCse);
        HashMap<Hop, ArrayList<Pair<Integer, Integer>>> hop2index = new HashMap<>();
        MutableInt opIndex = new MutableInt(0);
        count = 0;
        rAddHop(hop, opIndex, hop2index, singleCse, false);
        if (count != singleCse.ranges.size()) {
//            System.out.println(singleCse);
//            System.out.println(hop2index);
//            System.out.println(Explain.explain(hop));
            System.out.println("xx");
        }
    }

    public OperatorNode rAddHop(Hop hop, MutableInt opIndex, HashMap<Hop, ArrayList<Pair<Integer, Integer>>> hop2index, SingleCse singleCse, boolean transpose) {
        int begin, end;
        OperatorNode operatorNode = new OperatorNode();
        operatorNode.hop = hop;
        if (HopRewriteUtils.isTransposeOperation(hop)) {
            return rAddHop(hop.getInput().get(0), opIndex, hop2index, singleCse, !transpose);
        } else if (HopRewriteUtils.isUnary(hop, Types.OpOp1.CAST_AS_SCALAR) ||
                Judge.isWrite(hop)) {
            return rAddHop(hop.getInput().get(0), opIndex, hop2index, singleCse, transpose);
        } else if (hop instanceof LiteralOp) {
            return null;
        } else if (Judge.isLeafMatrix(hop)) {
            begin = opIndex.getValue();
            opIndex.increment();
            end = opIndex.getValue() - 1;
            operatorNode.thisCost = 0;
        } else {
            begin = opIndex.getValue();
            if (!transpose) {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    OperatorNode tmp = rAddHop(hop.getInput().get(i), opIndex, hop2index, singleCse, transpose);
                    if (tmp == null) continue;
                    operatorNode.inputs.add(tmp);
                    operatorNode.dependencies.addAll(tmp.dependencies);
                    operatorNode.accCost += tmp.accCost;
                }
            } else {
                for (int i = hop.getInput().size() - 1; i >= 0; i--) {
                    OperatorNode tmp = rAddHop(hop.getInput().get(i), opIndex, hop2index, singleCse, transpose);
                    if (tmp == null) continue;
                    operatorNode.inputs.add(tmp);
                    operatorNode.dependencies.addAll(tmp.dependencies);
                    operatorNode.accCost += tmp.accCost;
                }
            }
            end = opIndex.getValue() - 1;
            boolean isCse = isCse(begin, end, singleCse);
            double cost = estimateCost(hop); // todo: estimate cost
            if (isCse) {
                count++;
                operatorNode.thisCost = cost / singleCse.ranges.size();
                operatorNode.dependencies.add(singleCse);
            } else {
                operatorNode.thisCost = cost;
            }
            operatorNode.accCost += operatorNode.thisCost;
        }
        Pair<Integer, Integer> range = Pair.of(begin, end);
        // System.out.println(hop.getHopID()+" "+range);
        operatorNode.range = range;
        if (range2Node.containsKey(range)) {
            RangeNode rangeNode = range2Node.get(range);
            if (operatorNode != null) rangeNode.options.add(operatorNode);
        } else {
            RangeNode rangeNode = new RangeNode();
            rangeNode.range = range;
            if (operatorNode != null) rangeNode.options.add(operatorNode);
            range2Node.put(range, rangeNode);
        }

        if (!hop2index.containsKey(hop)) {
            hop2index.put(hop, new ArrayList<>());
        }
        hop2index.get(hop).add(range);
        return operatorNode;
    }

    public double estimateCost(Hop hop) {
        double cost;
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
            Hop l = hop.getInput().get(0);
            Hop r = hop.getInput().get(1);
            cost = l.getDim1() * l.getDim2() * r.getDim2();
        } else {
            cost = hop.getDim1() * hop.getDim2();
        }
        return cost;
    }

    boolean isCse(Integer begin, Integer end, SingleCse singleCse) {
        boolean isCse = false;
        for (Range range : singleCse.ranges) {
            if ((range.left == begin && range.right == end) ||
                    (range.left == end && range.right == begin)) {
                isCse = true;
            }
        }
        return isCse;
    }

    void selectBest() {

    }


}
