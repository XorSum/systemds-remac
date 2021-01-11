package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

public class OperatorNode {
    //        ArrayList<RangeNode> operands = new ArrayList<>();
//        Pair<Integer, Integer> range = null;
    ArrayList<Pair<Integer, Integer>> ranges = new ArrayList<>();
    HashSet<SingleCse> dependencies = new HashSet<>();
    HashSet<SingleCse> oldDependencies = new HashSet<>();
    Hop hop = null;
    double thisCost = Double.MAX_VALUE;
    double accCost = Double.MAX_VALUE;
    ArrayList<OperatorNode> inputs = new ArrayList<>();
    boolean isConstant = false;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperatorNode that = (OperatorNode) o;
        return Double.compare(that.thisCost, thisCost) == 0 &&
                Double.compare(that.accCost, accCost) == 0 &&
//                    range.equals(that.range) &&
                ranges.equals(that.ranges) &&
                Objects.equals(dependencies, that.dependencies);// &&
        //  Objects.equals(hop, that.hop) &&
        //   Objects.equals(inputs, that.inputs);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ON{");
        sb.append(hop.getOpString());
        sb.append(",");
        sb.append(thisCost);
        sb.append(",");
        sb.append(accCost);
        sb.append(",");
        sb.append(ranges);
        sb.append(",");
        sb.append(isConstant);
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
        sb.append("}\n");
        return sb.toString();
    }
}
