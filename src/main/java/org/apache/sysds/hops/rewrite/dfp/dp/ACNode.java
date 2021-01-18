package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

public class ACNode {

    static class DRange {
        Pair<Integer, Integer> lRange;
        Pair<Integer, Integer> rRange;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DRange dRange = (DRange) o;
            return lRange.equals(dRange.lRange) &&
                    rRange.equals(dRange.rRange);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lRange, rRange);
        }
    }

    DRange dRange;

    static class AC {
        double thisCost;
        double accCost;
        HashSet<SingleCse> cses=new HashSet<>();
    }
    AC bestOp;
    AC minAC;
    AC certainAC;
    ArrayList<AC> uncertainAC = new ArrayList<>();

  //  ArrayList<OperatorNode> operatorNodes = new ArrayList<>();

}
