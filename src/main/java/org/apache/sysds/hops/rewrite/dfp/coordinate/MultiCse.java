package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

import java.util.ArrayList;

public  class MultiCse {
    public ArrayList<SingleCse> cses = new ArrayList<>();
    public ArrayList<Hop> hops;
    public boolean[][] contain;
    int last_index = 0;

    public MultiCse() {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("cses:\n");
        for (SingleCse c : cses) {
            for (int i = c.ranges.get(0).left; i <= c.ranges.get(0).right; i++) {
                Hop h = RewriteCoordinate.leaves.get(i).hop;
                if (!HopRewriteUtils.isTransposeOperation(h)) {
                    sb.append(h.getName() + " ");
                } else {
                    h = h.getInput().get(0);
                    sb.append("t(" + h.getName() + ") ");
                }
            }
            for (Range r : c.ranges) {
                sb.append(r + " ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
