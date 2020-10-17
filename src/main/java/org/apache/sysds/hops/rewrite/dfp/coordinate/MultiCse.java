package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

import java.util.ArrayList;

public  class MultiCse {
    public ArrayList<SingleCse> cses = new ArrayList<>();

    int last_index = 0;

    public MultiCse() {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MultiCses:{\n");
        for (SingleCse c : cses) {
            sb.append(c.toString());
            sb.append("\n");
        }
        sb.append("}\n");
        return sb.toString();
    }
}
