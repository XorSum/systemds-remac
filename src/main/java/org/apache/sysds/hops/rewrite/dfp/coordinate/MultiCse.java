package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

import java.util.ArrayList;

public class MultiCse {
    public ArrayList<SingleCse> cses = new ArrayList<>();
    long id = -1;
    int last_index = 0;

    public MultiCse() {
    }

    public MultiCse(SingleCse cse, int last_index,long id) {
        this.cses.add(cse);
        this.last_index = last_index;
        this.id = id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MultiCses:{").append(id).append("\n");
        for (SingleCse c : cses) {
            sb.append(c.toString());
            sb.append("\n");
        }
        sb.append("}\n");
        return sb.toString();
    }
}
