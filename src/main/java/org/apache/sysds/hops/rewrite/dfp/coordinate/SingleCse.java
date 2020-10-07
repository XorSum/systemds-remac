package org.apache.sysds.hops.rewrite.dfp.coordinate;

import java.util.ArrayList;

import static java.lang.Math.max;
import static java.lang.Math.min;

public  class SingleCse {
    public HashKey hash;
    public ArrayList<Range> ranges;
    public int last_index = 0;

    public SingleCse(HashKey hash) {
        this.hash = hash;
        this.ranges = new ArrayList<>();
    }

    public boolean intersect(SingleCse other) {
        for (Range p : ranges) {
            for (Range q : other.ranges) {
                boolean a = max(p.left, q.left) <= min(p.right, q.right);
                boolean b = (p.left <= q.left && p.right >= q.right) || (q.left <= p.left && q.right >= p.right);
                if (a && !b) return true;
//                   if (a) return true;
            }
        }
        return false;
    }

    public boolean contain(SingleCse innner) {
        if (intersect(innner)) return false;
        for (Range p : ranges) {
            for (Range q : innner.ranges) {
                boolean b = p.left <= q.left && p.right >= q.right;
                if (b) return true;
            }
        }
        return false;
    }
}
