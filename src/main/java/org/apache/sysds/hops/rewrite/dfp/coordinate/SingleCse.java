package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.sysds.hops.Hop;

import java.util.ArrayList;
import java.util.HashSet;


public class SingleCse {
    public HashKey hash = null;
    public ArrayList<Range> ranges;
    public int last_index = 0;

    public Hop prototype = null;
    public Range protoRange = null;

    public String name = "";

    public SingleCse() {
        this.ranges = new ArrayList<>();
    }

    public SingleCse(HashKey hash, ArrayList<Range> ranges, Range newRange, int last_index) {
        this.hash = hash;
        this.ranges = (ArrayList<Range>) ranges.clone();
        this.ranges.add(newRange);
        this.last_index = last_index;
    }

    public SingleCse(Range range, Hop prototype) {
        this.ranges = new ArrayList<>();
        this.ranges.add(range);
        this.prototype = prototype;
        this.protoRange = range;
    }

    public boolean conflict(SingleCse other) {
        // 两者相交但是并不包含
        for (Range p : ranges) {
            for (Range q : other.ranges) {
                if (p.conflict(q))
                    return true;
            }
        }
        return false;
    }

    public boolean contain(SingleCse innner) {
        HashSet<ArrayList<Integer>> mask = new HashSet<>();
        for (Range or : ranges) {
            ArrayList<Integer> tmp = new ArrayList<>();
            for (Range ir : innner.ranges) {
                if (or.conflict(ir)) return false;
                if (or.contain(ir)) {
                    int m;
                    if (or.transpose) {
                        if (ir.transpose) m = ir.right - or.right;
                        else m = ir.left - or.right;
                    }else {
                        if (ir.transpose) m = or.left - ir.right;
                        else m = or.left - ir.left;
                    }
                    tmp.add(m);
                 //   System.out.println(or.toString() +" "+ ir.toString() +" "+(m));
                }
            }
            mask.add(tmp);
        }
        return mask.size() == 1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SingleCse: name=");
        sb.append(name);
//        sb.append(" hash=");
//        sb.append(hash);
        sb.append(" ranges=[");
        for (Range r : ranges) {
            sb.append(r.toString());
            sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }
}
