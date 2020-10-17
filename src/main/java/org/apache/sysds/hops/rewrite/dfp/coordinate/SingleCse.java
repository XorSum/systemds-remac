package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.utils.Hash;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static java.lang.Math.*;

public  class SingleCse {
    public HashKey hash=null;
    public ArrayList<Range> ranges=null;
    public int last_index = 0;

    public Hop prototype=null;
    public Range protoRange = null;

    public String name = "";

    public SingleCse(HashKey hash,ArrayList<Range> ranges, Range newRange,int last_index ) {
        this.hash = hash;
        this.ranges = (ArrayList<Range>)ranges.clone();
        this.ranges.add(newRange);
        this.last_index = last_index;
    }

    public SingleCse(Range range,Hop prototype) {
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
        for (Range p: ranges) {
            ArrayList<Integer> tmp = new ArrayList<>();
            for (Range q: innner.ranges) {
                if (p.conflict(q)) return false;
                if (p.contain(q)) {
                    tmp.add(q.left - p.left);
                }
            }
            mask.add(tmp);
        }
        return mask.size()==1;
    }

    @Override
    public String toString() {
        StringBuilder sb  = new StringBuilder();
        sb.append("SingleCse: name=");
        sb.append(name);
//        sb.append(" hash=");
//        sb.append(hash);
        sb.append(" ranges=[");
        for (Range r: ranges) {
            sb.append(r.toString());
            sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }
}
