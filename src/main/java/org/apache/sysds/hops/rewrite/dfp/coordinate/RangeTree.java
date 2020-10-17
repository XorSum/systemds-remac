package org.apache.sysds.hops.rewrite.dfp.coordinate;

import java.util.ArrayList;

public class RangeTree {
    public int left = -1;
    public int right = -1;
    public SingleCse singleCse = null;
    public ArrayList<RangeTree> children = new ArrayList<>();

    public RangeTree(int l,int r,SingleCse sc) {
        left = l;
        right = r;
        singleCse = sc;
    }


    @Override
    public String toString() {
        return "(" + left + "," + right + ")";
    }
}