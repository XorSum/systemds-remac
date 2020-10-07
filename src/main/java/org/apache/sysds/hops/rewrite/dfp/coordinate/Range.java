package org.apache.sysds.hops.rewrite.dfp.coordinate;

public  class Range {
    public int left;
    public int right;

    public static Range of(int l, int r) {
        Range range = new Range();
        range.left = l;
        range.right = r;
        return range;
    }

    @Override
    public String toString() {
        return "(" + left + "," + right + ")";
    }
}