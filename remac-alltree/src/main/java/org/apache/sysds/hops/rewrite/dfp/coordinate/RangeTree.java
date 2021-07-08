package org.apache.sysds.hops.rewrite.dfp.coordinate;

import java.util.ArrayList;

public class RangeTree {
    public int left = -1;
    public int right = -1;
    public SingleCse singleCse = null;
    public ArrayList<RangeTree> children = new ArrayList<>();
    public boolean transpose;

    public RangeTree(int l,int r,SingleCse sc, boolean t) {
        left = l;
        right = r;
        singleCse = sc;
        transpose=t;
    }

    @Override
    public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append("{" + left + "," + right + ","  );
         for (RangeTree son: children){
             sb.append(son.toString());
         }
         sb.append("}");
         return sb.toString();
    }

}