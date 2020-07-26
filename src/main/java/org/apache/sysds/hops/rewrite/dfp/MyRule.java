package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.hops.Hop;

public class MyRule {

    public Hop apply(Hop parent, Hop hi, int pos) {
//        if (( (BinaryOp) hi).getOp() == operator) {}
//        if (HopRewriteUtils.isBinary(hi,operator)) {}
        return hi;
    }

}
