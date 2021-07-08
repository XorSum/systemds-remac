package org.apache.sysds.hops.rewrite.dfp.rule.transpose;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

public class TransposeMinusSplitRule implements MyRule {

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // t(x-y)->t(x)-t(y)
        if (HopRewriteUtils.isTransposeOperation(hi)) {
            Hop xy = hi.getInput().get(0);
            if (HopRewriteUtils.isBinary(xy, Types.OpOp2.MINUS)) {
                Hop x = xy.getInput().get(0);
                Hop y = xy.getInput().get(1);
                if (x.isMatrix() && y.isMatrix()) {
                    Hop tx = HopRewriteUtils.createTranspose(x);
                    Hop ty = HopRewriteUtils.createTranspose(y);
                    Hop result = HopRewriteUtils.createBinary(tx, ty, Types.OpOp2.MINUS);
                    if (parent != null) {
                        HopRewriteUtils.replaceChildReference(parent, hi, result);
                        HopRewriteUtils.cleanupUnreferenced(hi);
                    }
                    hi = result;
//                System.out.println("New Hop:");
//                System.out.println(Explain.explain(hi));
                }
            }
        }
        return hi;
    }

    @Override
    public Boolean applicable(Hop parent, Hop hi, int pos) {
        if (HopRewriteUtils.isTransposeOperation(hi)) {
            Hop xy = hi.getInput().get(0);
            if (HopRewriteUtils.isBinary(xy, Types.OpOp2.MINUS)) {
                Hop x = xy.getInput().get(0);
                Hop y = xy.getInput().get(1);
                return x.isMatrix() && y.isMatrix();
            }
        }
        return false;
    }
}
