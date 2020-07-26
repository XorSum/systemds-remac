package org.apache.sysds.hops.rewrite.dfp.rule.transpose;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

public class TransposeMultSplitRule extends MyRule {

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // t(x*y)->t(y)*t(x)
        if (HopRewriteUtils.isTransposeOperation(hi)) {
            Hop xy = hi.getInput().get(0);
            if (HopRewriteUtils.isMatrixMultiply(xy)) {
                Hop x = xy.getInput().get(0);
                Hop y = xy.getInput().get(1);
                Hop tx = HopRewriteUtils.createTranspose(x);
                Hop ty = HopRewriteUtils.createTranspose(y);
                Hop result = HopRewriteUtils.createMatrixMultiply(ty, tx);
                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, hi, result);
                    HopRewriteUtils.cleanupUnreferenced(hi);
                }
                hi = result;
//                System.out.println("New Hop:");
//                System.out.println(Explain.explain(hi));
            }
        }
        return hi;
    }
}
