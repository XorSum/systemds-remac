package org.apache.sysds.hops.rewrite.dfp.rule.transpose;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

public class TransposeScalarMatrixMultSplitRule extends MyRule {

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // t(x*Y)-> x*t(Y)
        // t(Y*x)-> x*t(Y)
        if (HopRewriteUtils.isTransposeOperation(hi)) {
            Hop xy = hi.getInput().get(0);
            if (HopRewriteUtils.isScalarMatrixBinaryMult(xy)) {
                Hop x = xy.getInput().get(0);
                Hop y = xy.getInput().get(1);
                if (x.isMatrix()) {
                     x = xy.getInput().get(1);
                     y = xy.getInput().get(0);
                }
                Hop ty = HopRewriteUtils.createTranspose(y);
                Hop result = HopRewriteUtils.createBinary(x,ty, Types.OpOp2.MULT);
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
