package org.apache.sysds.hops.rewrite.dfp.rule.transpose;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

public class TransposeMultMergeRule extends MyRule {

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // t(y)*t(x) -> t(x*y)
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
            Hop left = hi.getInput().get(0);
            Hop right = hi.getInput().get(1);
            if (HopRewriteUtils.isTransposeOperation(left) &&
                    HopRewriteUtils.isTransposeOperation(right)) {
                Hop y = left.getInput().get(0);
                Hop x = right.getInput().get(0);
                Hop tmp = HopRewriteUtils.createMatrixMultiply(x, y);
                Hop result = HopRewriteUtils.createTranspose(tmp);
                if (parent != null)
                    HopRewriteUtils.replaceChildReference(parent, hi, result);
                hi = result;
            }
        }
        return hi;
    }
}
