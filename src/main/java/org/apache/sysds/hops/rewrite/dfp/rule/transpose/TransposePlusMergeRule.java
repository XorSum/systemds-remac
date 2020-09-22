package org.apache.sysds.hops.rewrite.dfp.rule.transpose;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

public class TransposePlusMergeRule implements MyRule {

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // t(x)+t(y) -> t(x+y)
        if (HopRewriteUtils.isBinary(hi, Types.OpOp2.PLUS)) {
            Hop left = hi.getInput().get(0);
            Hop right = hi.getInput().get(1);
            if (HopRewriteUtils.isTransposeOperation(left) &&
                    HopRewriteUtils.isTransposeOperation(right)) {
                Hop x = left.getInput().get(0);
                Hop y = right.getInput().get(0);
                Hop tmp = HopRewriteUtils.createBinary(x, y, Types.OpOp2.PLUS);
                Hop result = HopRewriteUtils.createTranspose(tmp);
                if (parent != null)
                    HopRewriteUtils.replaceChildReference(parent, hi, result);
                hi = result;
            }
        }
        return hi;
    }

    @Override
    public Boolean applicable(Hop parent, Hop hi, int pos) {
        if (HopRewriteUtils.isBinary(hi, Types.OpOp2.PLUS)) {
            Hop left = hi.getInput().get(0);
            Hop right = hi.getInput().get(1);
            return HopRewriteUtils.isTransposeOperation(left) &&
                    HopRewriteUtils.isTransposeOperation(right);
        }
        return false;
    }
}
