package org.apache.sysds.hops.rewrite.dfp.rule.fenpei;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

// a*(b+c) -> a*b+a*c
public class FenpeiRuleLeft implements MyRule {

    private OpOp2 cross;

    public FenpeiRuleLeft(OpOp2 cross) {

        this.cross = cross;
    }

    @Override
    public Hop apply(Hop parent, Hop abc, int pos) {
        if (HopRewriteUtils.isMatrixMultiply(abc)) {
            Hop a = abc.getInput().get(0);
            Hop bc = abc.getInput().get(1);
            if (HopRewriteUtils.isBinary(bc, cross)) {
                Hop b = bc.getInput().get(0);
                Hop c = bc.getInput().get(1);

                Hop ab = HopRewriteUtils.createMatrixMultiply(a, b);
                Hop ac = HopRewriteUtils.createMatrixMultiply(a, c);
                Hop result = HopRewriteUtils.createBinary(ab, ac, cross);

                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, abc, result);
                    HopRewriteUtils.cleanupUnreferenced(abc);
                }
                abc = result;
            }
        }
        return abc;
    }

    @Override
    public Boolean applicable(Hop parent, Hop abc, int pos) {
        if (HopRewriteUtils.isMatrixMultiply(abc)) {
            Hop bc = abc.getInput().get(1);
            return HopRewriteUtils.isBinary(bc, cross);
        }
        return false;
    }
}
