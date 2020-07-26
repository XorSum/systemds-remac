package org.apache.sysds.hops.rewrite.dfp.rule;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

// a*(b+c) -> a*b+a*c
public class FenpeiRuleLeft extends MyRule {
    private OpOp2 star;
    private OpOp2 cross;

    public FenpeiRuleLeft(OpOp2 star, OpOp2 cross) {
        this.star = star;
        this.cross = cross;
    }

    @Override
    public Hop apply(Hop parent, Hop abc, int pos) {
        if (HopRewriteUtils.isBinary(abc, star)) {
            Hop a = abc.getInput().get(0);
            Hop bc = abc.getInput().get(1);
            if (HopRewriteUtils.isBinary(bc, cross)) {
                Hop b = bc.getInput().get(0);
                Hop c = bc.getInput().get(1);

                Hop ab = HopRewriteUtils.createBinary(a, b, star);
                Hop ac = HopRewriteUtils.createBinary(a, c, star);
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
}
