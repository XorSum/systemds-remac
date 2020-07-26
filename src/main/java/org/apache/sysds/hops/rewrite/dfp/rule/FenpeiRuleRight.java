package org.apache.sysds.hops.rewrite.dfp.rule;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

// (b+c)*a -> b*a+c*a
public class FenpeiRuleRight extends MyRule {
    private OpOp2 star;
    private OpOp2 cross;

    public FenpeiRuleRight(OpOp2 star, OpOp2 cross) {
        this.star = star;
        this.cross = cross;
    }

    @Override
    public Hop apply(Hop parent, Hop bca, int pos) {
        if (HopRewriteUtils.isBinary(bca, star)) {
            Hop bc = bca.getInput().get(0);
            Hop a = bca.getInput().get(1);
            if (HopRewriteUtils.isBinary(bc, cross)) {
                Hop b = bc.getInput().get(0);
                Hop c = bc.getInput().get(1);

                Hop ba = HopRewriteUtils.createBinary(b, a, star);
                Hop ca = HopRewriteUtils.createBinary(c, a, star);
                Hop result = HopRewriteUtils.createBinary(ba, ca, cross);

                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, bca, result);
                    HopRewriteUtils.cleanupUnreferenced(bca);
                }
                bca = result;
            }
        }
        return bca;
    }
}
