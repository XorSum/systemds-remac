package org.apache.sysds.hops.rewrite.dfp.rule;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

// b*a+c*a -> (b+c)*a
public class FenpeiRuleRight2 extends MyRule {
    private OpOp2 star;
    private OpOp2 cross;

    public FenpeiRuleRight2(OpOp2 star, OpOp2 cross) {
        this.star = star;
        this.cross = cross;
    }

    @Override
    public Hop apply(Hop parent, Hop baca, int pos) {
        if (HopRewriteUtils.isBinary(baca, cross)) {
            Hop ba = baca.getInput().get(0);
            Hop ca = baca.getInput().get(1);
            if (HopRewriteUtils.isBinary(ba, star)) {
                Hop b = ba.getInput().get(0);
                Hop a = ba.getInput().get(1);
                if (HopRewriteUtils.isBinary(ca, star)) {
                    Hop c = ca.getInput().get(0);
                    Hop a2 = ca.getInput().get(1);
                    if (a.equals(a2)) {

                        Hop bc = HopRewriteUtils.createBinary(b, c, cross);
                        Hop result = HopRewriteUtils.createBinary(bc,a, star);

                        if (parent != null) {
                            HopRewriteUtils.replaceChildReference(parent, baca, result);
                            HopRewriteUtils.cleanupUnreferenced(baca);
                        }
                        baca = result;
                    }
                }
            }
        }
        return baca;
    }
}
