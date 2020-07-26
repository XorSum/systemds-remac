package org.apache.sysds.hops.rewrite.dfp.rule;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

// a*b+a*c -> a*(b+c)
public class FenpeiRuleLeft2 extends MyRule {
    private OpOp2 star;
    private OpOp2 cross;

    public FenpeiRuleLeft2(OpOp2 star, OpOp2 cross) {
        this.star = star;
        this.cross = cross;
    }

    @Override
    public Hop apply(Hop parent, Hop abac, int pos) {
        if (HopRewriteUtils.isBinary(abac, cross)) {
            Hop ab = abac.getInput().get(0);
            Hop ac = abac.getInput().get(1);
            if (HopRewriteUtils.isBinary(ab, star)) {
                Hop a = ab.getInput().get(0);
                Hop b = ab.getInput().get(1);
                if (HopRewriteUtils.isBinary(ac, star)) {
                    Hop a2 = ac.getInput().get(0);
                    Hop c = ac.getInput().get(1);
                    if (a.equals(a2)) {

                        Hop bc = HopRewriteUtils.createBinary(b, c, cross);
                        Hop result = HopRewriteUtils.createBinary(a, bc, star);

                        if (parent != null) {
                            HopRewriteUtils.replaceChildReference(parent, abac, result);
                            HopRewriteUtils.cleanupUnreferenced(abac);
                        }
                        abac = result;
                    }
                }
            }
        }
        return abac;
    }
}
