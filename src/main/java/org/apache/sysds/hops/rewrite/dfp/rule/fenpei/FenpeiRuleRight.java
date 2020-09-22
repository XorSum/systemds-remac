package org.apache.sysds.hops.rewrite.dfp.rule.fenpei;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

// (b+c)*a -> b*a+c*a
public class FenpeiRuleRight implements MyRule {

    private OpOp2 cross;

    public FenpeiRuleRight(OpOp2 cross) {

        this.cross = cross;
    }

    @Override
    public Hop apply(Hop parent, Hop bca, int pos) {
        if (HopRewriteUtils.isMatrixMultiply(bca)) {
            Hop bc = bca.getInput().get(0);
            Hop a = bca.getInput().get(1);
            if (HopRewriteUtils.isBinary(bc, cross)) {
                Hop b = bc.getInput().get(0);
                Hop c = bc.getInput().get(1);

                Hop ba = HopRewriteUtils.createMatrixMultiply(b, a);
                Hop ca = HopRewriteUtils.createMatrixMultiply(c, a);
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

    @Override
    public Boolean applicable(Hop parent, Hop bca, int pos) {
        if (HopRewriteUtils.isMatrixMultiply(bca)) {
            Hop bc = bca.getInput().get(0);
            return HopRewriteUtils.isBinary(bc, cross);
        }
        return false;
    }
}
