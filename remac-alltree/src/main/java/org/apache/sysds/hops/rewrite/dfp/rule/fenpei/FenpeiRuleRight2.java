package org.apache.sysds.hops.rewrite.dfp.rule.fenpei;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashHopDag;


// b*a+c*a -> (b+c)*a
public class FenpeiRuleRight2 implements MyRule {

    private OpOp2 operator;

    public FenpeiRuleRight2(OpOp2 operator) {

        this.operator = operator;
    }

    @Override
    public Hop apply(Hop parent, Hop baca, int pos) {
        if (HopRewriteUtils.isBinary(baca, operator)) {
            Hop ba = baca.getInput().get(0);
            Hop ca = baca.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(ba)) {
                Hop b = ba.getInput().get(0);
                Hop a = ba.getInput().get(1);
                if (HopRewriteUtils.isMatrixMultiply(ca)) {
                    Hop c = ca.getInput().get(0);
                    Hop a2 = ca.getInput().get(1);
                    if (hashHopDag(a) == hashHopDag(a2)) {

                        Hop bc = HopRewriteUtils.createBinary(b, c, operator);
                        Hop result = HopRewriteUtils.createMatrixMultiply(bc, a);

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

    @Override
    public Boolean applicable(Hop parent, Hop baca, int pos) {

        if (HopRewriteUtils.isBinary(baca, operator)) {
            Hop ba = baca.getInput().get(0);
            Hop ca = baca.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(ba)) {
                Hop a = ba.getInput().get(1);
                if (HopRewriteUtils.isMatrixMultiply(ca)) {
                    Hop a2 = ca.getInput().get(1);
                    return hashHopDag(a) == hashHopDag(a2);
                }
            }
        }
        return false;
    }
}
