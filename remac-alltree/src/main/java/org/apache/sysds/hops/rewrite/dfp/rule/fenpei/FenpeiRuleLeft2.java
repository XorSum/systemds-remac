package org.apache.sysds.hops.rewrite.dfp.rule.fenpei;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashHopDag;


// a*b+a*c -> a*(b+c)
public class FenpeiRuleLeft2 implements MyRule {

    private OpOp2 operator;

    public FenpeiRuleLeft2(OpOp2 operator) {

        this.operator = operator;
    }

    @Override
    public Hop apply(Hop parent, Hop abac, int pos) {
        if (HopRewriteUtils.isBinary(abac, operator)) {
            Hop ab = abac.getInput().get(0);
            Hop ac = abac.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(ab)) {
                Hop a = ab.getInput().get(0);
                Hop b = ab.getInput().get(1);
                if (HopRewriteUtils.isMatrixMultiply(ac)) {
                    Hop a2 = ac.getInput().get(0);
                    Hop c = ac.getInput().get(1);
                    if (hashHopDag(a)==hashHopDag(a2)) {

                        Hop bc = HopRewriteUtils.createBinary(b, c, operator);
                        Hop result = HopRewriteUtils.createMatrixMultiply(a, bc);

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

    @Override
    public Boolean applicable(Hop parent, Hop abac, int pos) {
        if (HopRewriteUtils.isBinary(abac, operator)) {
            Hop ab = abac.getInput().get(0);
            Hop ac = abac.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(ab)) {
                Hop a = ab.getInput().get(0);
                if (HopRewriteUtils.isMatrixMultiply(ac)) {
                    Hop a2 = ac.getInput().get(0);
                    if (hashHopDag(a) == hashHopDag(a2)) {
                        return true;
                    }
                }

            }
        }
        return false;
    }
}