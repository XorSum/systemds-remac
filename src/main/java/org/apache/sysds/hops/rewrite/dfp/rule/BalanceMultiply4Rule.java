package org.apache.sysds.hops.rewrite.dfp.rule;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.utils.Explain;

public class BalanceMultiply4Rule extends MyRule {
    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // ((a*b)*c)*d -> (a*b)*(c*d)
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
            Hop abc = hi.getInput().get(0);
            Hop d = hi.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(abc)) {
                Hop ab = abc.getInput().get(0);
                Hop c = abc.getInput().get(1);
                if (HopRewriteUtils.isMatrixMultiply(ab)) {
                    System.out.println(" balance 4 found ");
                    System.out.println(Explain.explain(hi));
                    System.out.println("---");
                    // create
                    Hop cd = HopRewriteUtils.createMatrixMultiply(c, d);
                    Hop abcd = HopRewriteUtils.createMatrixMultiply(ab, cd);

                    System.out.println(Explain.explain(abcd));
                    // replace
                    if (parent != null) {
                        HopRewriteUtils.replaceChildReference(parent, hi, abcd);
                        HopRewriteUtils.cleanupUnreferenced(hi);
                    }
                    hi = abcd;
                }
            }
        }
        return hi;
    }
}
