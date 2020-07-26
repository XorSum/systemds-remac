package org.apache.sysds.hops.rewrite.dfp.rule;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

public class RemoveUnnecessaryTransposeRule extends MyRule {
    @Override
    public Hop apply(Hop parent, Hop ttx, int pos) {
        // t(t(x)) -> x
        if (HopRewriteUtils.isTransposeOperation(ttx)) {
            Hop tx = ttx.getInput().get(0);
            if (HopRewriteUtils.isTransposeOperation(tx)) {
//                System.out.println("found t(t(x))");
//                System.out.println("Old Hop:");
//                System.out.println(Explain.explain(parent));
                Hop x = tx.getInput().get(0);
                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, ttx, x);
                    HopRewriteUtils.cleanupUnreferenced(ttx);
                }
                    ttx = x;
//                System.out.println("New Hop:");
//                System.out.println(Explain.explain(hi));
            }
        }
        return ttx;
    }
}
