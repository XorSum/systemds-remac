package org.apache.sysds.hops.rewrite.dfp.rule.scalar;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.utils.Explain;

public class ScalarLeftMoveRule implements MyRule {

    @Override
    public Hop apply(Hop parent, Hop hop, int pos) {
        // A%*%(b*C) -> b*(A%*%C)
        // A%*%(C*b) -> b*(A%*%C)
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
//            System.out.println(" operator found ");
            Hop a = hop.getInput().get(0);
            Hop bc = hop.getInput().get(1);
            if (HopRewriteUtils.isScalarMatrixBinaryMult(bc)) {
                Hop b = bc.getInput().get(0);
                Hop c = bc.getInput().get(1);
                if (b.isMatrix()) {
                    b = bc.getInput().get(1);
                    c = bc.getInput().get(0);
                }

                // System.out.println("apply  a*(b*c) -> (a*b)*c "+hi.getHopID());
                // create
                Hop ac = HopRewriteUtils.createMatrixMultiply(a, c);
                Hop result = HopRewriteUtils.createBinary(b,ac, Types.OpOp2.MULT);
//                System.out.println(Explain.explain(result));
                // replace
                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, hop, result);
                    HopRewriteUtils.cleanupUnreferenced(hop);
                }
                hop = result;
            }
        }
        return hop;
    }

    @Override
    public Boolean applicable(Hop parent, Hop hop, int pos) {
        boolean ans = false;
        if (HopRewriteUtils.isMatrixMultiply(hop) && hop.getInput().size()==2 ) {
            Hop bc = hop.getInput().get(1);
            if (HopRewriteUtils.isScalarMatrixBinaryMult(bc))
                ans = true;
        }
//        System.out.println(Explain.explain(hop));
//        System.out.println("scalar matrix lift "+ ans);
        return ans;
    }
}
