package org.apache.sysds.hops.rewrite.dfp.rule.scalar;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.utils.Explain;

public class ScalarRightMoveRule implements MyRule {

    @Override
    public Hop apply(Hop parent, Hop hop, int pos) {
        // (a*B)%*%C -> (B%*%C)*a
        // (B*a)%*%C -> (B%*%C)*a
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
//            System.out.println(" operator found ");
            Hop ab = hop.getInput().get(0);
            Hop c = hop.getInput().get(1);
            if (HopRewriteUtils.isScalarMatrixBinaryMult(ab)) {
                Hop a = ab.getInput().get(0);
                Hop b = ab.getInput().get(1);
                if (a.isMatrix()) {
                    a = ab.getInput().get(1);
                    b = ab.getInput().get(0);
                }

                // System.out.println("apply  a*(b*c) -> (a*b)*c "+hi.getHopID());
                // create
                Hop bc = HopRewriteUtils.createMatrixMultiply(b, c);
                Hop result = HopRewriteUtils.createBinary(bc,a, Types.OpOp2.MULT);
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
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
            Hop ab = hop.getInput().get(0);
            if (HopRewriteUtils.isScalarMatrixBinaryMult(ab))
                ans = true;
        }
//        System.out.println(Explain.explain(hop));
//        System.out.println("scalar matrix lift "+ ans);
        return ans;
    }
}
