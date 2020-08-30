package org.apache.sysds.hops.rewrite.dfp.rule.jiehe;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.utils.Explain;

// a*(b*c) -> (a*b)*c
public class MatrixMultJieheRule extends MyRule {

    public MatrixMultJieheRule() {

    }

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // a*(b*c) -> (a*b)*c
       // System.out.println("www");
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
//            System.out.println(" operator found ");
            Hop a = hi.getInput().get(0);
            Hop bc = hi.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(bc)) {
                Hop b = bc.getInput().get(0);
                Hop c = bc.getInput().get(1);
               // System.out.println("apply  a*(b*c) -> (a*b)*c "+hi.getHopID());
                // create
                Hop ab = HopRewriteUtils.createMatrixMultiply(a, b);
                Hop result = HopRewriteUtils.createMatrixMultiply(ab, c);
//                System.out.println(Explain.explain(result));
                // replace
                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, hi, result);
                    HopRewriteUtils.cleanupUnreferenced(hi);
                }
                hi = result;
            }
        }
        return hi;
    }
}
