package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;


// (a*b)*c -> a*(b*c)
public class JieheRule2 extends MyRule {
    private OpOp2 operator;

    public JieheRule2(OpOp2 operator) {
        this.operator = operator;
    }

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // (a*b)*c -> a*(b*c)
        if (HopRewriteUtils.isBinary(hi, operator)) {
            Hop ab = hi.getInput().get(0);
            Hop c = hi.getInput().get(1);
            if (HopRewriteUtils.isBinary(ab, operator)) {
                Hop a = ab.getInput().get(0);
                Hop b = ab.getInput().get(1);
                // create
                Hop bc = HopRewriteUtils.createBinary(b, c, operator);
                Hop abc = HopRewriteUtils.createBinary(a, bc, operator);
                // replace
                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, hi, abc);
                    HopRewriteUtils.cleanupUnreferenced(hi);
                }
                hi = abc;
            }
        }
        return hi;
    }

}
