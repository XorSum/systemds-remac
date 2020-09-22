package org.apache.sysds.hops.rewrite.dfp.rule.jiehe;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;


// a*(b*c) -> (a*b)*c
public class JieheRule implements MyRule {
    private OpOp2 operator;

    public JieheRule(OpOp2 operator) {
        this.operator = operator;
    }

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // a*(b*c) -> (a*b)*c
       // System.out.println("www");
        if (HopRewriteUtils.isBinary(hi, operator)) {
            System.out.println(" operator found ");
            Hop a = hi.getInput().get(0);
            Hop bc = hi.getInput().get(1);
            if (HopRewriteUtils.isBinary(bc, operator)) {
                Hop b = bc.getInput().get(0);
                Hop c = bc.getInput().get(1);
         //       System.out.println("jiehelv");
                // create
                Hop ab = HopRewriteUtils.createBinary(a, b, operator);
                Hop abc = HopRewriteUtils.createBinary(ab, c, operator);
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

    @Override
    public Boolean applicable(Hop parent, Hop hi, int pos) {
        if (HopRewriteUtils.isBinary(hi, operator)) {
            System.out.println(" operator found ");
            Hop bc = hi.getInput().get(1);
            return HopRewriteUtils.isBinary(bc, operator);
        }
        return false;
    }

}
