package org.apache.sysds.hops.rewrite.dfp.rule.jiehe;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

//   (a*b)*c -> a*(b*c)
public class MatrixMultJieheRule2 implements MyRule {

    public MatrixMultJieheRule2() {

    }

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {
        // (a*b)*c -> a*(b*c)
       // System.out.println("www");
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
//            System.out.println(" operator found ");
            Hop ab = hi.getInput().get(0);
            Hop c = hi.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(ab)) {
                Hop a = ab.getInput().get(0);
                Hop b = ab.getInput().get(1);
            //    System.out.println("apply (a*b)*c -> a*(b*c) "+hi.getHopID());
              //  System.out.println("jiehelv");
                // create
                Hop bc = HopRewriteUtils.createMatrixMultiply(b,c);
                Hop result = HopRewriteUtils.createMatrixMultiply(a, bc);
                //System.out.println(Explain.explain(result));
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

    @Override
    public Boolean applicable(Hop parent, Hop hi, int pos) {
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
//            System.out.println(" operator found ");
            Hop ab = hi.getInput().get(0);
            return HopRewriteUtils.isMatrixMultiply(ab);
        }
        return false;
    }
}
