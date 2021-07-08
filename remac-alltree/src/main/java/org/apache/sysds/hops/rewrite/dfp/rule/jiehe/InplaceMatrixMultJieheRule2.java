package org.apache.sysds.hops.rewrite.dfp.rule.jiehe;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;

//   (a*b)*c -> a*(b*c)
public class InplaceMatrixMultJieheRule2 implements MyRule {

    public InplaceMatrixMultJieheRule2() {

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
//                System.out.println("apply (a*b)*c -> a*(b*c) " + hi.getHopID());
//                System.out.println("all=" + MyExplain.myExplain(hi));
//                System.out.println("a=" + MyExplain.myExplain(a));
//                System.out.println("b=" + MyExplain.myExplain(b));
//                System.out.println("c=" + MyExplain.myExplain(c));
//                System.out.println("\n\n");


                hi.getInput().clear();
                ab.getParent().remove(hi);
                c.getParent().remove(hi);

                Hop bc = HopRewriteUtils.createMatrixMultiply(b, c);

                hi.getInput().add(a);
                a.getParent().add(hi);

                hi.getInput().add(bc);
                bc.getParent().add(hi);
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
