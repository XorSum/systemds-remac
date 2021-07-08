package org.apache.sysds.hops.rewrite.dfp.rule.jiehe;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.utils.Explain;

// a*(b*c) -> (a*b)*c
public class InplaceMatrixMultJieheRule implements MyRule {

    public InplaceMatrixMultJieheRule() {

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
//                System.out.println("apply  a*(b*c) -> (a*b)*c " + hi.getHopID());
//                System.out.println("all=" + MyExplain.myExplain(hi));
//                System.out.println("a=" + MyExplain.myExplain(a));
//                System.out.println("b=" + MyExplain.myExplain(b));
//                System.out.println("c=" + MyExplain.myExplain(c));
//                System.out.println("\n\n");


                hi.getInput().clear();
                bc.getParent().remove(hi);
                a.getParent().remove(hi);

                Hop ab = HopRewriteUtils.createMatrixMultiply(a, b);

                hi.getInput().add(ab);
                ab.getParent().add(hi);

                hi.getInput().add(c);
                c.getParent().add(hi);

            }
        }
        return hi;
    }

    @Override
    public Boolean applicable(Hop parent, Hop hi, int pos) {
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
            Hop bc = hi.getInput().get(1);
            return HopRewriteUtils.isMatrixMultiply(bc);
        }
        return false;
    }
}
