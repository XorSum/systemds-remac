package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteRule;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;

import java.util.ArrayList;

public class RewriteTempHopsDag extends HopRewriteRule {

    @Override
    public ArrayList<Hop> rewriteHopDAGs(ArrayList<Hop> roots, ProgramRewriteStatus state) {
        for (Hop hop : roots) {
            rewriteHopDAG(hop, state);
        }
        return roots;
    }

    @Override
    public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
        System.out.println("MEMORY ESTIMATE  " + MyExplain.myExplain(root));
        rEstimate(root);
        return root;
    }

    private void rEstimate(Hop root) {
        double m = root.getMemEstimate();
        System.out.println("memo estimate " + root.getName() + " " + m);
        for (Hop h : root.getInput()) {
            rEstimate(h);
        }
    }




}
