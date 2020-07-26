package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.HopsException;
import org.apache.sysds.hops.rewrite.HopRewriteRule;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.dfp.rule.*;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.sysds.hops.rewrite.dfp.MyUtils.deepCopyHopsDag;

public class RewriteDFP extends HopRewriteRule {
    @Override
    public ArrayList<Hop> rewriteHopDAGs(ArrayList<Hop> roots, ProgramRewriteStatus state) {
        //  System.out.println("bbbb");
        for (int i = 0; i < roots.size(); i++) {
            rewriteHopDAG(roots.get(i), state);
        }
        return roots;
    }

    @Override
    public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
        //  System.out.println("aaa");
        root = reorder(root);
        System.out.println("after reorder");
        System.out.println(Explain.explain(root));
        root = balance(root);
        System.out.println("after balance");
        System.out.println(Explain.explain(root));
        findAllSubExpression(root);
        return root;
    }

//    private void rule_rewritedfp(Hop hop, boolean descendFirst) {
//        if (hop.isVisited())
//            return;
//        for (int i = 0; i < hop.getInput().size(); i++) {
//            Hop hi = hop.getInput().get(i);
//            if (descendFirst)
//                rule_rewritedfp(hi, descendFirst);
//            findAllSubExpression(hi);
//            if (!descendFirst)
//                rule_rewritedfp(hi, descendFirst);
//        }
//        hop.setVisited();
//    }


    private static Hop reorder(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new TransposeSplitRule());
        HopRewriteUtils.isMatrixMultiply(hop);

        rules.add(new RemoveUnnecessaryTransposeRule());
        rules.add(new JieheRule(Types.OpOp2.MULT));
        hop = MyUtils.applyRule(hop, rules, 100);
        return hop;
    }

    private static Hop balance(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new BalanceMultiply4Rule());
        hop = MyUtils.applyRule(hop, rules, 100);
        return hop;
    }


    private static void getAllSubExpression(Hop hop, ArrayList<Hop> result) {
        if (hop.isVisited())
            return;
        result.add(hop);
        hop.setVisited();
        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop hi = hop.getInput().get(i);
            getAllSubExpression(hi, result);
        }
    }

    private static void findAllSubExpression(Hop hop) {

        // step 1. 找到所有的子表达式
        ArrayList<Hop> allSubExpression = new ArrayList<>();
        hop.resetVisitStatus();
        getAllSubExpression(hop, allSubExpression);
//        System.out.println("all of the sub expressions:");
//        for (int i = 0; i < allSubExpression.size(); i++) {
//            System.out.println(i);
//            Hop hi = allSubExpression.get(i);
//            System.out.println(Explain.explain(hi));
//        }

        // step 2. 判断子表达式是否相同
        for (int i = 0; i < allSubExpression.size(); i++) {
            for (int j = i + 1; j < allSubExpression.size(); j++) {
                Hop hi = allSubExpression.get(i);
                Hop hj = allSubExpression.get(j);
                Hop thj = HopRewriteUtils.createTranspose(hj);
                if (isSame(hi, hj) || isSame(hi, thj)) {
                    System.out.println("found equal sub expressions: " + i + " " + j);
                    System.out.println(Explain.explain(hi));
                    System.out.println(Explain.explain(hj));
                    System.out.println(Explain.explain(thj));
                    System.out.println(isSame(hi, hj));
                    System.out.println(isSame(hi, thj));
                }
            }
        }
//        if (allSubExpression.size()>=22) {
//            Hop h3 = allSubExpression.get(3);
//            Hop h22 = allSubExpression.get(22);
//            Hop th22 = HopRewriteUtils.createTranspose(h22);
//            System.out.println("h3 and h22:");
//            th22 = reorder(th22);
//            System.out.println(Explain.explain(h3));
//            System.out.println(Explain.explain(h22));
//            System.out.println(Explain.explain(th22));
//            System.out.println(isSame(h3,th22));
//        }

    }

    private static Hop createMC(ArrayList<Hop> hops, int i, int j) {
        Hop ret = hops.get(i);
        for (int k = i + 1; k <= j; k++) {
            ret = HopRewriteUtils.createMatrixMultiply(ret, hops.get(k));
        }
        return ret;
    }


    private static boolean isSame(Hop a, Hop b) {
//        System.out.println("same<");
        Hop aa = deepCopyHopsDag(a);
        aa = reorder(aa);
        //System.out.println(Explain.explain(aa));
        Hop bb = deepCopyHopsDag(b);
        bb = reorder(bb);
        //System.out.println(Explain.explain(bb));
        boolean ret = isSame_iter(aa, bb);
//        System.out.println("Ret=" + ret);
//        System.out.println(">same");
        return ret;
    }

    private static boolean isSame_iter(Hop a, Hop b) {
        if (a.equals(b)) return true;
        if (a.getInput().size() != b.getInput().size()) return false;
        if (!a.getInput().isEmpty()) {
            for (int i = 0; i < a.getInput().size(); i++) {
                if (isSame_iter(a.getInput().get(i), b.getInput().get(i)) == false) {
                    return false;
                }
            }
        }
//        System.out.println(a.getOpString());
//        System.out.println(b.getOpString());
        return a.getOpString().equals(b.getOpString());
    }


}
