package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteRule;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.dfp.rule.*;
import org.apache.sysds.hops.rewrite.dfp.rule.fenpei.FenpeiRuleLeft;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule2;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeMultSplitRule;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;

import static org.apache.sysds.hops.rewrite.dfp.MyUtils.deepCopyHopsDag;

public class RewriteDFP extends HopRewriteRule {
    @Override
    public ArrayList<Hop> rewriteHopDAGs(ArrayList<Hop> roots, ProgramRewriteStatus state) {
        System.out.println("bbbb");
        for (int i = 0; i < roots.size(); i++) {
            Hop hi = roots.get(i);
            hi = rewriteDFP(hi, state);
            roots.set(i, hi);
        }
        return roots;
    }

    @Override
    public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
        return rewriteDFP(root, state);
    }


    public Hop rewriteDFP(Hop trueroot, ProgramRewriteStatus state) {
        if (trueroot == null) return trueroot;
        //  System.out.println("aaa");
        Hop root = deepCopyHopsDag(trueroot);
        root = reorder(root);
//        System.out.println("after reorder");
//        root.resetVisitStatus();
//        System.out.println(Explain.explain(root));

//        Hop tmp = deepCopyHopsDag(root);
//        tmp = double_jiehe(tmp);
//        tmp.resetVisitStatus();
//        System.out.println("TMP<<<");
//        System.out.println(Explain.explain(tmp));
//        System.out.println(">>>");
//        Hop subExp = checkCommonSubExp(tmp);
//        if (subExp != null) {
//            replaceCommonSubExp(tmp, subExp);
//        }
//        root = tmp;


        for (int i = 0; i < 30; i++) {
            System.out.println("Round "+i);
            Hop tmp2 = deepCopyHopsDag(root);
            tmp2 = random_change(tmp2);
           // tmp2.resetVisitStatus();
//            System.out.println("TMP2<<<");
//            System.out.println(Explain.explain(tmp2));
//            System.out.println(">>>");
            findCommonSubExp(tmp2);
//            if (subexp != null) {
//                // root = replaceCommonSubExp(tmp2, subexp);
//                System.out.println("found");
//                subexp.resetVisitStatus();
//                System.out.println(Explain.explain(subexp));
//                System.out.println("i=" + i);
//                break;
//            }
        }
//        System.out.println("ROOT:");
//        root.resetVisitStatus();
//        System.out.println(Explain.explain(root));
        return trueroot;
    }


    private static Hop findCommonSubExp(Hop hop) {
        ArrayList<Hop> allSubExpression = new ArrayList<>();
        Hop target = null;
        hop.resetVisitStatus();
        getAllSubExpression(hop, allSubExpression);
//        for (int i = 0; i < allSubExpression.size(); i++) {
//            System.out.println("exp " + i);
//            Hop h1 = allSubExpression.get(i);
//            h1.resetVisitStatus();
//            System.out.println(Explain.explain(h1));
//        }
        DisjointSet djs = new DisjointSet(allSubExpression.size());
        for (int i = 0; i < allSubExpression.size() && target == null; i++) {
            for (int j = i + 1; j < allSubExpression.size() && target == null; j++) {
                Hop h1 = allSubExpression.get(i);
                Hop h2 = allSubExpression.get(j);
                Hop th2 = HopRewriteUtils.createTranspose(h2);
                if (h1.getInput().size() > 0
                        && h2.getInput().size() > 0
                        && !"dg(rand)".equals(h1.getOpString())
                        && !"dg(rand)".equals(h2.getOpString())
                    ) {
                    if (isSame(h1,h2)||isSame(h1,th2)) {
                        djs.merge(i, j);
                    }
                }
            }
        }
        for (int i = 0; i < allSubExpression.size(); i++) {
            if (djs.find(i) == i && djs.count(i)>1 ) {
                System.out.println("exp " + i+"  "+djs.count(i));
                Hop h1 = allSubExpression.get(i);
                h1.resetVisitStatus();
                System.out.println(Explain.explain(h1));
            }
        }
        return null;
    }

    private static Hop replaceCommonSubExp(Hop hop, Hop subexp) {
        hop.resetVisitStatus();
        Hop tsubexp = HopRewriteUtils.createTranspose(subexp);
        replaceCommonSubExp_iter(null, hop, subexp, tsubexp);
        System.out.println("after replace");
        hop.resetVisitStatus();
        System.out.println(Explain.explain(hop));
        return hop;
    }

    private static void replaceCommonSubExp_iter(Hop parent, Hop hop, Hop subexp, Hop tsubexp) {
        if (hop.isVisited())
            return;
        hop.setVisited();
        if (isSame(hop, subexp)) {
            if (parent != null) {
                // System.out.println("replace");
                HopRewriteUtils.replaceChildReference(parent, hop, subexp);
                HopRewriteUtils.cleanupUnreferenced(hop);
            }
        } else if (isSame(hop, tsubexp)) {
            if (parent != null) {
                //  System.out.println("replace");
                HopRewriteUtils.replaceChildReference(parent, hop, tsubexp);
                HopRewriteUtils.cleanupUnreferenced(hop);
            }
        }
        for (int i = 0; i < hop.getInput().size(); i++) {
            replaceCommonSubExp_iter(hop, hop.getInput().get(i), subexp, tsubexp);
        }
        return;
    }


    private static Hop double_jiehe(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new MatrixMultJieheRule());
        rules.add(new MatrixMultJieheRule2());
        hop = MyUtils.applyDAGRule(hop, rules, 100, false);
        return hop;
    }

    private static Hop random_change(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new MatrixMultJieheRule());
        rules.add(new MatrixMultJieheRule2());
        hop = MyUtils.applyDAGRule(hop, rules, 100, true);
        return hop;
    }

    private static Hop balance(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new BalanceMultiply4Rule());
        hop = MyUtils.applyDAGRule(hop, rules, 100, false);
        return hop;
    }

    private static Hop reorder(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new TransposeMultSplitRule());
        rules.add(new RemoveUnnecessaryTransposeRule());
        rules.add(new MatrixMultJieheRule());
        rules.add(new FenpeiRuleLeft(Types.OpOp2.MINUS));
        rules.add(new FenpeiRuleLeft(Types.OpOp2.PLUS));
        hop = MyUtils.applyDAGRule(hop, rules, 100, false);
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


    private static boolean isSame(Hop a, Hop b) {
        if (a == null || b == null) return false;
//        System.out.println("same<");
        Hop aa = deepCopyHopsDag(a);
        aa = reorder(aa);
//        System.out.println(Explain.explain(aa));
//        System.out.println(Explain.explain(b));
        Hop bb = deepCopyHopsDag(b);
        bb = reorder(bb);
//        System.out.println(Explain.explain(bb));
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
