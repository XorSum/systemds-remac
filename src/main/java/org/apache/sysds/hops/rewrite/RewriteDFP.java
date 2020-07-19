package org.apache.sysds.hops.rewrite;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.HopsException;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class RewriteDFP extends HopRewriteRule {
    @Override
    public ArrayList<Hop> rewriteHopDAGs(ArrayList<Hop> roots, ProgramRewriteStatus state) {
        System.out.println("bbbb");
        for (int i = 0; i < roots.size(); i++) {
            rewriteHopDAG(roots.get(i), state);
        }
        return roots;
    }

    @Override
    public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
        System.out.println("aaa");
        reorder(root);
        rule_rewritedfp(root, false);
        root.resetVisitStatus();
        rule_rewritedfp(root, true);
        return root;
    }

    private void rule_rewritedfp(Hop hop, boolean descendFirst) {
        if (hop.isVisited())
            return;
        // Hop bb = deepCopyHopsDag(hop);

        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop hi = hop.getInput().get(i);
            if (descendFirst)
                rule_rewritedfp(hi, descendFirst);
//            Hop bb = deepCopyHopsDag(hi);
//            reorder(bb);
//            isSame(hi, bb);
            findAllSubExpression(hi);
            if (!descendFirst)
                rule_rewritedfp(hi, descendFirst);
        }
        hop.setVisited();
    }


    private static void reorder(Hop hop) {
//        hop.resetVisitStatus();
//        reorder_iter(hop, false);
        hop.resetVisitStatus();
        reorder_iter(hop, true);
    }

    private static void reorder_iter(Hop hop, boolean descendFirst) {
        if (hop.isVisited())
            return;
//        hop = splitTranspose(null, hop, 0);
//        hop = removeUnnecessaryTranspose(null, hop, 0);
//        hop = reorderMultiply(null, hop, 0);
        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop hi = hop.getInput().get(i);
            if (descendFirst)
                reorder_iter(hi, descendFirst);
            hi = splitTranspose(hop, hi, i);
            hi = removeUnnecessaryTranspose(hop, hi, i);
            hi = reorderMultiply(hop, hi, i);
            if (!descendFirst)
                reorder_iter(hi, descendFirst);
        }
        hop.setVisited();
    }


    private static Hop splitTranspose(Hop parent, Hop hi, int pos) {
        // t(x*y)->t(y)*t(x)
        if (HopRewriteUtils.isTransposeOperation(hi)) {
            Hop xy = hi.getInput().get(0);
            System.out.println("is transpose");
            System.out.println("<<<");
            System.out.println(Explain.explain(hi));
            System.out.println(">>>");
            if (HopRewriteUtils.isMatrixMultiply(xy)) {
                System.out.println("is multiply");
                Hop x = xy.getInput().get(0);
                Hop y = xy.getInput().get(1);
                Hop tx = HopRewriteUtils.createTranspose(x);
                Hop ty = HopRewriteUtils.createTranspose(y);
                Hop result = HopRewriteUtils.createMatrixMultiply(ty, tx);
                HopRewriteUtils.replaceChildReference(parent, hi, result);
                hi = result;
                System.out.println("New Hop:");
                System.out.println(Explain.explain(hi));
            }
        }
        return hi;
    }

    private static Hop mergeTranspose(Hop parent, Hop hi, int pos) {
        // t(y)*t(x) -> t(x*y)
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
            Hop left = hi.getInput().get(0);
            Hop right = hi.getInput().get(1);
            if (HopRewriteUtils.isTransposeOperation(left) &&
                    HopRewriteUtils.isTransposeOperation(right)) {
                Hop y = left.getInput().get(0);
                Hop x = right.getInput().get(0);
                Hop tmp = HopRewriteUtils.createMatrixMultiply(x, y);
                Hop result = HopRewriteUtils.createTranspose(tmp);
                HopRewriteUtils.replaceChildReference(parent, hi, result);
                hi = result;
            }
        }
        return hi;
    }

    private static Hop removeUnnecessaryTranspose(Hop parent, Hop ttx, int pos) {
        // t(t(x)) -> x
        if (HopRewriteUtils.isTransposeOperation(ttx)) {
            Hop tx = ttx.getInput().get(0);
            if (HopRewriteUtils.isTransposeOperation(tx)) {
                System.out.println("found t(t(x))");
//                System.out.println("Old Hop:");
//                System.out.println(Explain.explain(parent));
                Hop x = tx.getInput().get(0);
                HopRewriteUtils.replaceChildReference(parent, ttx, tx);
                ttx = x;
//                System.out.println("New Hop:");
//                System.out.println(Explain.explain(hi));
            }
        }
        return ttx;
    }

    private static Hop reorderMultiply(Hop parent, Hop hi, int pos) {
        // a*(b*c) -> (a*b)*c
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
            Hop a = hi.getInput().get(0);
            Hop bc = hi.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(bc)) {
                Hop b = bc.getInput().get(0);
                Hop c = bc.getInput().get(1);
                // create
                Hop ab = HopRewriteUtils.createMatrixMultiply(a, b);
                Hop abc = HopRewriteUtils.createMatrixMultiply(ab, c);
                // replace
                HopRewriteUtils.replaceChildReference(parent, hi, abc);
                HopRewriteUtils.cleanupUnreferenced(hi);
                hi = abc;
            }
        }
        return hi;
    }

    private static void findAllSubExpression(Hop hop) {
        // input a multiply chain.

        // step 1. find all factors
        ArrayList<Hop> allFactors = new ArrayList<>();
        boolean expand = true;
        Hop hi = hop;
        while (expand) {
            expand = false;
            if (HopRewriteUtils.isMatrixMultiply(hi)) {
                Hop left = hi.getInput().get(0);
                Hop right = hi.getInput().get(1);
                allFactors.add(right);
                if (HopRewriteUtils.isMatrixMultiply(left)) {
                    expand = true;
                    hi = left;
                } else {
                    allFactors.add(left);
                }
            }
        }
        Collections.reverse(allFactors);
//        System.out.println("all factors:");
//        for (Hop h : allFactors) {
//            System.out.println(Explain.explain(h));
//            System.out.println("-----");
//        }
        // step 2. create all sub expressions
        ArrayList<Hop> allSubExpression = new ArrayList<>();
        for (int i = 0; i < allFactors.size(); i++) {
            for (int j = i; j < allFactors.size(); j++) {
                allSubExpression.add(deepCopyHopsDag(createMC(allFactors, i, j)));
            }
        }
//        System.out.println("all sub expressions:");
//        for (Hop h : allSubExpression) {
//            System.out.println(Explain.explain(h));
//            System.out.println("-----");
//        }
        // step 3. check weather sub expression are equal
        for (int i = 0; i < allSubExpression.size(); i++) {
            for (int j = i + 1; j < allSubExpression.size(); j++) {
                // boolean eq = isSame(allSubExpression.get(i), allSubExpression.get(j));
                //  System.out.println(i + (eq ? "==" : "!=") + j);

                if (i == 1 && j == 8) {
                    Hop a = allSubExpression.get(i);
                    Hop b = allSubExpression.get(j);
                    reorder(a);
                    System.out.println(Explain.explain(a));
                    System.out.println("---");
                    reorder(b);
                    System.out.println(Explain.explain(b));
                    System.out.println("---");
                    Hop tmp = HopRewriteUtils.createTranspose(b);
                    System.out.println(Explain.explain(tmp));
                    System.out.println("---");
                    reorder(tmp);
                    System.out.println(Explain.explain(tmp));

                    System.out.println("---");

                    boolean eq = isSame(allSubExpression.get(i), tmp);
                    System.out.println(i + (eq ? "==" : "!=") + "t(" + j + ")");
                }
            }
        }
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
        reorder(aa);
        //System.out.println(Explain.explain(aa));
        Hop bb = deepCopyHopsDag(b);
        reorder(bb);
        //System.out.println(Explain.explain(bb));
        boolean ret = isSame_iter(aa, bb);
//        System.out.println("Ret=" + ret);
//        System.out.println(">same");
        return ret;
    }

    private static boolean isSame_iter(Hop a, Hop b) {
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


    /**
     * Deep copy of hops dags for parallel recompilation.
     *
     * @param hops high-level operator
     * @return high-level operator
     */
    public static Hop deepCopyHopsDag(Hop hops) {
        Hop ret = null;

        try {
            HashMap<Long, Hop> memo = new HashMap<>(); //orig ID, new clone
            ret = rDeepCopyHopsDag(hops, memo);
        } catch (Exception ex) {
            throw new HopsException(ex);
        }

        return ret;
    }

    private static Hop rDeepCopyHopsDag(Hop hop, HashMap<Long, Hop> memo)
            throws CloneNotSupportedException {
        Hop ret = memo.get(hop.getHopID());

        //create clone if required
        if (ret == null) {
            ret = (Hop) hop.clone();

            //create new childs and modify references
            for (Hop in : hop.getInput()) {
                Hop tmp = rDeepCopyHopsDag(in, memo);
                ret.getInput().add(tmp);
                tmp.getParent().add(ret);
            }
            memo.put(hop.getHopID(), ret);
        }

        return ret;
    }

}
