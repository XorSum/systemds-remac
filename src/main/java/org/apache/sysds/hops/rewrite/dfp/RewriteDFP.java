package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.HopsException;
import org.apache.sysds.hops.rewrite.HopRewriteRule;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;
import java.util.HashMap;

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
        hop.resetVisitStatus();
        hop = reorder_iter(null, hop, false);
        hop.resetVisitStatus();
        hop = reorder_iter(null, hop, true);
        return hop;
    }

    private static Hop reorder_iter(Hop parent, Hop hop, boolean descendFirst) {
        if (hop.isVisited())
            return hop;
        if (descendFirst) {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop hi = hop.getInput().get(i);
                reorder_iter(hop, hi, descendFirst);
            }
        }
        hop = splitTranspose(parent, hop, 0);
        hop = removeUnnecessaryTranspose(parent, hop, 0);
        hop = reorderMultiply(parent, hop, 0);
        if (!descendFirst) {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop hi = hop.getInput().get(i);
                reorder_iter(hop, hi, descendFirst);
            }
        }
        hop.setVisited();
        return hop;
    }

    private static Hop balance(Hop hop) {
        hop.resetVisitStatus();
        hop = balance_iter(null, hop);
        return hop;
    }

    private static Hop balance_iter(Hop parent, Hop hop) {
        if (hop.isVisited())
            return hop;
        hop = balanceMultiply4(parent, hop, 0);

        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop hi = hop.getInput().get(i);
            balance_iter(hop, hi);
        }

        hop.setVisited();
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


    private static Hop splitTranspose(Hop parent, Hop hi, int pos) {
        // t(x*y)->t(y)*t(x)
        if (HopRewriteUtils.isTransposeOperation(hi)) {
            Hop xy = hi.getInput().get(0);
            if (HopRewriteUtils.isMatrixMultiply(xy)) {
                Hop x = xy.getInput().get(0);
                Hop y = xy.getInput().get(1);
                Hop tx = HopRewriteUtils.createTranspose(x);
                Hop ty = HopRewriteUtils.createTranspose(y);
                Hop result = HopRewriteUtils.createMatrixMultiply(ty, tx);
                if (parent != null)
                    HopRewriteUtils.replaceChildReference(parent, hi, result);
                hi = result;
//                System.out.println("New Hop:");
//                System.out.println(Explain.explain(hi));
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
                if (parent != null)
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
//                System.out.println("found t(t(x))");
//                System.out.println("Old Hop:");
//                System.out.println(Explain.explain(parent));
                Hop x = tx.getInput().get(0);
                if (parent != null)
                    HopRewriteUtils.replaceChildReference(parent, ttx, x);
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
                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, hi, abc);
                    HopRewriteUtils.cleanupUnreferenced(hi);
                }
                hi = abc;
            }
        }
        return hi;
    }

    private static Hop balanceMultiply4(Hop parent, Hop hi, int pos) {
        // ((a*b)*c)*d -> (a*b)*c
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
            Hop abc = hi.getInput().get(0);
            Hop d = hi.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(abc)) {
                Hop ab = abc.getInput().get(0);
                Hop c = abc.getInput().get(1);
                // create
                Hop cd = HopRewriteUtils.createMatrixMultiply(c, d);
                Hop abcd = HopRewriteUtils.createMatrixMultiply(ab, cd);
                // replace
                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, hi, abcd);
                    HopRewriteUtils.cleanupUnreferenced(hi);
                }
                hi = abcd;
            }
        }

        return hi;
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
