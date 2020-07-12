package org.apache.sysds.hops.rewrite;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;

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
        rule_rewritedfp(root, false);
        root.resetVisitStatus();
        rule_rewritedfp(root, true);
        return root;
    }

    private void rule_rewritedfp(Hop hop, boolean descendFirst) {
        if (hop.isVisited())
            return;
        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop hi = hop.getInput().get(i);
            if (descendFirst)
                rule_rewritedfp(hi, descendFirst);
            hi = mySimplifyRule3(hop, hi, i);
            hi = mySimplifyRule4(hop, hi, i);
            if (!descendFirst)
                rule_rewritedfp(hi, descendFirst);
        }
        hop.setVisited();
    }

    private static Hop mySimplifyRule(Hop parent, Hop hi, int pos) {
        // t(y)*t(x) -> t(x*y)
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
            Hop left = hi.getInput().get(0);
            Hop right = hi.getInput().get(1);
            if (HopRewriteUtils.isTransposeOperation(left) &&
                    HopRewriteUtils.isTransposeOperation(right)) {
                System.out.println("found pattern");
                System.out.println("Old Hop:");
                System.out.println(Explain.explain(parent));

                Hop y = left.getInput().get(0);
                Hop x = right.getInput().get(0);
                Hop tmp = HopRewriteUtils.createMatrixMultiply(x, y);
                Hop result = HopRewriteUtils.createTranspose(tmp);
                HopRewriteUtils.replaceChildReference(parent, hi, result);
                hi = result;
                System.out.println("New Hop:");
                System.out.println(Explain.explain(hi));
            }
        }
        return hi;
    }

    private static Hop mySimplifyRule2(Hop parent, Hop hi, int pos) {
        // (x*y)*(t(y)*t(x)) -> (x*y)*t(x*y)
        if (HopRewriteUtils.isMatrixMultiply(hi)) {
            Hop left = hi.getInput().get(0);
            Hop right = hi.getInput().get(1);
            if (HopRewriteUtils.isMatrixMultiply(left) &&
                    HopRewriteUtils.isMatrixMultiply(right) &&
                    HopRewriteUtils.isTransposeOperation(right.getInput().get(0)) &&
                    HopRewriteUtils.isTransposeOperation(right.getInput().get(1))
            ) {
                Hop a = left.getInput().get(0);
                Hop b = left.getInput().get(1);
                Hop c = right.getInput().get(0).getInput().get(0);
                Hop d = right.getInput().get(1).getInput().get(0);
//                System.out.println("Found Pattern 2");
//                System.out.println(Explain.explain(hi));
//                System.out.println("a:");
//                System.out.println(Explain.explain(a));
//                System.out.println("b:");
//                System.out.println(Explain.explain(b));
//                System.out.println("c:");
//                System.out.println(Explain.explain(c));
//                System.out.println("d:");
//                System.out.println(Explain.explain(d));
                if (a == d && b == c) {
                    //   System.out.println("Equal");
                    Hop xy = HopRewriteUtils.createMatrixMultiply(a, b);
                    Hop txy = HopRewriteUtils.createTranspose(xy);
                    Hop result = HopRewriteUtils.createMatrixMultiply(xy, txy);
                    HopRewriteUtils.replaceChildReference(parent, hi, result);
                    HopRewriteUtils.cleanupUnreferenced(hi);
                    System.out.println(Explain.explain(result));
                    hi = result;
                }
            }
        }
        return hi;
    }

    private static Hop mySimplifyRule4(Hop parent, Hop hi, int pos) {
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


    private static Hop mySimplifyRule3(Hop parent, Hop hi, int pos) {
        // (x*y*t(y)*t(x))/(t(y)*t(x)*y) -> (x*y*t(x*y))/(t(x*y)*y)
        if (HopRewriteUtils.isBinary(hi, Types.OpOp2.DIV)) {
            System.out.println("div");
            Hop abcd = hi.getInput().get(0); // x*y*t(y)*t(x)
            Hop efg = hi.getInput().get(1); //t(y)*t(x)*y

            if (HopRewriteUtils.isMatrixMultiply(abcd)) {
                Hop abc = abcd.getInput().get(0);
                Hop d = abcd.getInput().get(1);
                if (HopRewriteUtils.isMatrixMultiply(abc)) {
                    Hop ab = abc.getInput().get(0);
                    Hop c = abc.getInput().get(1);
                    if (HopRewriteUtils.isMatrixMultiply(ab)) {
                        Hop a = ab.getInput().get(0);
                        Hop b = ab.getInput().get(1);
                        if (HopRewriteUtils.isMatrixMultiply(efg)) {
                            Hop ef = efg.getInput().get(0);
                            Hop g = efg.getInput().get(1);
                            if (HopRewriteUtils.isMatrixMultiply(ef)) {
                                Hop e = ef.getInput().get(0);
                                Hop f = ef.getInput().get(1);
                                if (HopRewriteUtils.isTransposeOperation(c) &&
                                        HopRewriteUtils.isTransposeOperation(d) &&
                                        HopRewriteUtils.isTransposeOperation(e) &&
                                        HopRewriteUtils.isTransposeOperation(f)
                                ) {
                                    Hop tc = c.getInput().get(0);
                                    Hop td = d.getInput().get(0);
                                    Hop te = e.getInput().get(0);
                                    Hop tf = f.getInput().get(0);
                                    if (a == td && a == tf && b == tc && b == te) {
                                        System.out.println("DFP expression found");

                                        Hop tab = HopRewriteUtils.createTranspose(ab);
                                        Hop ab_tab = HopRewriteUtils.createMatrixMultiply(ab, tab);
                                        Hop tab_b = HopRewriteUtils.createMatrixMultiply(tab, b);
                                        Hop result = HopRewriteUtils.createBinary(ab_tab, tab_b, Types.OpOp2.DIV);

                                        HopRewriteUtils.replaceChildReference(parent, hi, result);
                                        HopRewriteUtils.cleanupUnreferenced(hi);
                                        System.out.println(Explain.explain(result));
                                        hi = result;

                                    }
                                }

                            }
                        }
                    }
                }
            }
        }
        return hi;
    }


}
