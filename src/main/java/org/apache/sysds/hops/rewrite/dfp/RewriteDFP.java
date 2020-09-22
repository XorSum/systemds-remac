package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.*;
import org.apache.sysds.hops.rewrite.dfp.rule.BalanceMultiply4Rule;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule2;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.apache.sysds.hops.rewrite.dfp.utils.ApplyRulesOnDag.applyDAGRule;
import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.*;
import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class RewriteDFP extends HopRewriteRule {
    @Override
    public ArrayList<Hop> rewriteHopDAGs(ArrayList<Hop> roots, ProgramRewriteStatus state) {
        System.out.println("bbbb");
        for (int i = 0; i < roots.size(); i++) {
            Hop hi = roots.get(i);
            long startTime = System.currentTimeMillis();
            hi = rewriteDFP(hi, state);
            roots.set(i, hi);
            long endTime = System.currentTimeMillis();
            long totalTime = endTime -startTime;
            System.out.println("rewriteDAG执行耗时：" + totalTime + " ms");
        }
        return roots;
    }

    @Override
    public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
//        return root;
        return rewriteDFP(root, state);
    }

    private static ArrayList<MatrixMultChain> chains;

    public static Hop rewriteDFP(Hop root, ProgramRewriteStatus state) {
        if (root == null) return root;

        System.out.println(MyExplain.myExplain(root));


                    long startTime = System.currentTimeMillis();
        root = reorder(root);
                    long endTime = System.currentTimeMillis();
                    long totalTime = endTime -startTime;
                    System.out.println(">reorder执行耗时：" + totalTime + " ms");
        System.out.println(MyExplain.myExplain(root));
     //   System.out.println(Judge.isSymmetryMatrixInLoop(root));

        root.resetVisitStatusForced(new HashSet<>());
//        System.out.println("Root: ");
//        System.out.println(Explain.explain(root));

                    startTime = System.currentTimeMillis();
        ArrayList<Triple<Hop, Hop, Integer>> blocks = findMultChains(root);
        System.out.println("Chain size: " + blocks.size());
                    endTime = System.currentTimeMillis();
                    totalTime = endTime -startTime;
                    System.out.println(">寻找矩阵连乘块执行耗时：" + totalTime + " ms");

                    startTime = System.currentTimeMillis();
        ArrayList<Hop> solutions = new ArrayList<>();
        chains = new ArrayList<>();
        for (Triple<Hop, Hop, Integer> chain : blocks) {
            MatrixMultChain chain1 = new MatrixMultChain();
            chain1.gao(chain.getMiddle());
            chains.add(chain1);
        }

        Set<Pair<Long, Long>> allSubExps = new HashSet<>();
        for (MatrixMultChain chain : chains) {
            allSubExps.addAll(chain.allTreeesHashMap.keySet());
        }
        System.out.println("Sub Exp size: " + allSubExps.size());
                    endTime = System.currentTimeMillis();
                    totalTime = endTime -startTime;
                    System.out.println(">初始化哈希表执行耗时：" + totalTime + " ms");

        HopRewriteRule rule1 = new RewriteMatrixMultChainOptimization();
        HopRewriteRule rule2 = new RewriteCommonSubexpressionElimination();


        startTime = System.currentTimeMillis();
        for (Pair<Long, Long> targetHash : allSubExps) {
            long allCount = 0L;
            Hop targetDag = null;
            for (MatrixMultChain chain : chains) {
                if (chain.allTreeesHashMap.containsKey(targetHash)) {
                    allCount = allCount + chain.allTreeesHashMap.get(targetHash).getRight();
                    if (targetDag == null) {
                        targetDag = deepCopyHopsDag(chain.allTreeesHashMap.get(targetHash).getMiddle());
                    }
                }
            }
            if (targetDag != null && allCount > 2) {
//                System.out.println("<----");
                System.out.println("Target: " + solutions.size() + ", count=" + allCount +", exp="+MyExplain.myExplain(targetDag));

                Hop sol = genSolution(root, targetHash, targetDag);
                solutions.add(sol);

//                if ("h%*%t(a)%*%a%*%d".equals(tarExp)) {
//                //    targetDag = rule1.rewriteHopDAG(targetDag,new ProgramRewriteStatus());
//                 //   System.out.println(Explain.explain(targetDag));
////                    System.out.println("Solution: h%*%t(a)%*%a%*%d");
////                    sol.resetVisitStatusForced(new HashSet<>());
////                    System.out.println(MyExplain.myExplain(sol));
////                    System.out.println(Explain.explain(sol));
//                    sol  = rule1.rewriteHopDAG(sol, new ProgramRewriteStatus() );
////                  sol.resetVisitStatusForced(new HashSet<>());
//                    sol.resetVisitStatus();
//                    sol  = rule2.rewriteHopDAG(sol, new ProgramRewriteStatus() );
//                    sol.resetVisitStatusForced(new HashSet<>());
//                    System.out.println("after");
//                    System.out.println(Explain.explain(sol));
//                 //   return sol;
//                }
//                System.out.println("---->");
            }
        }

                    endTime = System.currentTimeMillis();
                    totalTime = endTime -startTime;
                    System.out.println(">构造所有计划执行耗时：" + totalTime + " ms");



        System.out.println("Solution size: " + solutions.size());
        System.out.println("\n\n==========================\n\n");
        return root;
    }


    private static ArrayList<Triple<Hop, Hop, Integer>> findMultChains(Hop root) {
        ArrayList<Triple<Hop, Hop, Integer>> result = new ArrayList<>();
        // root.resetVisitStatus();
        findMultChains_iter(null, root, 0, result);
        return result;
    }

    private static void findMultChains_iter(Hop parent, Hop hop, int index, ArrayList<Triple<Hop, Hop, Integer>> result) {
        //   if (hop.isVisited()) return;
        if ((parent == null || !HopRewriteUtils.isMatrixMultiply(parent))
                && isAllOfMult(hop)) {
            result.add(Triple.of(parent, hop, index));
        } else {
            for (int i = 0; i < hop.getInput().size(); i++) {
                findMultChains_iter(hop, hop.getInput().get(i), i, result);
            }
        }
        //  hop.setVisited();
    }

    static int chain_index;

    private static Hop genSolution(Hop root, Pair<Long, Long> targetHash, Hop targetDag) {
        Hop copy = deepCopyHopsDag(root);
        chain_index = 0;
        copy = genSolution_iter(null, copy, targetHash, targetDag);
        return copy;
    }

    private static Hop genSolution_iter(Hop parent, Hop hop, Pair<Long, Long> targetHash, Hop targetDag) {
        if ((parent == null || !HopRewriteUtils.isMatrixMultiply(parent))
                && isAllOfMult(hop)) {
          //  System.out.println("++++++++++++++++++++++++++++++++++++++++    " + hop.getHopID() + " replace ");
            Hop subTree = chains.get(chain_index).getTree(targetHash, targetDag);
//            System.out.println("subtree");
//            subTree.resetVisitStatusForced(new HashSet<>());
//            System.out.println(MyExplain.myExplain(subTree));
            chain_index = chain_index + 1;
            if (parent != null) {
                HopRewriteUtils.replaceChildReference(parent, hop, subTree);
                HopRewriteUtils.cleanupUnreferenced(hop);
            }
            hop = subTree;
        } else {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop tmp = genSolution_iter(hop, hop.getInput().get(i), targetHash, targetDag);
                hop.getInput().set(i, tmp);
            }
        }
        return hop;
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
//                if (isSampleHop(h1)&&isSampleHop(h2)){
                    if (isSame(h1, h2) || isSame(h1, th2)) {
                        djs.merge(i, j);
                    }
                }
            }
        }
        for (int i = 0; i < allSubExpression.size(); i++) {
            if (djs.find(i) == i && djs.count(i) > 1) {
                Hop h1 = allSubExpression.get(i);
                if (!isSampleHop(h1)) {
                    System.out.println("exp " + i + "  " + djs.count(i) + " " + isSampleHop(h1));
                    h1.resetVisitStatus();
                    System.out.println(Explain.explain(h1));
                }
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
        hop = applyDAGRule(hop, rules, 100, false);
        return hop;
    }

    private static Hop random_change(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new MatrixMultJieheRule());
        rules.add(new MatrixMultJieheRule2());
        hop = applyDAGRule(hop, rules, 100, true);
        return hop;
    }

    private static Hop balance(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new BalanceMultiply4Rule());
        hop = applyDAGRule(hop, rules, 100, false);
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


}
