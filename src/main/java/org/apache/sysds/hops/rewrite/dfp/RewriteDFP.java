package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.*;
import org.apache.sysds.hops.rewrite.dfp.rule.BalanceMultiply4Rule;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule2;
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil;
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
            rewriteDFP(hi);
            long endTime = System.currentTimeMillis();
            long totalTime = endTime -startTime;
            System.out.println("rewriteDAG执行耗时：" + totalTime + " ms");
        }
        return roots;
    }

    @Override
    public Hop rewriteHopDAG(Hop root, ProgramRewriteStatus state) {
//        return root;
        rewriteDFP(root);
        return root;
    }

   private static HopRewriteRule rewriteMatrixMultChainOptimization = new RewriteMatrixMultChainOptimization();
   private static HopRewriteRule rewriteCommonSubexpressionElimination = new RewriteCommonSubexpressionElimination();


    private static ArrayList<MySolution> solutions ;
    private static ArrayList<MatrixMultChain> chains;

    public static ArrayList<MySolution> rewriteDFP(Hop root) {
        if (root == null) return null;

        solutions =  new ArrayList<>();

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
//        ArrayList<Triple<Hop, Hop, Integer>> blocks = findMultChains(root);
         chains = findMultChains(root);

        System.out.println("Chain size: " + chains.size());
                    endTime = System.currentTimeMillis();
                    totalTime = endTime -startTime;
                    System.out.println(">寻找矩阵连乘块执行耗时：" + totalTime + " ms");

        for (MatrixMultChain chain: chains) {
            chain.generateAlltrees();
        }

        func(root,false);
        func(root,true);


        return solutions;
    }


    private static void func(Hop root,
            boolean onlySearchConstantSubExp )   {

      long  startTime = System.currentTimeMillis();
        Set<Pair<Long, Long>> allSubExps = new HashSet<>();
        for (MatrixMultChain chain : chains) {
            chain.onlySearchConstantSubExp = onlySearchConstantSubExp;
            chain.recordSubexp();
            allSubExps.addAll(chain.allTreeesHashMap.keySet());
        }
        System.out.println("Sub Exp size: " + allSubExps.size());
       long endTime = System.currentTimeMillis();
       long totalTime = endTime -startTime;
        System.out.println(">初始化哈希表执行耗时：" + totalTime + " ms");


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
                ProgramRewriteStatus prs = new ProgramRewriteStatus();
               // targetDag =  rewriteMatrixMultChainOptimization.rewriteHopDAG(targetDag,prs);
               // targetDag = rewriteCommonSubexpressionElimination.rewriteHopDAG(targetDag,prs);
                System.out.println("Target: " + solutions.size() + ", count=" + allCount +", exp="+MyExplain.myExplain(targetDag));
                Hop hop  = genSolution( root, chains, targetHash, targetDag);

                MySolution solution ;


                if (onlySearchConstantSubExp) {
                    solution = ConstantUtil.liftLoopConstant(hop);
                    System.out.println(solution);
                }else {
                   solution = new MySolution();
                   solution.body = hop;
                }
                solutions.add(solution);
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
        System.out.println("Solution Size: "+solutions.size());

    }


    private static ArrayList<MatrixMultChain> findMultChains(Hop root) {
        ArrayList<MatrixMultChain> result = new ArrayList<>();
        ArrayList<Integer> path = new ArrayList<>();
        findMultChains_iter(null,root,path,0, result);
        return result;
    }

    private static void findMultChains_iter(Hop parent, Hop hop,
                                            ArrayList<Integer> path, int depth,
                                            ArrayList<MatrixMultChain> result) {
        //   if (hop.isVisited()) return;
        if ((parent == null || !HopRewriteUtils.isMatrixMultiply(parent))
                && isAllOfMult(hop)) {
            MatrixMultChain chain = new MatrixMultChain(hop,path,depth  );
            result.add(chain);
        } else {
            for (int i = 0; i < hop.getInput().size(); i++) {
                if (path.size() <= depth) path.add(i);
                else path.set(depth, i);
                findMultChains_iter(hop, hop.getInput().get(i), path,depth+1, result);
            }
        }
        //  hop.setVisited();
    }


    private static Hop genSolution(Hop root,ArrayList<MatrixMultChain> chains, Pair<Long, Long> targetHash, Hop targetDag) {
        Hop copy = deepCopyHopsDag(root);
        for (MatrixMultChain chain: chains) {
            Hop subTree = chain.getTree(targetHash, targetDag);
            if (chain.depth == 0) {
                copy = subTree;
            } else {
                Hop parent = null;
                Hop cur = copy;
                for (int i = 0; i < chain.depth; i++) {
                    parent = cur;
                    cur = cur.getInput().get(chain.path.get(i));
                }
                HopRewriteUtils.replaceChildReference(parent, cur, subTree);
                HopRewriteUtils.cleanupUnreferenced(cur);
            }
        }
        return copy;
    }


}
