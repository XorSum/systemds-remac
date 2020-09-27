package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.*;
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.hops.rewrite.dfp.utils.Prime;
import spire.macros.Auto;

import java.util.*;
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
            long totalTime = endTime - startTime;
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


    private static ArrayList<MySolution> solutions;
    private static ArrayList<MatrixMultChain> chains;

    public static ArrayList<MySolution> rewriteDFP(Hop root) {
        if (root == null) return null;

        solutions = new ArrayList<>();

        System.out.println(MyExplain.myExplain(root));


        long startTime = System.currentTimeMillis();
        root = reorder(root);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
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
        totalTime = endTime - startTime;
        System.out.println(">寻找矩阵连乘块执行耗时：" + totalTime + " ms");

        for (MatrixMultChain chain : chains) {
            chain.generateAlltrees();
        }

        func(root, false);
        func(root, true);


        return solutions;
    }


    private static void func(Hop root,
                             boolean onlySearchConstantSubExp) {

        long startTime = System.currentTimeMillis();
        Set<Pair<Long, Long>> allSubExps = new HashSet<>();
        for (MatrixMultChain chain : chains) {
            chain.onlySearchConstantSubExp = onlySearchConstantSubExp;
            chain.recordSubexp();
            allSubExps.addAll(chain.allTreeesHashMap.keySet());
        }
        System.out.println("Sub Exp size: " + allSubExps.size());
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
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
                System.out.println("Target: " + solutions.size() + ", count=" + allCount + ", exp=" + MyExplain.myExplain(targetDag));
                Hop hop = genSolution(root, chains, targetHash, targetDag);

                MySolution solution;


                if (onlySearchConstantSubExp) {
                    solution = ConstantUtil.liftLoopConstant(hop);
                    System.out.println(solution);
                } else {
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
        totalTime = endTime - startTime;
        System.out.println(">构造所有计划执行耗时：" + totalTime + " ms");
        System.out.println("Solution Size: " + solutions.size());

    }


    private static ArrayList<MatrixMultChain> findMultChains(Hop root) {
        ArrayList<MatrixMultChain> result = new ArrayList<>();
        ArrayList<Integer> path = new ArrayList<>();
        findMultChains_iter(null, root, path, 0, result);
        return result;
    }

    private static void findMultChains_iter(Hop parent, Hop hop,
                                            ArrayList<Integer> path, int depth,
                                            ArrayList<MatrixMultChain> result) {
        //   if (hop.isVisited()) return;
        if ((parent == null || !HopRewriteUtils.isMatrixMultiply(parent))
                && isAllOfMult(hop)) {
            MatrixMultChain chain = new MatrixMultChain(hop, path, depth);
            result.add(chain);
        } else {
            for (int i = 0; i < hop.getInput().size(); i++) {
                if (path.size() <= depth) path.add(i);
                else path.set(depth, i);
                findMultChains_iter(hop, hop.getInput().get(i), path, depth + 1, result);
            }
        }
        //  hop.setVisited();
    }


    private static Hop genSolution(Hop root, ArrayList<MatrixMultChain> chains, Pair<Long, Long> targetHash, Hop targetDag) {
        Hop copy = deepCopyHopsDag(root);
        for (MatrixMultChain chain : chains) {
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

    ///////////////////////////////////////////////////////////

    static DisjointSet djs = new DisjointSet(1000);
    static HashMap<Long, Integer> hopId2LeafIndex = new HashMap<>();
    static ArrayList<Leaf> leaves = new ArrayList<>();
    private static ArrayList<Pair<Integer, Integer>> nodeRange = new ArrayList<>();
    private static HashMap<Pair<Long, Long>, ArrayList<Pair<Integer, Integer>>> tmp = new HashMap<>();

    private static int findAllLeaf(Hop hop,
                                   ArrayList<Integer> path,
                                   int depth
    ) {
        if (isLeafMatrix(hop) ||
                (HopRewriteUtils.isTransposeOperation(hop) && isLeafMatrix(hop.getInput().get(0)))) {
            int index = leaves.size();
            hopId2LeafIndex.put(hop.getHopID(), index);
            Leaf leaf = new Leaf(hop, path, depth);
            leaves.add(leaf);
            return index;
        } else {
            if (HopRewriteUtils.isMatrixMultiply(hop)) {
                if (path.size() <= depth) path.add(0);
                else path.set(depth, 0);
                int l = findAllLeaf(hop.getInput().get(0), path, depth + 1);
                path.set(depth, 1);
                int r = findAllLeaf(hop.getInput().get(1), path, depth + 1);
                if (l >= 0 && r >= 0) {
                    djs.merge(l, r);
                    return l;
                }
            } else {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    if (path.size() <= depth) path.add(i);
                    else path.set(depth, i);
                    findAllLeaf(hop.getInput().get(i), path, depth + 1);
                }
            }
        }
        return -1;
    }


    public static void main(Hop hop) {
        djs = new DisjointSet(1000);
        hopId2LeafIndex = new HashMap<>();
        leaves = new ArrayList<>();
        nodeRange = new ArrayList<>();
        tmp = new HashMap<>();
        //commonSubExp = new HashMap<>();

        hop = reorder(hop);

        System.out.println(MyExplain.myExplain(hop));
        findAllLeaf(hop, new ArrayList<>(), 0);
        for (int i = 0; i < leaves.size(); i++) {
            if (djs.find(i) == i) {
                int l = i, r = i;
                while (l - 1 >= 0 && djs.find(l - 1) == djs.find(i)) l--;
                while (r + 1 < leaves.size() && djs.find(r + 1) == djs.find(i)) r++;
                nodeRange.add(Pair.of(l, r));
                System.out.println("Range " + l + " " + r);
                for (int j = l; j <= r; j++) {
                    System.out.print(MyExplain.myExplain(leaves.get(j).hop) + " ");
                }
                System.out.println();
            }
        }

        for (Pair<Integer, Integer> block : nodeRange) {
            for (int l = block.getLeft(); l <= block.getRight(); l++) {
                for (int r = l + 1; r <= block.getRight(); r++) {
                    Long hashTag = hash(l, r);
                    Long tHashTag = tHash(l, r);
                    if (hashTag > tHashTag) {
                        Long t = hashTag;
                        hashTag = tHashTag;
                        tHashTag = t;
                    }
                    Pair<Long, Long> hash = Pair.of(hashTag, tHashTag);
                    if (!tmp.containsKey(hash))
                        tmp.put(hash, new ArrayList<>());
                    tmp.get(hash).add(Pair.of(l, r));
                }
            }
        }

        for (Iterator<Map.Entry<Pair<Long, Long>, ArrayList<Pair<Integer, Integer>>>> it = tmp.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Pair<Long, Long>, ArrayList<Pair<Integer, Integer>>> e = it.next();
            ArrayList<Pair<Integer, Integer>> list = e.getValue();
            if (list.size() < 2 || list.get(0).getRight() >= list.get(list.size() - 1).getLeft()) {
                it.remove();
            }
        }

        for (Map.Entry<Pair<Long, Long>, ArrayList<Pair<Integer, Integer>>> e : tmp.entrySet()) {
            System.out.print("\n\nexp = ");
            Pair<Integer, Integer> r = e.getValue().get(0);
            for (int i = r.getLeft(); i <= r.getRight(); i++) {
                System.out.print(MyExplain.myExplain(leaves.get(i).hop) + " ");
            }
            System.out.println();
            System.out.println("hash = " + e.getKey());
            for (Pair<Integer, Integer> range : e.getValue()) {
                System.out.println("range = [" + range.getLeft() + "," + range.getRight() + "]");
            }
        }
    }


    private static Long hash(int l, int r) {
        Long ret = 0L;
        for (int i = 0; l + i <= r; i++) {
            Long single;
            Hop h = leaves.get(l + i).hop;
            if (!HopRewriteUtils.isTransposeOperation(h)) {
                single = (long) h.getOpString().hashCode();
            } else {
                h = h.getInput().get(0);
                single = (long) h.getOpString().hashCode() * 998244853;
            }
            ret = ret + single * Prime.getPrime(i);
        }
        return ret;
    }

    private static Long tHash(int l, int r) {
        Long ret = 0L;
        for (int i = 0; r - i >= l; i++) {
            Long single = 0L;
            Hop h = leaves.get(r - i).hop;
            if (!HopRewriteUtils.isTransposeOperation(h)) {
                single = (long) h.getOpString().hashCode() * 998244853;

            } else {
                h = h.getInput().get(0);
                single = (long) h.getOpString().hashCode();
            }
            ret = ret + single * Prime.getPrime(i);
        }
        return ret;
    }


}

/*
       private static HashMap<Pair<Long,Long>,ArrayList<ArrayList<Pair<Integer,Integer>>>> commonSubExp = new HashMap<>();


    private static void genCSE(Pair<Long,Long> hash,ArrayList<Pair<Integer,Integer>> ranges) {
        int maxSize = 0;
        ArrayList<ArrayList<Pair<Integer,Integer>>> dp0 = new ArrayList<>();
        ArrayList<ArrayList<Pair<Integer,Integer>>> dp1 = new ArrayList<>();
        dp0.set(0,new ArrayList<>());
        ArrayList<Pair<Integer,Integer>> t = new ArrayList<>();
        t.add(ranges.get(0));
        dp1.set(0,t);
        for (int i=1;i<ranges.size();i++) {
            if (dp0.get(i-1).size() <dp1.get(i-1).size()) {
                dp0.set(i,(ArrayList<Pair<Integer,Integer>>)dp1.get(i-1).clone());
            }else {
                dp0.set(i,(ArrayList<Pair<Integer,Integer>>)dp0.get(i-1).clone());
            }
            if (maxSize<dp0.size()) maxSize = dp0.size();
            Pair<Integer,Integer> cur = ranges.get(i);
            int j=i-1;
            for (;j>=0;j--) {
                Pair<Integer,Integer> pre = ranges.get(j);
                if (pre.getRight()<cur.getLeft()) {
                    break;
                }
            }
            if (j<0) {
                t = new ArrayList<>();
                t.add(cur);
                dp1.set(i,t);
            } else {
                if (dp0.get(j).size() <dp1.get(j).size()) {
                    t = (ArrayList<Pair<Integer,Integer>>)dp1.get(j).clone();
                }else {
                    t=(ArrayList<Pair<Integer,Integer>>)dp0.get(j).clone();
                }
                t.add(cur);
                dp1.set(i,t);
            }
            if (maxSize<dp1.size()) maxSize = dp1.size();
        }
        if (maxSize>=2) {
            if (!commonSubExp.containsKey(hash)) {
                commonSubExp.put(hash,new ArrayList<>());
            }
            for (int i=0;i<ranges.size();i++) {
                if (dp0.get(i).size()==maxSize) {
                    commonSubExp.get(hash).add(dp0.get(i));
                }
                if (dp1.get(i).size()==maxSize) {
                    commonSubExp.get(hash).add(dp1.get(i));
                }
            }
        }
    }




 */

