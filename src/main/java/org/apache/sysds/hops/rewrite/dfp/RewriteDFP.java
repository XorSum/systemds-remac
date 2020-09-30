package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.*;
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil;
import org.apache.sysds.hops.rewrite.dfp.utils.Hash;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.hops.rewrite.dfp.utils.Prime;
import org.spark_project.jetty.util.ArrayQueue;


import java.util.*;

import static java.lang.Math.min;
import static java.lang.Math.max;
//import static org.apache.commons.lang3.ObjectUtils.max;
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
    private static ArrayList<Range> nodeRange = new ArrayList<>();
    private static HashMap<HashKey, ArrayList<Range>> tmp = new HashMap<>();


    static class Range {
        public int left;
        public int right;
        public static Range of(int l,int r) {
            Range range = new Range();
            range.left = l;
            range.right = r;
            return range;
        }

        @Override
        public String toString() {
            return "(" + left + "," + right + ")";
        }
    }


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

    static  int rrr =0;
    static int sss = 0;

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
                nodeRange.add(Range.of(l, r));
                System.out.println("Range " + l + " " + r);
                for (int j = l; j <= r; j++) {
                    System.out.print(MyExplain.myExplain(leaves.get(j).hop) + " ");
                }
                System.out.println();
            }
        }

        for (Range block : nodeRange) {
            for (int l = block.left; l <= block.right; l++) {
                for (int r = l + 1; r <= block.right; r++) {
                    HashKey hash = rangeHash(l, r);
                    if (!tmp.containsKey(hash))
                        tmp.put(hash, new ArrayList<>());
                    tmp.get(hash).add(Range.of(l, r));
                }
            }
        }

        for (Iterator<Map.Entry<HashKey, ArrayList<Range>>> it = tmp.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<HashKey, ArrayList<Range>> e = it.next();
            ArrayList<Range> list = e.getValue();
            if (list.size() < 2 || list.get(0).right >= list.get(list.size() - 1).left) {
                it.remove();
            }
        }

        ArrayList<Cse> cse = new ArrayList<>();
        for (Map.Entry<HashKey, ArrayList<Range>> e : tmp.entrySet()) {
            System.out.print("\n\nexp = ");
            Range r = e.getValue().get(0);
            for (int i = r.left; i <= r.right; i++) {
                System.out.print(MyExplain.myExplain(leaves.get(i).hop) + " ");
            }
            System.out.println();
            System.out.println("hash = " + e.getKey());
            for (Range range : e.getValue()) {
                System.out.println("range = [" + range.left + "," + range.right + "]");
            }
            ArrayList<Cse> ac = genCse(e.getKey(), e.getValue());
            cse.addAll(ac);
            System.out.println(ac.size());
            for (Cse c: ac) {
                System.out.println(c.ranges);
            }
        }
        ArrayList<Cses> cses = genCses(cse);
        System.out.println("cses.size="+cses.size());
        System.out.println("rrr="+rrr);
        System.out.println("sss="+sss);
    }

    static class Cse {
        public HashKey hash;
        public ArrayList<Range> ranges;
        public  int last_index=0;
        public Cse(HashKey hash) {
            this.hash = hash;
            this.ranges = new ArrayList<>();
        }
        public boolean intersect( Cse other ) {
            sss ++;
            for (Range p: ranges) {
                for (Range q : other.ranges) {
                    rrr++;
                    boolean a = max(p.left,q.left) <=min(p.right,q.right);
                    boolean b = (p.left<=q.left&&p.right>=q.right)||(q.left<=p.left&&q.right>=p.right);
                    if (a && !b) return true;
//                    if (a) return true;
                }
            }
            return false;
        }
    }

    static class HashKey {
        public long left;
        public long right;
        public static HashKey of(Long l,Long r) {
            HashKey key = new HashKey();
            key.left = l;
            key.right = r;
            return key;
        }

        @Override
        public boolean equals(Object obj) {
            HashKey o = (HashKey)obj;
            return left == o.left && right==o.right;
        }

        @Override
        public int hashCode() {
            return (int)(left*(1e9+7)+right);
        }
    }

    public static ArrayList<Cse> genCse(HashKey hash, ArrayList<Range> ranges) {
        ArrayList<Cse> result = new ArrayList<>();
        for (int j = 0; j < ranges.size(); j++) {
            Cse tmp = new Cse(hash);
            tmp.ranges.add(ranges.get(j));
            tmp.last_index = j;
            result.add(tmp);
        }
        for (int i = 0; i < result.size(); i++) {
            Cse tmp = result.get(i);
            for (int j = tmp.last_index + 1; j < ranges.size(); j++) {
                Range a = ranges.get(j);
                boolean ok = true;
                for (int k = 0; ok && k < tmp.ranges.size(); k++) {
                    Range b = tmp.ranges.get(k);
                    if (max(a.left, b.right) <= min(a.right, b.right)) {
                        ok = false;
                    }
                }
                if (ok) {
                    Cse xin = new Cse(hash);
                    xin.ranges = (ArrayList<Range>) tmp.ranges.clone();
                    xin.ranges.add(a);
                    xin.last_index = j;
                    result.add(xin);
                }
            }
        }
        for (int j = 0; j < ranges.size(); j++) {
            result.remove(0);
        }
        return result;
    }

    static class Cses {
        public ArrayList<Cse> cses= new ArrayList<>();
        int last_index=0;
        public Cses() {}

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("cses:\n");
            for (Cse c: cses) {
                for (int i=c.ranges.get(0).left;i<=c.ranges.get(0).right;i++) {
                    Hop h = leaves.get(i).hop;
                    if (!HopRewriteUtils.isTransposeOperation(h)) {
                        sb.append(h.getName()+" ");
                    }else  {
                        h = h.getInput().get(0);
                        sb.append("t("+h.getName()+") ");
                    }
                }
                for (Range r: c.ranges) {
                    sb.append(r+" ");
                }
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    private static ArrayList<Cses> genCses(ArrayList<Cse> cse) {
        ArrayList<Cses> result = new ArrayList<>();
        for (int j=0;j<cse.size();j++) {
            Cses c = new Cses();
            c.cses.add(cse.get(j));
            c.last_index = j;
            result.add(c);
        }
        for (int i=0;i<result.size();i++) {
            System.out.println("i="+i);
            Cses front = result.get(i);
            System.out.println(front);
//            System.out.println("Front: ");
//            for (Cse c: front.cses) {
//                System.out.println(c.hash +" "+c.ranges);
//            }
//            System.out.println("\n\n");
            for (int j=front.last_index+1;j<cse.size();j++) {
                Cse cj = cse.get(j);
                boolean ok = true;
                for (int k=0;ok&&k<front.cses.size();k++) {
                    Cse ck = front.cses.get(k);
                    if ( ck.hash == cj.hash || ck.intersect(cj)  ) ok = false;
                }
                if (ok) {
                    Cses xin = new Cses();
                    xin.cses = (ArrayList<Cse>)front.cses.clone();
                    xin.cses.add( cj );
                    xin.last_index = j;
                    result.add(xin);
//                    System.out.println("Xin: ");
//                    for (Cse c: xin.cses) {
//                        System.out.println(c.hash +" "+c.ranges);
//                    }
//                    System.out.println("\n\n");

                }
            }
        }
        return result;
    }

    public static HashKey rangeHash(int l, int r) {
        Long first = 0L;
        for (int i = 0; l + i <= r; i++) {
            Long single;
            Hop h = leaves.get(l + i).hop;
            if (HopRewriteUtils.isTransposeOperation(h)) {
                h = h.getInput().get(0);
                if (AnalyzeSymmetryMatrix.querySymmetry(h.getName())) {
                    single = (long) h.getOpString().hashCode();
                } else {
                    single = (long) h.getOpString().hashCode() * 998244353;
                }
            } else {
                single = (long) h.getOpString().hashCode();
            }
            first = first + single * Prime.getPrime(i);
        }
        Long second = 0L;
        for (int i = 0; r - i >= l; i++) {
            Long single;
            Hop h = leaves.get(r - i).hop;
            if (HopRewriteUtils.isTransposeOperation(h)) {
                h = h.getInput().get(0);
                single = (long) h.getOpString().hashCode();
            } else {
                if (AnalyzeSymmetryMatrix.querySymmetry(h.getName())) {
                    single = (long) h.getOpString().hashCode();
                } else {
                    single = (long) h.getOpString().hashCode() * 998244353;
                }
            }
            second = second + single * Prime.getPrime(i);
        }
        if (first < second) {
            Long tmp = first;
            first = second;
            second = tmp;
        }
        return HashKey.of(first, second);
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

