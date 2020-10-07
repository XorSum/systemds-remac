package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.AnalyzeSymmetryMatrix;
import org.apache.sysds.hops.rewrite.dfp.DisjointSet;
import org.apache.sysds.hops.rewrite.dfp.Leaf;
import org.apache.sysds.hops.rewrite.dfp.utils.FakeCostEstimator;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.hops.rewrite.dfp.utils.Prime;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;

import java.util.*;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.isLeafMatrix;
import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class RewriteCoordinate {


    static DisjointSet djs = new DisjointSet(1000);
    static HashMap<Long, Integer> hopId2LeafIndex = new HashMap<>();
    public static ArrayList<Leaf> leaves = new ArrayList<>();
    private static ArrayList<Range> nodeRange = new ArrayList<>();
    private static HashMap<HashKey, ArrayList<Range>> hash2Ranges = new HashMap<>();
    private static ExecutionContext ec;

    public static void main(Hop hop, ExecutionContext executionContext) {
        ec = executionContext;
        djs = new DisjointSet(1000);
        hopId2LeafIndex = new HashMap<>();
        leaves = new ArrayList<>();
        nodeRange = new ArrayList<>();
        hash2Ranges = new HashMap<>();

        // 1. 格式化
        hop = reorder(hop);

        System.out.println(MyExplain.myExplain(hop));
        // 2. 找到所有的叶子，把叶子根据乘法放入并查集中
        findAllLeaf(hop, new ArrayList<>(), 0);
        // 3. 把叶子根据并查集分为多个块
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
        // 4. 求出每个区间的哈希值，并把哈希值相同的汇聚起来
        for (Range block : nodeRange) {
            for (int l = block.left; l <= block.right; l++) {
                for (int r = l + 1; r <= block.right; r++) {
                    HashKey hash = rangeHash(l, r);
                    if (!hash2Ranges.containsKey(hash))
                        hash2Ranges.put(hash, new ArrayList<>());
                    hash2Ranges.get(hash).add(Range.of(l, r));
                }
            }
        }
        // 过滤掉不是公共子式的区间
        for (Iterator<Map.Entry<HashKey, ArrayList<Range>>> it = hash2Ranges.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<HashKey, ArrayList<Range>> e = it.next();
            ArrayList<Range> list = e.getValue();
            if (list.size() < 2 || list.get(0).right >= list.get(list.size() - 1).left) {
                it.remove();
            }
        }

        // 构造出所有的SingleCse
        ArrayList<SingleCse> singleCse = new ArrayList<>();
        for (Map.Entry<HashKey, ArrayList<Range>> e : hash2Ranges.entrySet()) {
            ArrayList<SingleCse> sc = genSingleCse(e.getKey(), e.getValue());
            singleCse.addAll(sc);
//            Range r = e.getValue().get(0);
//            System.out.print("\n\nexp = ");
//            for (int i = r.left; i <= r.right; i++) {
//                System.out.print(MyExplain.myExplain(leaves.get(i).hop) + " ");
//            }
//            System.out.println();
//            System.out.println("hash = " + e.getKey());
//            for (Range range : e.getValue()) {
//                System.out.println("range = [" + range.left + "," + range.right + "]");
//            }
//            System.out.println(sc.size());
//            for (SingleCse c : sc) {
//                System.out.println(c.ranges);
//            }
        }


        // 构造出所有的MultiCse
        long srtart = System.currentTimeMillis();
        ArrayList<MultiCse> cs = genMultiCse(singleCse);
        System.out.println("cses.size=" + cs.size());

        long end = System.currentTimeMillis();
        System.out.println("生成计划耗时" + (end - srtart) + "ms");
//        if (cses.size()>70000) {
//            for (int i=70000;i<70010;i++) {
//                genHop(cses.get(i));
//            }
//        }


        // 构造出所有的Hop
        srtart = System.currentTimeMillis();
        ArrayList<Hop> hops = new ArrayList<>();
        for (MultiCse c : cs) {
            Hop hop1 = genHop(c);
            hops.add(hop1);
            FakeCostEstimator.estimate(hop);
        }

        end = System.currentTimeMillis();
        System.out.println("生成hop耗时" + (end - srtart) + "ms");
        System.out.println("cses.size=" + cs.size());

//        genCsesBaoli();
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


    public static ArrayList<SingleCse> genSingleCse(HashKey hash, ArrayList<Range> ranges) {
        ArrayList<SingleCse> result = new ArrayList<>();
        for (int j = 0; j < ranges.size(); j++) {
            SingleCse tmp = new SingleCse(hash);
            tmp.ranges.add(ranges.get(j));
            tmp.last_index = j;
            result.add(tmp);
        }
        for (int i = 0; i < result.size(); i++) {
            SingleCse tmp = result.get(i);
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
                    SingleCse xin = new SingleCse(hash);
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


    private static ArrayList<MultiCse> genMultiCse(ArrayList<SingleCse> singleCse) {
        long start = System.currentTimeMillis();
        ArrayList<MultiCse> result = new ArrayList<>();
        for (int j = 0; j < singleCse.size(); j++) {
            MultiCse c = new MultiCse();
            c.cses.add(singleCse.get(j));
            c.last_index = j;
            result.add(c);
        }
        for (int i = 0; i < result.size(); i++) {
            //   System.out.println("i=" + i);
            MultiCse front = result.get(i);
            //    System.out.println(front);
            for (int j = front.last_index + 1; j < singleCse.size(); j++) {
                SingleCse cj = singleCse.get(j);
                boolean ok = true;
                for (int k = 0; ok && k < front.cses.size(); k++) {
                    SingleCse ck = front.cses.get(k);
                    if (ck.hash == cj.hash || ck.intersect(cj)) ok = false;
                }
                if (ok) {
                    MultiCse xin = new MultiCse();
                    xin.cses = (ArrayList<SingleCse>) front.cses.clone();
                    xin.cses.add(cj);
                    xin.last_index = j;
                    result.add(xin);
                }
            }
        }
        long end = System.currentTimeMillis();
        long totalTime = end - start;
        System.out.println(">广搜所有相容坐标耗时：" + totalTime + " ms");
        return result;
    }


    private static Hop genHop(MultiCse multiCse) {
        int n = multiCse.cses.size();
        multiCse.hops = new ArrayList<>();
        for (int i = 0; i < n; i++) multiCse.hops.add(null);
        multiCse.contain = new boolean[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                SingleCse ci = multiCse.cses.get(i);
                SingleCse cj = multiCse.cses.get(j);
                multiCse.contain[i][j] = ci.contain(cj);
                //  System.out.print(cses.contain[i][j]+" ");
            }
            //  System.out.println();
        }
        for (Range block : nodeRange) {
            Hop block_hop = build_sub_hop(multiCse, block.left, block.right);
            if (block_hop == null) {
                System.out.println("NULL");
            }
        }
        return null;
    }


    private static Hop build_sub_hop(MultiCse multiCse, int L, int R) {
        ArrayList<Pair<Range, SingleCse>> ranges = new ArrayList<>();
        for (SingleCse singleCse : multiCse.cses) {
            for (Range range : singleCse.ranges) {
                if (range.left >= L && range.right <= R) {
                    ranges.add(Pair.of(range, singleCse));
                    break;
                }
            }
        }
        Stack<Pair<Range, SingleCse>> stack = new Stack<>();
        for (Pair<Range, SingleCse> p : ranges) {
            while (!stack.empty() && stack.peek().getLeft().left >= p.getLeft().left) stack.pop();
            stack.push(p);
        }
        //  System.out.println("stack "+L+ " "+R);
        ArrayList<Hop> children = new ArrayList<>();
        int cur = L;
        for (Pair<Range, SingleCse> p : stack) {
            //   System.out.println(p.getLeft());
            int l = p.getLeft().left;
            int r = p.getLeft().right;
            while (cur < l) {
                children.add(leaves.get(cur).hop);
                cur++;
            }
            Hop t = build_sub_hop(multiCse, p.getRight(), p.getLeft().left, p.getLeft().right);
            children.add(t);
            cur = r + 1;
        }
        while (cur <= R) {
            children.add(leaves.get(cur).hop);
            cur++;
        }
        if (children.size() > 0) {
            Hop ret = children.get(0);
            for (int i = 1; i < children.size(); i++) {
                ret = HopRewriteUtils.createMatrixMultiply(ret, children.get(i));
            }
            return ret;
        } else {
            return null;
        }
    }


    private static Hop build_sub_hop(MultiCse multiCse, SingleCse singleCse, int L, int R) {
        int index = multiCse.cses.indexOf(singleCse);
        Hop ret = multiCse.hops.get(index);
        if (ret != null) {
            return ret;
        }
        ArrayList<Pair<Range, SingleCse>> stack = new ArrayList<>();
        for (int j = 0; j < multiCse.cses.size(); j++) {
            if (multiCse.contain[index][j]) {
                SingleCse cj = multiCse.cses.get(j);
                int l = -1, r = -1;
                for (Range range : cj.ranges) {
                    if (range.left >= L && range.right <= R && (range.right - range.left < R - L)) {
                        l = range.left;
                        r = range.right;
                        break;
                    }
                }
                if (l != -1 && r != -1)
                    stack.add(Pair.of(Range.of(l, r), cj));
            }
        }
        // System.out.println(" cse:");
        ArrayList<Hop> children = new ArrayList<>();
        int cur = L;
        for (Pair<Range, SingleCse> p : stack) {
            //   System.out.println(" "+p.getLeft());
            int l = p.getLeft().left;
            int r = p.getLeft().right;
            while (cur < l) {
                children.add(leaves.get(cur).hop);
                cur++;
            }
            Hop t = build_sub_hop(multiCse, p.getRight(), l, r);
            children.add(t);
            cur = r + 1;
        }
        while (cur <= R) {
            children.add(leaves.get(cur).hop);
            cur++;
        }
        if (children.size() > 0) {
            ret = children.get(0);
            for (int i = 1; i < children.size(); i++) {
                ret = HopRewriteUtils.createMatrixMultiply(ret, children.get(i));
            }
        }
        multiCse.hops.set(index, ret);
        return ret;
    }


    private static ArrayList<MultiCse> genCsesBaoli() {
        long start = System.currentTimeMillis();
        ArrayList<MultiCse> result = new ArrayList<>();
        MultiCse multiCse = new MultiCse();
        result.add(multiCse);
        for (Map.Entry<HashKey, ArrayList<Range>> e : hash2Ranges.entrySet()) {
            ArrayList<SingleCse> cses1 = genSingleCse(e.getKey(), e.getValue());
            int size = result.size();
            System.out.println("size=" + size + " x" + (cses1.size() + 1));
            for (int j = 0; j < size; j++) {
                for (SingleCse c : cses1) {
                    MultiCse novel = new MultiCse();
                    novel.cses = (ArrayList<SingleCse>) result.get(j).cses.clone();
                    novel.cses.add(c);
                    result.add(novel);
                }
            }
        }
        System.out.println("baoli size = " + result.size());
        long end = System.currentTimeMillis();
        long totalTime = end - start;
        System.out.println(">构造所有坐标耗时：" + totalTime + " ms");
        start = System.currentTimeMillis();
        ArrayList<MultiCse> compatibleResult = new ArrayList<>();
        for (MultiCse c : result) {
            boolean compatible = true;
            for (int i = 0; compatible && i < c.cses.size(); i++) {
                for (int j = i + 1; compatible && j < c.cses.size(); j++) {
                    if (c.cses.get(i).intersect(c.cses.get(j))) {
                        compatible = false;
                    }
                }
            }
            if (compatible) compatibleResult.add(c);
        }

        System.out.println("compitable size = " + compatibleResult.size());
        end = System.currentTimeMillis();
        totalTime = end - start;
        System.out.println(">判断相容坐标执行耗时：" + totalTime + " ms");

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
