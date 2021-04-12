package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.RewriteMatrixMultChainOptimization;
import org.apache.sysds.hops.rewrite.dfp.AnalyzeSymmetryMatrix;
import org.apache.sysds.hops.rewrite.dfp.DisjointSet;
import org.apache.sysds.hops.rewrite.dfp.Leaf;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.hops.rewrite.dfp.utils.Prime;
import org.apache.sysds.parser.VariableSet;

import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.isLeafMatrix;
import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class Coordinate {

    protected final Log LOG = LogFactory.getLog(Coordinate.class.getName());

    private static RewriteMatrixMultChainOptimization rewriteMatrixMultChainOptimization = new RewriteMatrixMultChainOptimization();

    private static boolean showBlock = false;
    private static boolean showSingleCse = false;

    public ArrayList<Leaf> leaves = new ArrayList<>();

    public VariableSet variablesUpdated = null;

    @Override
    protected Coordinate clone() {
        Coordinate coordinate = new Coordinate();
        coordinate.variablesUpdated = variablesUpdated;
        for (Leaf leaf : leaves) {
            Leaf newLeaf = new Leaf(deepCopyHopsDag(leaf.hop), (ArrayList<Integer>) leaf.path.clone(), leaf.depth);
            coordinate.leaves.add(newLeaf);
        }
        return coordinate;
    }


    Triple<Hop, ArrayList<Range>, ArrayList<SingleCse>> generateOptions(Hop root) {

        long start2 = System.nanoTime();

        // 1. 深拷贝，格式化
        Hop template = deepCopyHopsDag(root);
        template = reorder(template);

        template.resetVisitStatusForced(new HashSet<>());

//        LOG.debug("template: \n" + Explain.explain(template));
//        LOG.info("origin cost=" + originalSolution.cost);
        LOG.debug("after reorder    " + MyExplain.myExplain(template));

        DisjointSet djs = new DisjointSet(1000);

//        hopId2LeafIndex = new HashMap<>();
        leaves = new ArrayList<>();

        // 找到所有的叶子节点
        findAllLeaf(template, new ArrayList<>(), 0, djs);
        if (leaves.size() < 4) {
            long end2 = System.nanoTime();
            RewriteCoordinate.allGenerateOptionsTime += end2 - start2;
            return null;
        }

        for (int i = 0; i < leaves.size(); i++) {
            Hop hop = leaves.get(i).hop;
            if (HopRewriteUtils.isTransposeOperation(hop)) {
                hop = hop.getInput().get(0);
                LOG.info("leaf " + i + " t(" + hop.getName() + ")");
            } else {
                LOG.info("leaf " + i + " " + hop.getName());
            }
        }

        // 生成singleCes

        ArrayList<Range> blockRanges =new ArrayList<>();
        ArrayList<SingleCse> singleCses;

        if (RewriteCoordinate.useDynamicProgramPolicy ) {
            singleCses = genSingleCsePerceiveSameBlock(djs, blockRanges);
        } else {
             singleCses = genSingleCse(djs, blockRanges);
        }

//        ArrayList<Range> blockRanges1 = new ArrayList<>();
//        ArrayList<SingleCse> singleCses1 = genSingleCseSameBlockAttention(djs, blockRanges1);
//        ArrayList<Range> blockRanges2 = new ArrayList<>();
//        ArrayList<SingleCse> singleCses2 = genSingleCse(djs, blockRanges2);
//        LOG.info("singleCses1:" + singleCses1.size() + ", singleCses2:" + singleCses2.size());

        long end2 = System.nanoTime();
        RewriteCoordinate.allGenerateOptionsTime += end2 - start2;

        return Triple.of(template, blockRanges, singleCses);
//        return Triple.of(template, blockRanges1, singleCses1);

    }


    private int findAllLeaf(Hop hop, ArrayList<Integer> path, int depth, DisjointSet djs) {
        //  System.out.println("findAllLeaf visit: " + hop.getHopID() + " " + hop.getName());
        if (isLeafMatrix(hop)
                || (HopRewriteUtils.isTransposeOperation(hop) && isLeafMatrix(hop.getInput().get(0)))
            // || hop.getParent().size() > 1)
        ) {
            int index = leaves.size();
            Leaf leaf = new Leaf(hop, path, depth);
            leaves.add(leaf);
            return index;
        } else {
            if (HopRewriteUtils.isMatrixMultiply(hop)) {
                if (path.size() <= depth) path.add(0);
                else path.set(depth, 0);
                int l = findAllLeaf(hop.getInput().get(0), path, depth + 1, djs);
                path.set(depth, 1);
                int r = findAllLeaf(hop.getInput().get(1), path, depth + 1, djs);
                if (l >= 0 && r >= 0) {
                    djs.merge(l, r);
                }
                return r;
            } else {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    if (path.size() <= depth) path.add(i);
                    else path.set(depth, i);
                    findAllLeaf(hop.getInput().get(i), path, depth + 1, djs);
                }
                return -1;
            }
        }
    }

    private void genBlocksPerceiveSameBlock(DisjointSet djs, ArrayList<Range> blockRanges, HashMap<HashKey, ArrayList<HashSet<Range>>> hash2Rangesset) {
        // 3. 把叶子根据并查集分为多个块
        for (int i = 0; i < leaves.size(); i++) {
            if (djs.find(i) == i) {
                int l = i, r = i;
                while (l - 1 >= 0 && djs.find(l - 1) == djs.find(i)) l--;
                while (r + 1 < leaves.size() && djs.find(r + 1) == djs.find(i)) r++;
                blockRanges.add(Range.of(l, r, false));
                // if (showBlock)
                LOG.info("Range " + l + " " + r + " " + getRangeName(l, r));
            }
        }

        HashMap<HashKey, ArrayList<Range>> hash2blockranges = new HashMap<>();
        for (Range r : blockRanges) {
            long first = rangeHash1(r.left, r.right);
            long second = rangeHash2(r.left, r.right);
            r.transpose = false;
            if (first < second) {
                Long tmp = first;
                first = second;
                second = tmp;
                r.transpose = true;
            }
            HashKey hash = HashKey.of(first, second);
            if (hash2blockranges.containsKey(hash)) {
                hash2blockranges.get(hash).add(r);
            } else {
                ArrayList<Range> ranges1 = new ArrayList<>();
                ranges1.add(r);
                hash2blockranges.put(hash, ranges1);
            }
        }

//        System.out.println(hash2blockranges.size());
//        for (ArrayList<Range> ranges: hash2blockranges.values()) {
//            System.out.println(ranges);
//        }


        // 4. 求出每个区间的哈希值，并把哈希值相同的汇聚起来
        HashMap<HashKey, ArrayList<Range>> hash2Ranges = new HashMap<>();
        for (Range block : blockRanges) {
            for (int l = block.left; l <= block.right; l++) {
                //    LOG.debug("i=" + l + " name=" + leaves.get(l).hop.getName() + " updated=" + variablesUpdated.containsVariable(leaves.get(l).hop.getName()));
                //  if (onlySearchConstantSubExp && notConstant(l)) continue;
                // int r = onlySearchConstantSubExp ? l + 1 : l + 2;
                int r = l + 1;
                for (; r <= block.right; r++) {
                    //  if (onlySearchConstantSubExp && notConstant(r)) break;
                    long first = rangeHash1(l, r);
                    long second = rangeHash2(l, r);
                    boolean transpose = false;
                    if (first < second) {
                        Long tmp = first;
                        first = second;
                        second = tmp;
                        transpose = true;
                    }
                    HashKey hash = HashKey.of(first, second);
                    if (!hash2Ranges.containsKey(hash))
                        hash2Ranges.put(hash, new ArrayList<>());
                    hash2Ranges.get(hash).add(Range.of(l, r, transpose));
                }
            }
        }
        // 过滤掉不是公共子式的区间
        for (Iterator<Map.Entry<HashKey, ArrayList<Range>>> it = hash2Ranges.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<HashKey, ArrayList<Range>> e = it.next();
            ArrayList<Range> list = e.getValue();
            if (list.size() < 2) {
                if (!isConstant(list.get(0).left, list.get(0).right)) {
                    it.remove();
                }
            } else {
                if (list.get(0).right >= list.get(list.size() - 1).left) {
                    it.remove();
                }
            }
        }
        LOG.info("number of commom exp = " + hash2Ranges.size());

        // 根据block分类

        for (Map.Entry<HashKey, ArrayList<Range>> entry : hash2Ranges.entrySet()) {
            ArrayList<HashSet<Range>> arrayList = new ArrayList<>();
            for (ArrayList<Range> blocks : hash2blockranges.values()) {
                HashMap<Long, HashSet<Range>> long2hashSet = new HashMap<>();
                for (Range block : blocks) {
                    for (Range range : entry.getValue()) {
                        if (block.left <= range.left && range.right <= block.right) {
                            long mask;
                            if (block.transpose) mask = block.right - range.right;
                            else mask = range.left - block.left;
                            if (!long2hashSet.containsKey(mask)) long2hashSet.put(mask, new HashSet<>());
                            long2hashSet.get(mask).add(range);
                        }
                    }
                }
                for (HashSet<Range> hashSet : long2hashSet.values()) {
                    if (!hashSet.isEmpty()) {
                        //   System.out.println(hashSet.size() + " " + blocks + " " + hashSet);
                        arrayList.add(hashSet);
                    }
                }
            }
            hash2Rangesset.put(entry.getKey(), arrayList);
        }
    }

    private void genBlocks(DisjointSet djs, ArrayList<Range> blockRanges, HashMap<HashKey, ArrayList<Range>> hash2Ranges) {
        // 3. 把叶子根据并查集分为多个块
        for (int i = 0; i < leaves.size(); i++) {
            if (djs.find(i) == i) {
                int l = i, r = i;
                while (l - 1 >= 0 && djs.find(l - 1) == djs.find(i)) l--;
                while (r + 1 < leaves.size() && djs.find(r + 1) == djs.find(i)) r++;
                blockRanges.add(Range.of(l, r, false));
                if (showBlock)
                    LOG.info("Range " + l + " " + r + " " + getRangeName(l, r));
            }
        }
        // 4. 求出每个区间的哈希值，并把哈希值相同的汇聚起来
        for (Range block : blockRanges) {
            for (int l = block.left; l <= block.right; l++) {
                //    LOG.debug("i=" + l + " name=" + leaves.get(l).hop.getName() + " updated=" + variablesUpdated.containsVariable(leaves.get(l).hop.getName()));
//                if (onlySearchConstantSubExp && notConstant(l)) continue;
                // int r = onlySearchConstantSubExp ? l + 1 : l + 2;
                int r = l + 1;
                for (; r <= block.right; r++) {
//                    if (onlySearchConstantSubExp && notConstant(r)) break;
                    long first = rangeHash1(l, r);
                    long second = rangeHash2(l, r);
                    boolean transpose = false;
                    if (first < second) {
                        Long tmp = first;
                        first = second;
                        second = tmp;
                        transpose = true;
                    }
                    HashKey hash = HashKey.of(first, second);
                    if (!hash2Ranges.containsKey(hash))
                        hash2Ranges.put(hash, new ArrayList<>());
                    hash2Ranges.get(hash).add(Range.of(l, r, transpose));
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
        LOG.info("number of commom exp = " + hash2Ranges.size());

    }

    private boolean isConstant(int left, int right) {
        for (int i = left; i <= right; i++) {
            if (notConstant(i)) return false;
        }
        return true;
    }

    private boolean notConstant(int index) {
        Hop hop = leaves.get(index).hop;
        if (HopRewriteUtils.isTransposeOperation(hop)) hop = hop.getInput().get(0);
        return variablesUpdated.containsVariable(hop.getName());
    }

    private ArrayList<SingleCse> genSingleCse(DisjointSet djs, ArrayList<Range> blockRanges) {
        HashMap<HashKey, ArrayList<Range>> hash2Ranges = new HashMap<>();
        // 划分 块
        genBlocks(djs, blockRanges, hash2Ranges);
        hash2Ranges.forEach((x, y) -> {
            System.out.println(x + "->" + y);
        });


        // 构造出所有的SingleCse
        long start = System.nanoTime();
        ArrayList<SingleCse> result = new ArrayList<>();
        for (Map.Entry<HashKey, ArrayList<Range>> e : hash2Ranges.entrySet()) {
            ArrayList<SingleCse> singleCses = genSingleCseFromRanges(e.getKey(), e.getValue());
            result.addAll(singleCses);
        }
        if (showSingleCse) {
            for (int i = 0; i < result.size(); i++) {
                LOG.info(i + " " + result.get(i));
            }
        }
        long end = System.nanoTime();
        LOG.info("number of single cse = " + result.size());
        LOG.info("bfs search all single cses cost " + ((end - start) / 1e6) + "ms");
        return result;
    }

    private ArrayList<SingleCse> genSingleCseFromRanges(HashKey hash, ArrayList<Range> ranges) {
        ArrayList<SingleCse> result = new ArrayList<>();
        for (int index = 0; index < ranges.size(); index++) {
            SingleCse tmp = new SingleCse(hash, new ArrayList<>(), ranges.get(index), index);
            tmp.name = getRangeName(ranges.get(index));
            result.add(tmp);
        }
        for (int i = 0; i < result.size(); i++) {
            SingleCse frontSC = result.get(i);
            for (int index = frontSC.last_index + 1; index < ranges.size(); index++) {
                Range rangeA = ranges.get(index);
                boolean ok = true;
                for (int k = 0; ok && k < frontSC.ranges.size(); k++) {
                    Range rangeB = frontSC.ranges.get(k);
                    if (rangeA.intersect(rangeB)) {
                        ok = false;
                    }
                }
                if (ok) {
                    SingleCse newSC = new SingleCse(hash, frontSC.ranges, rangeA, index);
                    newSC.name = frontSC.name;
                    result.add(newSC);
//                    if (result.size() % 1000 == 0) {
//                        System.out.println(result.size());
//                    }
                }
            }
        }
        if (ranges.size() < 1) return result;
        boolean isCons = isConstant(ranges.get(0).left, ranges.get(0).right);
        if (!isCons) {
            if (ranges.size() > 0) {
                result.subList(0, ranges.size()).clear();
            }
        } else {
            for (SingleCse singleCse : result) {
                singleCse.isConstant = true;
            }
        }
        return result;
    }

    private ArrayList<SingleCse> genSingleCsePerceiveSameBlock(DisjointSet djs, ArrayList<Range> blockRanges) {
        HashMap<HashKey, ArrayList<HashSet<Range>>> hash2Ranges = new HashMap<>();
        // 划分 块
        genBlocksPerceiveSameBlock(djs, blockRanges, hash2Ranges);
        hash2Ranges.forEach((x, y) -> {
            for (HashSet<Range> hs : y) {
                System.out.print(hs);
            }
            System.out.println("");
        });

        // 构造出所有的SingleCse
        long start = System.nanoTime();
        ArrayList<SingleCse> result = new ArrayList<>();
        for (Map.Entry<HashKey, ArrayList<HashSet<Range>>> e : hash2Ranges.entrySet()) {
            ArrayList<SingleCse> singleCses = genSingleCseFromRangesSameBlockAttention(e.getKey(), e.getValue());
            result.addAll(singleCses);
        }
        if (showSingleCse) {
            for (int i = 0; i < result.size(); i++) {
                LOG.info(i + " " + result.get(i));
            }
        }
        long end = System.nanoTime();
        LOG.info("number of single cse = " + result.size());
        LOG.info("bfs search all single cses cost " + ((end - start) / 1e6) + "ms");
        return result;
    }

    private ArrayList<SingleCse> genSingleCseFromRangesSameBlockAttention(
            HashKey hash,
            ArrayList<HashSet<Range>> rangesets) {
        ArrayList<SingleCse> result = new ArrayList<>();
        for (int index = 0; index < rangesets.size(); index++) {
            SingleCse tmp = new SingleCse(hash, rangesets.get(index), index);
            for (Range r : rangesets.get(index)) {
                tmp.name = getRangeName(r);
                break;
            }
            result.add(tmp);
        }
        for (int i = 0; i < result.size(); i++) {
            SingleCse frontSC = result.get(i);
            for (int index = frontSC.last_index + 1; index < rangesets.size(); index++) {
                HashSet<Range> rangeAset = rangesets.get(index);
                boolean ok = true;
                for (int k = 0; ok && k < frontSC.ranges.size(); k++) {
                    Range rangeB = frontSC.ranges.get(k);
                    for (Range rangeA : rangeAset) {
                        if (rangeA.intersect(rangeB)) {
                            ok = false;
                            break;
                        }
                    }
                }
                if (ok) {
                    SingleCse newSC = new SingleCse(hash, frontSC.ranges, index);
                    newSC.ranges.addAll(rangeAset);
                    newSC.name = frontSC.name;
                    result.add(newSC);
//                    if (result.size() % 1000 == 0) {
//                        System.out.println(result.size());
//                    }
                }
            }
        }
        if (rangesets.size() < 1) return result;
        boolean isCons = true;
        for (Range range : rangesets.get(0)) {
            if (!isConstant(range.left, range.right)) {
                isCons = false;
                break;
            }
        }
        if (!isCons) {
            if (rangesets.size() > 0) {
                result.subList(0, rangesets.size()).clear();
            }
        } else {
            for (SingleCse singleCse : result) {
                singleCse.isConstant = true;
            }
        }
        return result;
    }


    boolean isTranspose(int l, int r) {
        long first = rangeHash1(l, r);
        long second = rangeHash2(l, r);
        boolean transpose = false;
        if (first < second) {
            transpose = true;
        }
        return transpose;
    }

    String getRangeName(Range range) {
        return getRangeName(range.left, range.right);
    }

    String getRangeName(int l, int r) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = l; i <= r; i++) {
            sb.append(MyExplain.myExplain(leaves.get(i).hop));
            sb.append(" ");
        }
        sb.append("}");
        return sb.toString();
    }

    public long rangeHash1(int l, int r) {
        long first = 0L;
        for (int i = 0; l + i <= r; i++) {
            long single;
            Hop h = leaves.get(l + i).hop;
            if (HopRewriteUtils.isTransposeOperation(h)) {
                h = h.getInput().get(0);
                if (AnalyzeSymmetryMatrix.querySymmetry(h.getName())) {
                    single = (long) h.getOpString().hashCode();
                } else {
                    single = (long) h.getOpString().hashCode() * 998244353l;
                }
            } else {
                single = (long) h.getOpString().hashCode();
            }
            first = first + single * Prime.getPrime(i);
        }
        return first;
    }

    public long rangeHash2(int l, int r) {
        long second = 0L;
        for (int i = 0; r - i >= l; i++) {
            long single;
            Hop h = leaves.get(r - i).hop;
            if (HopRewriteUtils.isTransposeOperation(h)) {
                h = h.getInput().get(0);
                single = (long) h.getOpString().hashCode();
            } else {
                if (AnalyzeSymmetryMatrix.querySymmetry(h.getName())) {
                    single = (long) h.getOpString().hashCode();
                } else {
                    single = (long) h.getOpString().hashCode() * 998244353l;
                }
            }
            second = second + single * Prime.getPrime(i);
        }
        return second;
    }


    Hop createHop(SingleCse sc, Hop template, ArrayList<Range> blockRanges) {
        try {
            ArrayList<RangeTree> list = new ArrayList<>();
            sc.prototype = null;
            sc.protoRange = null;
            for (Range range : sc.ranges) {
                list.add(new RangeTree(range.left, range.right, sc, range.transpose));
            }
            return createHopInner(list, template, blockRanges);
        } catch (Exception e) {
            return null;
        }
    }

    Hop createHop(MultiCse multiCse, Hop template, ArrayList<Range> blockRanges) {
        try {
            ArrayList<RangeTree> list = new ArrayList<>();
            for (SingleCse sc : multiCse.cses) {
                sc.prototype = null;
                sc.protoRange = null;
                for (Range range : sc.ranges) {
                    list.add(new RangeTree(range.left, range.right, sc.clone(), range.transpose));
                }
            }
            return createHopInner(list, template, blockRanges);
        } catch (Exception e) {
            return null;
        }
    }

    private Hop createHopInner(ArrayList<RangeTree> list, Hop template, ArrayList<Range> blockRanges) {
        try {
            Hop copy = deepCopyHopsDag(template);
            // 准备区间数组
            for (int i = 0; i < this.leaves.size(); i++) {
                boolean ok = true;
                for (RangeTree rangeTree : list) {
                    if (rangeTree.left == i && rangeTree.right == i) {
                        ok = false;
                        break;
                    }
                }
                if (!ok) continue;
                SingleCse sc = new SingleCse(Range.of(i, i, false), this.leaves.get(i).hop);
                sc.name = this.getRangeName(i, i);
                RangeTree rangeTree = new RangeTree(i, i, sc, false);
                list.add(rangeTree);
            }
//            for (Range r : blockRanges) {
            for (int i = 0; i < blockRanges.size(); i++) {
                Range r = blockRanges.get(i);
                boolean ok = true;
                for (RangeTree rangeTree : list) {
                    if (rangeTree.left == r.left && rangeTree.right == r.right) {
                        ok = false;
                        break;
                    }
                }
                if (!ok) continue;
                SingleCse sc = new SingleCse(Range.of(r.left, r.right, false), null);
                sc.name = this.getRangeName(r);
                list.add(new RangeTree(r.left, r.right, sc, this.isTranspose(r.left, r.right)));
            }
            // 把区间数组排序，先按右端点升序，再按左端点降序
            list.sort((RangeTree ra, RangeTree rb) -> {
                if (ra.right != rb.right)
                    return ra.right - rb.right;
//                if (rb.left - ra.left!=0)
                return rb.left - ra.left;
            });
            // 用栈建起RangeTree
            Stack<RangeTree> stack = new Stack<>();
            for (int i = 0; i < list.size(); i++) {
                RangeTree cur = list.get(i);
                while (!stack.empty() && stack.peek().left >= cur.left) {
                    cur.children.add(stack.pop());
                }
                Collections.reverse(cur.children);
                stack.push(cur);
            }
            // 递归遍历RangeTree，建起HopDag
            for (RangeTree rt : stack) {
//                System.out.println("handle rangetree "+rt);
//                System.out.println(Explain.explain(copy));
                // 建起一个块的HopDag
                //    LOG.debug(rt.toString());
                Hop block_hop = rCreateHop(rt);
//                System.out.println(Explain.explain(block_hop));
                if (block_hop == null) {
                    //    LOG.debug("BLOCK HOP NULL");
                    return null;
                }
                // 找到这个块的位置
                Hop cur = copy;
                Hop parent = null;
                Leaf leaf1 = this.leaves.get(rt.left);
                Leaf leaf2 = this.leaves.get(rt.right);
                for (int i = 0; i < leaf1.depth && i < leaf2.depth; i++) {
                    if (leaf1.path.get(i).equals(leaf2.path.get(i))) {
                        parent = cur;
                        assert cur != null;
                        cur = cur.getInput().get(leaf1.path.get(i));
                    } else {
                        break;
                    }
                }
                // 替换
                if (parent != null) {
//                    System.out.println("replace child reference "+parent.getHopID()+" "+cur.getHopID()+" "+block_hop.getHopID());
                    HopRewriteUtils.replaceChildReference(parent, cur, block_hop);
//                    HopRewriteUtils.cleanupUnreferenced(cur);
                } else {
                    copy = block_hop;
                }
            }
            if (copy != null) {
//                copy = deepCopyHopsDag(copy);
//                rewriteCommonSubexpressionElimination.rewriteHopDAG(copy, new ProgramRewriteStatus());
                return copy;
            }
        } catch (Exception e) {
            LOG.warn("construct hop error");
            e.printStackTrace();
            return null;
        }
        return null;
    }

    Hop rCreateHop(RangeTree rt) {
        if (rt.left == rt.right) {
            return rt.singleCse.prototype;
//            return this.leaves.get(rt.left).hop;
        }
        if (rt.singleCse.prototype != null) {
            rt.singleCse.prototype.shouldPersist = true;
            if (rt.transpose == rt.singleCse.protoRange.transpose) {
                return rt.singleCse.prototype;
            } else {
                return HopRewriteUtils.createTranspose(rt.singleCse.prototype);
            }
        }

        ArrayList<Hop> children = new ArrayList<>();
        for (RangeTree son : rt.children) {
            Hop s = rCreateHop(son);
            if (s == null) return null;
//                if ( ! isSame(son.left,son.right,son.singleCse.protoRange.left,son.singleCse.protoRange.right) )
//                    if (son.transpose!=son.singleCse.protoRange.transpose)
//                    s = HopRewriteUtils.createTranspose(s);
            children.add(s);
        }
        Hop ret = build_binary_tree_mmc(children);
        ret.isConstant = rt.singleCse.isConstant;
        //           Hop ret = build_binary_tree_naive(children);
//            if (!Judge.isSame(ret1,ret2)) {
//                LOG.debug(MyExplain.myExplain(ret2));
//                LOG.debug("naive");
//                LOG.debug(Explain.explain(ret2));
//                if (ret1!=null) {
//                    LOG.debug("mmc");
//                    LOG.debug(Explain.explain(ret1));
//                }
//                LOG.debug("mmc error");
//            }
//            Hop ret = ret1;
        rt.singleCse.prototype = ret;
        rt.singleCse.protoRange = Range.of(rt.left, rt.right, rt.transpose);
        return ret;

    }


    private Hop build_binary_tree_mmc(ArrayList<Hop> mmChain) {
        assert mmChain.size() > 0;
        if (mmChain.size() == 1) return mmChain.get(0);
        ArrayList<Hop> mmOperators = new ArrayList<>();
        Hop ret = mmChain.get(0);
        for (int i = 1; i < mmChain.size(); i++) {
            Hop tmp = HopRewriteUtils.createMatrixMultiply(ret, mmChain.get(i));
            mmOperators.add(tmp);
            ret = tmp;
        }
        Collections.reverse(mmOperators);
        try {
            rewriteMatrixMultChainOptimization.optimizeMMChain(ret, mmChain, mmOperators, new ProgramRewriteStatus());
        } catch (Exception e) {
            e.printStackTrace();
//            ret.resetVisitStatusForced(new HashSet<>());
//            LOG.debug(Explain.explain(ret));
            ret = null;
        }
        if (ret != null) {
//            LOG.debug("ret no null");
        }
        return ret;
    }


}
