package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.cost.CostEstimationWrapper;
import org.apache.sysds.hops.rewrite.*;
import org.apache.sysds.hops.rewrite.dfp.AnalyzeSymmetryMatrix;
import org.apache.sysds.hops.rewrite.dfp.DisjointSet;
import org.apache.sysds.hops.rewrite.dfp.Leaf;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.hops.rewrite.dfp.utils.FakeCostEstimator;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.hops.rewrite.dfp.utils.Prime;
import org.apache.sysds.parser.*;
import org.apache.sysds.runtime.controlprogram.Program;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.utils.Explain;

import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.isLeafMatrix;
import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class RewriteCoordinate extends StatementBlockRewriteRule {

    protected final Log LOG = LogFactory.getLog(RewriteCoordinate.class.getName());

    private DisjointSet djs = new DisjointSet(1000);
    private HashMap<Long, Integer> hopId2LeafIndex = new HashMap<>();
    private ArrayList<Leaf> leaves = new ArrayList<>();
    private ArrayList<Range> blockRanges = new ArrayList<>();
    private HashMap<HashKey, ArrayList<Range>> hash2Ranges = new HashMap<>();
    private ExecutionContext ec;
    public StatementBlock statementBlock;

    private RewriteMatrixMultChainOptimization rewriteMatrixMultChainOptimization = new RewriteMatrixMultChainOptimization();
    private RewriteCommonSubexpressionElimination rewriteCommonSubexpressionElimination = new RewriteCommonSubexpressionElimination();
    //  private  final Log LOG = LogFactory.getLog(RewriteCoordinate.class.getName());

    public boolean onlySearchConstantSubExp = false;
    public VariableSet variablesUpdated = null;

    // <configuration>
    private boolean showBlock = false;
    private boolean showSingleCse = false;
    private boolean showMultiCse = false;
    private boolean showCost = false;
    private boolean showMinCostHop = true;
    private boolean showOriginHop = true;

    private boolean skipEstimateStage = false;
    //    private int hSingleCseNumber = 546;
    private int hSingleCseNumber = 355;

    // private int hMultiCseId = 6392;

    private int maxMultiCseNumber = 30000; // 设为-1,则生成所有的；设为正数，则最多生成那么多个


    // </configuration>

    public Hop rewiteHopDag(Hop root) {
        try {

            // 1. 深拷贝，格式化
            Hop hop = deepCopyHopsDag(root);
            hop = reorder(hop);

            hop.resetVisitStatusForced(new HashSet<>());
            LOG.debug("start coordinate " + MyExplain.myExplain(root));
            LOG.debug("after reorder    " + MyExplain.myExplain(hop));

            prepare();

            findAllLeaf(hop, new ArrayList<>(), 0);

            genBlocks();

            if (leaves.size() < 4) return root;

            if (showOriginHop) {
                root.resetVisitStatusForced(new HashSet<>());
                LOG.debug("before coordinate");
                LOG.debug(Explain.explain(root));
            }

            ArrayList<SingleCse> singleCses = genSingleCse();
            if (singleCses.size() <= 13) {
                LOG.debug("ss<=13 return original hop");
                return root;
            }

//            System.out.println("xxx");
//
//            return root;

            if (skipEstimateStage) {
                if (singleCses.size() != hSingleCseNumber) {
                    LOG.debug("not target, return original hop");
                    return root;
                } else {
//                    MultiCse multiCse = createBestMultiCse546(singleCses);
                    MultiCse multiCse = createBestMultiCse1386(singleCses);
                    LOG.debug(multiCse);
                    Hop result = createHop(multiCse, hop);
                    result = deepCopyHopsDag(result);
                    rewriteCommonSubexpressionElimination.rewriteHopDAG(result, new ProgramRewriteStatus());
                    LOG.debug("return best hop");
                    LOG.debug(Explain.explain(result));
                    return result;
                }
            } else {
                // 构造出所有的MultiCse
                ArrayList<MultiCse> multiCses = genMultiCse(singleCses);
                if (multiCses.size() == 0) return root;
                //    if (multiCses.size()>5000 || multiCses.size()==2769 ) return root;

                Hop result = genAndSelectHop(singleCses, multiCses, hop, root);
                if (result != null) {
                    if (showMinCostHop) {
                        result.resetVisitStatusForced(new HashSet<>());
                        LOG.debug("after coordinate");
                        LOG.debug(Explain.explain(result));

                        LOG.info("before coordinate runtime plan");
                        FakeCostEstimator2.printInstructions(constructProgramBlocks(root));
                        LOG.info("after coordinate runtime plan");
                        FakeCostEstimator2.printInstructions(constructProgramBlocks(result));
                    }
                    return result;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.debug("final return original hop");
        return root;
    }

    private int findAllLeaf(Hop hop, ArrayList<Integer> path, int depth) {
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

    private void prepare() {

        djs = new DisjointSet(1000);
        hopId2LeafIndex = new HashMap<>();
        leaves = new ArrayList<>();
        blockRanges = new ArrayList<>();
        hash2Ranges = new HashMap<>();

    }

    private void genBlocks() {

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
                if (onlySearchConstantSubExp && isConstant(l)) continue;
                for (int r = l + 2; r <= block.right; r++) {
                    if (onlySearchConstantSubExp && isConstant(r)) break;
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
    }

    private boolean isConstant(int index) {
        Hop hop = leaves.get(index).hop;
        if (HopRewriteUtils.isTransposeOperation(hop)) hop = hop.getInput().get(0);
        return variablesUpdated.containsVariable(hop.getName());
    }

    private ArrayList<SingleCse> genSingleCse() {
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
                }
            }
        }
        for (int j = 0; j < ranges.size(); j++) {
            result.remove(0);
        }
        return result;
    }

    private ArrayList<MultiCse> genMultiCse(ArrayList<SingleCse> singleCse) {
        long start = System.nanoTime();
        // 按长度，降序
        singleCse.sort((SingleCse a, SingleCse b) -> {
            Range ra = a.ranges.get(0);
            int la = ra.right - ra.left;
            Range rb = b.ranges.get(0);
            int lb = rb.right - rb.left;
            return lb - la;
        });
//        for (int j = 0; j < singleCse.size(); j++) {
//            LOG.debug(singleCse.get(j).ranges.get(0));
//        }

        ArrayList<MultiCse> result = new ArrayList<>();
        for (int j = 0; j < singleCse.size(); j++) {
            MultiCse c = new MultiCse();
            c.cses.add(singleCse.get(j));
            c.last_index = j;
            result.add(c);
        }
        for (int i = 0; i < result.size(); i++) {
            if (maxMultiCseNumber >= 0 && result.size() >= maxMultiCseNumber) break;
            MultiCse frontMC = result.get(i);
            if (showMultiCse) {
                LOG.debug(frontMC);
            }
            for (int index = frontMC.last_index + 1; index < singleCse.size(); index++) {
                SingleCse scA = singleCse.get(index);
                boolean ok = true;
                for (int k = 0; ok && k < frontMC.cses.size(); k++) {
                    SingleCse scB = frontMC.cses.get(k);
                    if (scB.hash == scA.hash || scB.conflict(scA) || !scB.contain(scA)) ok = false;
                }
                if (ok) {
                    MultiCse newMC = new MultiCse();
                    newMC.cses = (ArrayList<SingleCse>) frontMC.cses.clone();
                    newMC.cses.add(scA);
                    newMC.last_index = index;
                    result.add(newMC);
                }
            }
        }
        long end = System.nanoTime();
        LOG.info("number of multi cse = " + result.size());
        LOG.info("bfs search all multi cses cost " + ((end - start) / 1e6) + "ms");
        return result;
    }

    private MultiCse createBestMultiCse546(ArrayList<SingleCse> singleCses) {
        MultiCse m = new MultiCse();
        SingleCse s1 = new SingleCse();
        s1.ranges.add(Range.of(1, 4, false));
        s1.ranges.add(Range.of(5, 8, true));
        s1.ranges.add(Range.of(9, 12, true));
        m.cses.add(s1);

        SingleCse s2 = new SingleCse();
        s2.ranges.add(Range.of(2, 4, false));
        s2.ranges.add(Range.of(5, 7, true));
        s2.ranges.add(Range.of(9, 11, true));
        s2.ranges.add(Range.of(13, 15, false));
        s2.ranges.add(Range.of(22, 24, false));
        m.cses.add(s2);

        SingleCse s3 = new SingleCse();
        s3.ranges.add(Range.of(3, 4, false));
        s3.ranges.add(Range.of(5, 6, true));
        s3.ranges.add(Range.of(9, 10, true));
        s3.ranges.add(Range.of(14, 15, false));
        s3.ranges.add(Range.of(16, 17, false));
        s3.ranges.add(Range.of(18, 19, true));
        s3.ranges.add(Range.of(20, 21, true));
        s3.ranges.add(Range.of(23, 24, false));

        m.cses.add(s3);

        return m;
    }

    private MultiCse createBestMultiCse1386(ArrayList<SingleCse> singleCses) {
        MultiCse m = new MultiCse();
        SingleCse s1 = new SingleCse();
        s1.ranges.add(Range.of(1, 5, false));
        s1.ranges.add(Range.of(6, 10, true));
        s1.ranges.add(Range.of(11, 15, true));
        m.cses.add(s1);
        SingleCse s2 = new SingleCse();
        s2.ranges.add(Range.of(2, 5, false));
        s2.ranges.add(Range.of(6, 9, true));
        s2.ranges.add(Range.of(11, 14, true));
        s2.ranges.add(Range.of(16, 19, false));
        s2.ranges.add(Range.of(26, 29, false));
        m.cses.add(s2);
        SingleCse s3 = new SingleCse();
        s3.ranges.add(Range.of(4, 5, false));
        s3.ranges.add(Range.of(6, 7, true));
        s3.ranges.add(Range.of(11, 12, true));
        s3.ranges.add(Range.of(18, 19, false));
        s3.ranges.add(Range.of(20, 21, false));
        s3.ranges.add(Range.of(22, 23, true));
        s3.ranges.add(Range.of(24, 25, true));
        s3.ranges.add(Range.of(28, 29, false));
        m.cses.add(s3);
        return m;
    }


    private Hop genAndSelectHop(ArrayList<SingleCse> singleCses, ArrayList<MultiCse> multiCses, Hop template, Hop originalRoot) {
        // 构造出所有的Hop

        long constructHopTime = 0;
        long estimateCostTime = 0;
        long start, end;

        MultiCse multiCse = null;
        Hop result = null;
        int id = -1;
        double cost = Double.MAX_VALUE;  //estimate(originalRoot);

        for (int i = 0; i < multiCses.size(); i++) {
            try {
                start = System.nanoTime();
                MultiCse c = multiCses.get(i);
                Hop hop = createHop(c, template);
                end = System.nanoTime();
                constructHopTime += end - start;

                if (hop == null) continue;

                start = System.nanoTime();
                double newCost = 0;
                //   if (multiCses.size()==4643 && i == 4552) {
                newCost = estimate(hop);
                //        System.out.println("x");
                //       }
                end = System.nanoTime();
                estimateCostTime += end - start;

                if (showCost)
                    LOG.debug("cost=" + newCost);

                if (cost > newCost || result == null) {
                    result = hop;
                    cost = newCost;
                    multiCse = c;
                    id = i;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        double newCost = estimate(originalRoot);
        if (cost >= newCost || result == null) {
            result = originalRoot;
            cost = newCost;
            multiCse = null;
            id = -1;
        }

        LOG.info("construct hop time =" + (constructHopTime / 1e6) + "ms");
        LOG.info("estimate cost time =" + (estimateCostTime / 1e6) + "ms");
        LOG.info("minium cost = " + cost);
        LOG.info("original cost = " + newCost);
        if (id != -1) {
            LOG.info("use rewrited hop, multicse id = " + id);
            if (multiCse != null) {
                LOG.info(multiCse);
            }
            // deep copy 是为了清除无用的父节点,返回一个干净的hop
            result = deepCopyHopsDag(result);
            rewriteCommonSubexpressionElimination.rewriteHopDAG(result, new ProgramRewriteStatus());
        } else {
            LOG.info("use original hop");
        }

        return result;
    }

    private double estimate(Hop hop) {
        // 代价估计
        double cost;
        try {
            if (ec == null) {
                cost = FakeCostEstimator.estimate(hop);
            } else {
                Program programBlocks = constructProgramBlocks(hop);
                //cost = CostEstimationWrapper.getTimeEstimate(programBlocks, ec);
                cost = FakeCostEstimator2.estimate(programBlocks);
            }
        } catch (Exception e) {
            e.printStackTrace();
            cost = Double.MAX_VALUE;
        }
        return cost;
    }

    private boolean isTranspose(int l, int r) {
        long first = rangeHash1(l, r);
        long second = rangeHash2(l, r);
        boolean transpose = false;
        if (first < second) {
            transpose = true;
        }
        return transpose;
    }


    private Hop createHop(MultiCse multiCse, Hop template) {
        try {
            Hop copy = deepCopyHopsDag(template);
            // 准备区间数组
            ArrayList<RangeTree> list = new ArrayList<>();
            for (int i = 0; i < leaves.size(); i++) {
                SingleCse sc = new SingleCse(Range.of(i, i, false), leaves.get(i).hop);
                sc.name = getRangeName(i, i);
                RangeTree rangeTree = new RangeTree(i, i, sc, false);
                list.add(rangeTree);
            }
            for (SingleCse sc : multiCse.cses) {
                sc.prototype = null;
                sc.protoRange = null;
                for (Range range : sc.ranges) {
                    list.add(new RangeTree(range.left, range.right, sc, range.transpose));
                }
            }
            for (Range r : blockRanges) {
                SingleCse sc = new SingleCse(Range.of(r.left, r.right, false), null);
                sc.name = getRangeName(r);
                list.add(new RangeTree(r.left, r.right, sc, isTranspose(r.left, r.right)));
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
                // 建起一个块的HopDag
                //    LOG.debug(rt.toString());
                Hop block_hop = rCreateHop(rt);
                if (block_hop == null) {
                    //    LOG.debug("BLOCK HOP NULL");
                    return null;
                }
                // 找到这个块的位置
                Hop cur = copy;
                Hop parent = null;
                Leaf leaf1 = leaves.get(rt.left);
                Leaf leaf2 = leaves.get(rt.right);
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
                    HopRewriteUtils.replaceChildReference(parent, cur, block_hop);
                    HopRewriteUtils.cleanupUnreferenced(cur);
                } else {
                    copy = block_hop;
                }
            }
            if (copy != null) {
                //   copy = deepCopyHopsDag(copy);
                //   rewriteCommonSubexpressionElimination.rewriteHopDAG(copy, new ProgramRewriteStatus());
                return copy;
            }
        } catch (Exception e) {
            LOG.warn("construct hop error");
            return null;
        }
        return null;
    }

    private Hop rCreateHop(RangeTree rt) {
        if (rt.singleCse.prototype != null) {
            if (rt.transpose == rt.singleCse.protoRange.transpose) {
                return rt.singleCse.prototype;
            } else {
                return HopRewriteUtils.createTranspose(rt.singleCse.prototype);
            }
        }
        if (rt.left == rt.right) {
            return leaves.get(rt.left).hop;
        } else {
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
    }

    private Hop build_binary_tree_naive(ArrayList<Hop> mmChain) {
        assert mmChain.size() > 0;
        if (mmChain.size() == 1) return mmChain.get(0);
        Hop ret = mmChain.get(mmChain.size() - 1);
        for (int i = mmChain.size() - 2; i >= 0; i--) {
            ret = HopRewriteUtils.createMatrixMultiply(mmChain.get(i), ret);
        }
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

    public boolean isSame(int l1, int r1, int l2, int r2) {

        boolean result = true;
        if (r1 - l1 != r2 - l2) result = false;
        for (int i = l1, j = l2; result && i <= r1 && j <= r2; i++, j++) {
            Hop h1 = leaves.get(i).hop;
            Hop h2 = leaves.get(j).hop;
            if (HopRewriteUtils.isTransposeOperation(h1) && AnalyzeSymmetryMatrix.querySymmetry(h1.getInput().get(0).getName()))
                h1 = h1.getInput().get(0);
            if (HopRewriteUtils.isTransposeOperation(h2) && AnalyzeSymmetryMatrix.querySymmetry(h2.getInput().get(0).getName()))
                h2 = h2.getInput().get(0);
            if (HopRewriteUtils.isTransposeOperation(h1)) {
                h1 = h1.getInput().get(0);
                if (HopRewriteUtils.isTransposeOperation(h2)) {
                    h2 = h2.getInput().get(0);
                    if (!h1.getName().equals(h2.getName())) result = false;
                } else result = false;
            } else {
                if (HopRewriteUtils.isTransposeOperation(h2)) {
                    result = false;
                } else {
                    if (!h1.getName().equals(h2.getName())) result = false;
                }
            }
            //if (!h1.getName().equals(h2.getName()))
            //result = false;
        }
        if (result == false && getRangeName(l1, r1).equals(getRangeName(l2, r2))) {
            LOG.debug("issame " + l1 + " " + r1 + " " + l2 + " " + r2 + " " + result);
            LOG.debug(getRangeName(l1, r1) + " | " + getRangeName(l2, r2));
            result = true;
            for (int i = l1, j = l2; result && i <= r1 && j <= r2; i++, j++) {
                Hop h1 = leaves.get(i).hop;
                Hop h2 = leaves.get(j).hop;
                if (HopRewriteUtils.isTransposeOperation(h1) && AnalyzeSymmetryMatrix.querySymmetry(h1.getName()))
                    h1 = h1.getInput().get(0);
                if (HopRewriteUtils.isTransposeOperation(h2) && AnalyzeSymmetryMatrix.querySymmetry(h2.getName()))
                    h2 = h2.getInput().get(0);
                LOG.debug(h1.getName() + " " + h2.getName());
            }


        }

        return result;
    }

    private String getRangeName(Range range) {
        return getRangeName(range.left, range.right);
    }

    private String getRangeName(int l, int r) {
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

    public RewriteCoordinate(ExecutionContext executionContext) {
        ec = executionContext;
    }

    @Override
    public boolean createsSplitDag() {
        return false;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {
        try {
            LOG.trace("Rewrite Coordinate StatementBlock");
            statementBlock = sb;
            if (sb.getHops() != null) {
                for (int i = 0; i < sb.getHops().size(); i++) {
                    Hop h = sb.getHops().get(i);
                    Hop g = rewiteHopDag(h);
                    sb.getHops().set(i, g);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList<StatementBlock> list = new ArrayList<>();
        list.add(sb);
        return list;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
        return sbs;
    }

    private Program constructProgramBlocks(Hop hop) {
        DMLProgram prog = new DMLProgram();
        StatementBlock sb = new StatementBlock();
        ArrayList<Hop> hops = new ArrayList<>();
        hops.add(hop);
        sb.setHops(hops);
        sb.setLiveIn(statementBlock.liveIn());
        sb.setLiveOut(statementBlock.liveOut());
        prog.addStatementBlock(sb);
        DMLTranslator dmlt = new DMLTranslator(prog);
        try {
            dmlt.constructLops(prog);
        } catch (Exception e) {
            hop.resetVisitStatusForced(new HashSet<>());
            //  System.out.println(Explain.explain(hop));
            System.out.println("x");
            System.out.println(MyExplain.myExplain(hop));
            System.out.println("y");
        }
        Program rtprog = dmlt.getRuntimeProgram(prog, ConfigurationManager.getDMLConfig());
        return rtprog;
    }


}
