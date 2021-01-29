package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.*;
import org.apache.sysds.hops.rewrite.dfp.AnalyzeSymmetryMatrix;
import org.apache.sysds.hops.rewrite.dfp.DisjointSet;
import org.apache.sysds.hops.rewrite.dfp.Leaf;
import org.apache.sysds.hops.rewrite.dfp.MySolution;
import org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.hops.rewrite.dfp.dp.CostTree;
import org.apache.sysds.hops.rewrite.dfp.dp.HopCostEstimator;
import org.apache.sysds.hops.rewrite.dfp.utils.*;
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
    public ConstantUtil constantUtil = null; // new ConstantUtil(variablesUpdated);

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

    private int maxMultiCseNumber = 100000; // 设为-1,则生成所有的；设为正数，则最多生成那么多个

    private static long epoch = 100;

    // </configuration>

    public MySolution rewiteHopDag(Hop root) {
//        System.out.println("rootname"+root.getName());
//        System.out.println("xx");

        MySolution originalSolution = new MySolution(root);

        try {
            originalSolution.cost = estimate(originalSolution, true);
            if (!"h".equals(root.getName())) {
                //   HopCostEstimator.buildMMNodeTree(root);
                return originalSolution;
            }

            // 1. 深拷贝，格式化
            Hop hop = deepCopyHopsDag(root);
            hop = reorder(hop);

            hop.resetVisitStatusForced(new HashSet<>());
            LOG.debug("start coordinate " + MyExplain.myExplain(root));

            LOG.info("origin cost=" + originalSolution.cost);
            System.out.println(ec.getVariables().keySet());
            LOG.debug("after reorder    " + MyExplain.myExplain(hop));

            prepare();

            // 找到所有的叶子节点
            findAllLeaf(hop, new ArrayList<>(), 0);

            // 划分 块
            genBlocks();

            if (leaves.size() < 4) return originalSolution;

            if (showOriginHop) {
                root.resetVisitStatusForced(new HashSet<>());
                LOG.debug("before coordinate");
                LOG.debug(Explain.explain(root));
            }


//            Program program = constructProgramBlocks(root);
//            System.out.println(Explain.explain(program));
//            System.out.println(Explain.explain(program));
//            System.out.println("x");

//            try {
//                MySolution aaaSolution = createAAA(hop);
//                return aaaSolution;
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.exit(0);
//            }
            // 生成singleCes
            ArrayList<SingleCse> singleCses = genSingleCse();

            try {
                MySolution dpSolution = testCostTree(singleCses, hop);
                LOG.info("return dynamic program solution");
                return dpSolution;
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }

            if (true) {
                LOG.info("return original solution");
                return originalSolution;
            }
//            if (!onlySearchConstantSubExp && singleCses.size() <= 13) {
//                LOG.debug("ss<=13 return original hop");
//                return originalSolution;
//            }

//            System.out.println("xxx");
//
//            return root;

            if (skipEstimateStage) {
                if (singleCses.size() != hSingleCseNumber) {
                    LOG.debug("not target, return original hop");
                    return originalSolution;
                } else {
//                    MultiCse multiCse = createBestMultiCse546(singleCses);
                    MultiCse multiCse = createBestMultiCse1386(singleCses);
                    LOG.debug(multiCse);
                    Hop result = createHop(multiCse, hop);
                    result = deepCopyHopsDag(result);
                    rewriteCommonSubexpressionElimination.rewriteHopDAG(result, new ProgramRewriteStatus());
                    LOG.debug("return best hop");
                    LOG.debug(Explain.explain(result));
                    MySolution solution = new MySolution(result);

                    return solution;
                }
            } else {
                // 构造出所有的MultiCse
                ArrayList<MultiCse> multiCses = genMultiCse(singleCses);

//                try {
//                    originalSolution.cost = estimate(originalSolution);
//                    System.out.println("cost="+originalSolution.cost);
//                }catch (Exception e) {
//                    e.printStackTrace();
//                    System.out.println("xxx");
//                }

                if (multiCses.size() == 0) return originalSolution;
                //    if (multiCses.size()>5000 || multiCses.size()==2769 ) return root;


                // 构造计划
                ArrayList<MySolution> solutions = genSolutions(multiCses, hop);

                // 估代价并返回代价最小的计划
                MySolution solution = selectSolution(solutions, originalSolution);
//                Hop result = genAndSelectHop(singleCses, multiCses, hop, root);
                if (solution.body != null) {
                    if (showMinCostHop) {
                        solution.body.resetVisitStatusForced(new HashSet<>());
                        LOG.debug("after coordinate");
                        LOG.debug(solution);

//                        LOG.info("before coordinate runtime plan");
//                        FakeCostEstimator2.printInstructions(constructProgramBlocks(root));
//                        LOG.info("after coordinate runtime plan");
//                        FakeCostEstimator2.printInstructions(constructProgramBlocks(solution.body));
                    }
                    return solution;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.debug("final return original hop");

        return originalSolution;

    }

    MySolution testCostTree(ArrayList<SingleCse> singleCses, Hop template) {
//        ArrayList<Pair<Hop, SingleCse>> pairs = new ArrayList<>();
//        for (SingleCse singleCse : singleCses) {
//            MultiCse multiCse = new MultiCse();
//            multiCse.cses.add(singleCse);
//            Hop hop = createHop(multiCse, template);
//            pairs.add(Pair.of(hop, singleCse));
//        }
        ArrayList<Pair<SingleCse, Hop>> list = genHopFromSingleCses(singleCses, template);

        CostTree costTree = new CostTree(variablesUpdated);
//        costTree.testCostTree(list);
        ArrayList<MultiCse> multiCse = costTree.testOperatorGraph(list);
        ArrayList<MySolution> solutions = genSolutions(multiCse, template);

        MySolution mySolution = new MySolution(template);

        // 估代价并返回代价最小的计划
        MySolution solution = selectSolution(solutions, mySolution);

        solution.body = deepCopyHopsDag(solution.body);
        rewriteCommonSubexpressionElimination.rewriteHopDAG(solution.body, new ProgramRewriteStatus());

        LOG.info("dp result:");
        LOG.info(solution);
        System.out.println("x");
//        System.exit(0);
        return solution;
    }

    MySolution createAAA(Hop template) {
//        MultiCses:{
//            SingleCse: name={h g } ranges=[(4,5,false),(6,7,true),(11,12,true),(18,19,false),(20,21,false),(28,29,false),]
//            SingleCse: name={t(a) a h g } ranges=[(2,5,false),(6,9,true),(11,14,true),(16,19,false),(26,29,false),]
//            SingleCse: name={h t(a) a h g } ranges=[(1,5,false),(6,10,true),(11,15,true),(25,29,false),]
//        }
        MultiCse multiCse = new MultiCse();
        SingleCse singleCse1 = new SingleCse();
        singleCse1.ranges.add(Range.of(4, 5, false));
        singleCse1.ranges.add(Range.of(6, 7, true));
        singleCse1.ranges.add(Range.of(11, 12, true));
        singleCse1.ranges.add(Range.of(18, 19, false));
        singleCse1.ranges.add(Range.of(20, 21, false));
        singleCse1.ranges.add(Range.of(28, 29, false));
        multiCse.cses.add(singleCse1);
        SingleCse singleCse2 = new SingleCse();
        singleCse2.ranges.add(Range.of(2, 5, false));
        singleCse2.ranges.add(Range.of(6, 9, true));
        singleCse2.ranges.add(Range.of(11, 14, true));
        singleCse2.ranges.add(Range.of(16, 19, false));
        singleCse2.ranges.add(Range.of(26, 29, false));
        multiCse.cses.add(singleCse2);
        SingleCse singleCse3 = new SingleCse();
        singleCse3.ranges.add(Range.of(1, 5, false));
        singleCse3.ranges.add(Range.of(6, 10, true));
        singleCse3.ranges.add(Range.of(11, 15, true));
        singleCse3.ranges.add(Range.of(25, 29, false));
        multiCse.cses.add(singleCse3);
        ArrayList<MultiCse> multiCses = new ArrayList<>();
        multiCses.add(multiCse);

        ArrayList<MySolution> solutions = genSolutions(multiCses, template);

        MySolution mySolution = new MySolution(template);

        // 估代价并返回代价最小的计划
        MySolution solution = selectSolution(solutions, mySolution);

        solution.body = deepCopyHopsDag(solution.body);
        rewriteCommonSubexpressionElimination.rewriteHopDAG(solution.body, new ProgramRewriteStatus());

        return solution;
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
                if (onlySearchConstantSubExp && notConstant(l)) continue;
                // int r = onlySearchConstantSubExp ? l + 1 : l + 2;
                int r = l + 1;
                for (; r <= block.right; r++) {
                    if (onlySearchConstantSubExp && notConstant(r)) break;
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

    private boolean notConstant(int index) {
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
                    if (result.size() % 1000 == 0) {
                        System.out.println(result.size());
                    }
                }
            }
        }
        for (int j = 0; j < ranges.size(); j++) {
            result.remove(0);
        }
        return result;
    }

    static boolean searchCombinedCandidates = true;

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
        if (searchCombinedCandidates) {
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


    private ArrayList<MySolution> genSolutions(ArrayList<MultiCse> multiCses, Hop template) {
        long constructHopTime = 0;
        long start = System.nanoTime();
        ArrayList<MySolution> solutions = new ArrayList<>();
        HashMap<Pair<Long, Long>, ArrayList<Hop>> filter = new HashMap<>();
        for (int i = 0; i < multiCses.size(); i++) {
            try {
                MultiCse c = multiCses.get(i);
                Hop hop = createHop(c, template);
                Pair<Long, Long> key = Hash.hashHopDag(hop);
                if (filter.containsKey(key)) {
                    for (Hop another : filter.get(key)) {
                        if (Judge.isSame(hop, another)) {
                            hop = null;
                        }
                    }
                    if (hop != null) {
                        filter.get(key).add(hop);
                    }
                } else {
                    ArrayList<Hop> arrayList = new ArrayList<>();
                    arrayList.add(hop);
                    filter.put(key, arrayList);
                }
                if (hop != null) {
                    if (onlySearchConstantSubExp) {
                        Hop copy = deepCopyHopsDag(hop);
                        MySolution mySolution = constantUtil.liftLoopConstant(copy);
                        solutions.add(mySolution);
                    } else {
                        MySolution solution = new MySolution(c, hop);
                        solutions.add(solution);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long end = System.nanoTime();
        constructHopTime += end - start;
        LOG.info("solution size = " + solutions.size());
        LOG.info("construct hop time =" + (constructHopTime / 1e6) + "ms");
        return solutions;
    }

    private MySolution selectSolution(ArrayList<MySolution> solutions, MySolution originSolution) {
        long estimateCostTime = 0;
        long start, end;
        int id = -1;
        start = System.nanoTime();
        MySolution bestSolution = new MySolution();

        for (int i = 0; i < solutions.size(); i++) {
            try {
                MySolution solution = solutions.get(i);
                solution.cost = estimate(solution, false);
                if (showCost)
                    LOG.debug("cost=" + solution.cost);
                if (bestSolution.body == null
                        || bestSolution.cost > solution.cost
                        || (Math.abs(bestSolution.cost - solution.cost) < 0.0001
                        && bestSolution.multiCse.cses.size() < solution.multiCse.cses.size())) {
                    bestSolution = solution;
                    id = i;
                }
            } catch (Exception e) {
                LOG.error("estimate error");
                //   e.printStackTrace();
            }
        }
        LOG.info("minium cost = " + bestSolution.cost);
        if (bestSolution.body != null) {
            estimate(bestSolution, true);
        }
        if (bestSolution.body == null || bestSolution.cost >= originSolution.cost) {
            bestSolution = originSolution;
            id = -1;
        }

        end = System.nanoTime();
        estimateCostTime += end - start;
        LOG.info("estimate cost time =" + (estimateCostTime / 1e6) + "ms");

        LOG.info("original cost = " + originSolution.cost);
        if (id != -1) {
            LOG.info("use rewrited hop, multicse id = " + id);
            if (bestSolution.multiCse != null) {
                LOG.info(bestSolution.multiCse);
            }
            // deep copy 是为了清除无用的父节点,返回一个干净的hop
            bestSolution.body = deepCopyHopsDag(bestSolution.body);
            rewriteCommonSubexpressionElimination.rewriteHopDAG(bestSolution.body, new ProgramRewriteStatus());
        } else {
            LOG.info("use original hop");
        }
        return bestSolution;
    }

//    private Hop genAndSelectHop(ArrayList<SingleCse> singleCses, ArrayList<MultiCse> multiCses, Hop template, Hop originalRoot) {
//        // 构造出所有的Hop
//
//        long constructHopTime = 0;
//        long estimateCostTime = 0;
//        long start, end;
//
//        MultiCse multiCse = null;
//        Hop result = null;
//        int id = -1;
//        double cost = Double.MAX_VALUE;  //estimate(originalRoot);
//
//        for (int i = 0; i < multiCses.size(); i++) {
//            try {
//                start = System.nanoTime();
//                MultiCse c = multiCses.get(i);
//                Hop hop = createHop(c, template);
//                end = System.nanoTime();
//                constructHopTime += end - start;
//
//                if (hop == null) continue;
//
//                start = System.nanoTime();
//                double newCost = 0;
//                //   if (multiCses.size()==4643 && i == 4552) {
//                newCost = estimate(hop);
//                //        System.out.println("x");
//                //       }
//                end = System.nanoTime();
//                estimateCostTime += end - start;
//
//                if (showCost)
//                    LOG.debug("cost=" + newCost);
//
//                if (cost > newCost || result == null) {
//                    result = hop;
//                    cost = newCost;
//                    multiCse = c;
//                    id = i;
//                }
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        double newCost = estimate(originalRoot);
//        if (cost >= newCost || result == null) {
//            result = originalRoot;
//            cost = newCost;
//            multiCse = null;
//            id = -1;
//        }
//
//        LOG.info("construct hop time =" + (constructHopTime / 1e6) + "ms");
//        LOG.info("estimate cost time =" + (estimateCostTime / 1e6) + "ms");
//        LOG.info("minium cost = " + cost);
//        LOG.info("original cost = " + newCost);
//        if (id != -1) {
//            LOG.info("use rewrited hop, multicse id = " + id);
//            if (multiCse != null) {
//                LOG.info(multiCse);
//            }
//            // deep copy 是为了清除无用的父节点,返回一个干净的hop
//            result = deepCopyHopsDag(result);
//            rewriteCommonSubexpressionElimination.rewriteHopDAG(result, new ProgramRewriteStatus());
//        } else {
//            LOG.info("use original hop");
//        }
//
//        return result;
//    }


    private double estimate(MySolution solution, boolean showDetails) {
        // 代价估计
        double cost = 0;
        if (showDetails) {
            LOG.debug("runtime program<<<");
            FakeCostEstimator2.MMShowCostFlag = true;
        }
        try {
            if (ec != null) {
                FakeCostEstimator2.ec = ec;
                double preCost = 0;
                for (Hop h : solution.preLoopConstants) {
                    Program program = constructProgramBlocks(h);
                    if (showDetails)
                        LOG.debug(Explain.explain(program));
                    preCost += FakeCostEstimator2.estimate(program);
                }
                Program programBlocks = constructProgramBlocks(solution.body);
                if (showDetails)
                    LOG.debug(Explain.explain(programBlocks));
                //cost = CostEstimationWrapper.getTimeEstimate(programBlocks, ec);
                double bodyCost = FakeCostEstimator2.estimate(programBlocks);
                if (showDetails)
                    LOG.debug("preCOst=" + preCost + " bodyCost=" + bodyCost);
                cost = preCost + bodyCost * epoch;
            }
        } catch (Exception e) {
            //  e.printStackTrace();
            cost = Double.MAX_VALUE;
        }
        if (showDetails) {
            LOG.debug("runtime program>>>");
            FakeCostEstimator2.MMShowCostFlag = false;
        }
        FakeCostEstimator2.cleanUnusedMMNode();
        return cost;
    }

//    private void showRtprog(MySolution solution) {
//        LOG.debug("runtime program<<<");
//        for (Hop h : solution.preLoopConstants) {
//            Program program = constructProgramBlocks(h);
//           LOG.debug(Explain.explain(program));
//        }
//        Program programBlocks = constructProgramBlocks(solution.body);
//        LOG.debug(Explain.explain(programBlocks));
//        LOG.debug(">>runtime program");
//    }

    private boolean isTranspose(int l, int r) {
        long first = rangeHash1(l, r);
        long second = rangeHash2(l, r);
        boolean transpose = false;
        if (first < second) {
            transpose = true;
        }
        return transpose;
    }

    private ArrayList<Pair<SingleCse, Hop>> genHopFromSingleCses(ArrayList<SingleCse> singleCses, Hop template) {
        HashMap<Pair<Long, Long>, Pair<SingleCse, Hop>> filter = new HashMap<>();
        for (SingleCse sc : singleCses) {
            Hop h = createHop(sc, template);
            Pair<Long, Long> hash = Hash.hashHopDag(h);
            if ((!filter.containsKey(hash)) ||
                    (filter.containsKey(hash) &&
                            (filter.get(hash).getLeft().ranges.size() < sc.ranges.size()))) {
                Hop copy = deepCopyHopsDag(h);
                rewriteCommonSubexpressionElimination.rewriteHopDAG(copy, new ProgramRewriteStatus());
                filter.put(hash, Pair.of(sc, copy));
            }
        }
        ArrayList<Pair<SingleCse, Hop>> list = new ArrayList<>();
        list.addAll(filter.values());
        return list;
    }

    private Hop createHop(SingleCse sc, Hop template) {
        try {
            ArrayList<RangeTree> list = new ArrayList<>();
            sc.prototype = null;
            sc.protoRange = null;
            for (Range range : sc.ranges) {
                list.add(new RangeTree(range.left, range.right, sc, range.transpose));
            }
            return createHopInner(list, template);
        } catch (Exception e) {
            return null;
        }
    }

    private Hop createHop(MultiCse multiCse, Hop template) {
        try {
            ArrayList<RangeTree> list = new ArrayList<>();
            for (SingleCse sc : multiCse.cses) {
                sc.prototype = null;
                sc.protoRange = null;
                for (Range range : sc.ranges) {
                    list.add(new RangeTree(range.left, range.right, sc, range.transpose));
                }
            }
            return createHopInner(list, template);
        } catch (Exception e) {
            return null;
        }
    }

    private Hop createHopInner(ArrayList<RangeTree> list, Hop template) {
        try {
            Hop copy = deepCopyHopsDag(template);
            // 准备区间数组
            for (int i = 0; i < leaves.size(); i++) {
                SingleCse sc = new SingleCse(Range.of(i, i, false), leaves.get(i).hop);
                sc.name = getRangeName(i, i);
                RangeTree rangeTree = new RangeTree(i, i, sc, false);
                list.add(rangeTree);
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
//                LOG.debug("inner created hop:");
//                LOG.debug(Explain.explain(copy));
                return copy;
            }
        } catch (Exception e) {
            LOG.warn("construct hop error");
            return null;
        }
        return null;
    }

    private Hop rCreateHop(RangeTree rt) {
        if (rt.left == rt.right) {
            return leaves.get(rt.left).hop;
        }
        if (rt.singleCse.prototype != null) {
            rt.singleCse.prototype.shouldPersist = true;
            System.out.println("Set hop " + rt.singleCse.prototype.getHopID() + " should persist");
            if (rt.transpose == rt.singleCse.protoRange.transpose) {
                return rt.singleCse.prototype;
            } else {
                return HopRewriteUtils.createTranspose(rt.singleCse.prototype);
            }
        }
        {
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

    public RewriteCoordinate(ExecutionContext executionContext, StatementBlock statementBlock) {
        ec = executionContext;
        this.statementBlock = statementBlock;
        FakeCostEstimator2.ec = executionContext;
        DistributedScratch.ec = executionContext;
        onlySearchConstantSubExp = false;
    }

    public RewriteCoordinate(ExecutionContext executionContext, StatementBlock statementBlock, VariableSet variablesUpdated) {
        ec = executionContext;
        this.statementBlock = statementBlock;
        this.variablesUpdated = variablesUpdated;
        FakeCostEstimator2.ec = executionContext;
        DistributedScratch.ec = executionContext;
        onlySearchConstantSubExp = true;
        constantUtil = new ConstantUtil(variablesUpdated);
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
                    MySolution solution = rewiteHopDag(sb.getHops().get(i));
                    Hop g = solution.body;
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
            System.out.println("Construct Program Blocks Error");
            hop.resetVisitStatusForced(new HashSet<>());
            System.out.println(MyExplain.myExplain(hop));
            System.out.println("y");
        }
        Program rtprog = dmlt.getRuntimeProgram(prog, ConfigurationManager.getDMLConfig());
        return rtprog;
    }


}
