package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.*;
import org.apache.sysds.hops.rewrite.dfp.AnalyzeSymmetryMatrix;
import org.apache.sysds.hops.rewrite.dfp.DisjointSet;
import org.apache.sysds.hops.rewrite.dfp.Leaf;
import org.apache.sysds.hops.rewrite.dfp.MySolution;
import org.apache.sysds.hops.rewrite.dfp.costmodel.CostModelCommon;
import org.apache.sysds.hops.rewrite.dfp.costmodel.DistributedScratch;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.hops.rewrite.dfp.dp.CostGraph;
import org.apache.sysds.hops.rewrite.dfp.dp.NodeCost;
import org.apache.sysds.hops.rewrite.dfp.dp.OperatorNode;
import org.apache.sysds.hops.rewrite.dfp.dp.SinglePlan;
import org.apache.sysds.hops.rewrite.dfp.utils.*;
import org.apache.sysds.parser.*;
import org.apache.sysds.runtime.controlprogram.Program;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.utils.Explain;

import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.CreateRuntimeProgram.constructProgramBlocks;
import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.isLeafMatrix;
import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class RewriteCoordinate extends StatementBlockRewriteRule {

    protected final Log LOG = LogFactory.getLog(RewriteCoordinate.class.getName());

    public static String manualType = null;

    public ArrayList<Leaf> leaves = new ArrayList<>();
    public HashMap<Long, Integer> hopId2LeafIndex = new HashMap<>();

    private ExecutionContext ec;
    public StatementBlock statementBlock;

    private RewriteMatrixMultChainOptimization rewriteMatrixMultChainOptimization = new RewriteMatrixMultChainOptimization();
    private RewriteCommonSubexpressionElimination rewriteCommonSubexpressionElimination = new RewriteCommonSubexpressionElimination();
    //  private  final Log LOG = LogFactory.getLog(RewriteCoordinate.class.getName());

    // public boolean onlySearchConstantSubExp = false;
    public VariableSet variablesUpdated = null;
    //    public ConstantUtil constantUtil = null; // new ConstantUtil(variablesUpdated);
    public ConstantUtilByTag constantUtilByTag;
    public long iterationNumber = 2;

    // <configuration>
    private boolean showBlock = false;
    private boolean showSingleCse = false;
    private boolean showMultiCse = false;
    private boolean showCost = false;
    private boolean showMinCostHop = true;
    private boolean showOriginHop = true;

    // private int hMultiCseId = 6392;

    private int maxMultiCseNumber = -1; // 设为-1,则生成所有的；设为正数，则最多生成那么多个

    // private static long epoch = 100;

    private boolean useManualPolicy = false;
    private boolean useDynamicProgramPolicy = false;
    private boolean useBruceForcePolicy = true;


    // </configuration>

    public static long allGenerateOptionsTime = 0;
    public static long allGenerateCombinationsTime = 0;
//    public static long estimateTime = 0;

    public MySolution rewiteHopDag(Hop root) {
        MySolution originalSolution = new MySolution(root);

        try {
            LOG.debug("start coordinate " + MyExplain.myExplain(root));
            LOG.debug("root: \n" + Explain.explain(root));

            Triple<Hop, ArrayList<Range>, ArrayList<SingleCse>> triple = generateOptions(root);
            if (triple == null) {
                LOG.debug("small leaves size, return original solution");
                return originalSolution;
            }

            Hop template = triple.getLeft();
            ArrayList<Range> blockRanges = triple.getMiddle();
            ArrayList<SingleCse> singleCses = triple.getRight();

            long start3 = System.nanoTime();
            MySolution mySolution = null;
            try {
                if (useManualPolicy) {
                    mySolution = testManualAll(root, template, blockRanges);
                } else if (useDynamicProgramPolicy) {
                    mySolution = testDynamicProgramming(singleCses, template, blockRanges);
                } else if (useBruceForcePolicy) {
                    mySolution = testBruteForce(singleCses, template, blockRanges);
                }
//                testCreateHops(template, blockRanges);
            } catch (Exception e) {
                e.printStackTrace();
                mySolution = null;
            }
            long end3 = System.nanoTime();
            allGenerateCombinationsTime += end3 - start3;
            if (mySolution != null && (mySolution.cost < originalSolution.cost || useManualPolicy)) {
                LOG.info("return rewrited solution");
                System.out.println("return rewrited solution");
                return mySolution;
            } else {
                LOG.info("return original solution");
                System.out.println("return original solution");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        LOG.debug("final return original hop");
        return originalSolution;
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

        hopId2LeafIndex = new HashMap<>();
        leaves = new ArrayList<>();

        // 找到所有的叶子节点
        findAllLeaf(template, new ArrayList<>(), 0, hopId2LeafIndex, djs);
        if (leaves.size() < 4) {
            long end2 = System.nanoTime();
            allGenerateOptionsTime += end2 - start2;
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
        ArrayList<Range> blockRanges = new ArrayList<>();
//        ArrayList<SingleCse> singleCses = genSingleCseSameBlockAttention(djs, blockRanges);
        ArrayList<SingleCse> singleCses = genSingleCse(djs, blockRanges);

        long end2 = System.nanoTime();
        allGenerateOptionsTime += end2 - start2;

        return Triple.of(template, blockRanges, singleCses);
    }

    MySolution testManualAll(Hop root, Hop template, ArrayList<Range> blockRanges) {
        MySolution mySolution = null;
        ManualSolution manualSolution = new ManualSolution(this);
        if (manualType == null) {
            System.exit(-1);
        } else if ("g".equals(root.getName())) {
            if (manualType.equals("dfp-ata")
                    || manualType.equals("dfp-ata-dtd")
                    || manualType.equals("bfgs-ata")
                    || manualType.equals("bfgs-ata-dtd")
            ) {
                MultiCse multiCse = manualSolution.createMultiCseGAtA();
                mySolution = genManualSolution(template, blockRanges, multiCse, true);
                LOG.info("return g-ata ");
            }
        } else if (manualType.equals("dfp") && "h".equals(root.getName())) {
            MultiCse multiCse = manualSolution.createMultiCseDfp();
            mySolution = genManualSolution(template, blockRanges, multiCse, false);
            LOG.info("return dfp");
        } else if (manualType.equals("dfp-ata") && "h".equals(root.getName())) {
            MultiCse multiCse = manualSolution.createMultiCseDfpAta();
            mySolution = genManualSolution(template, blockRanges, multiCse, true);
            LOG.info("return dfp-ata h");
        } else if (manualType.equals("dfp-ata-dtd") && "h".equals(root.getName())) {
            MultiCse multiCse = manualSolution.createMultiCseDfpAtaDtd();
            mySolution = genManualSolution(template, blockRanges, multiCse, true);
            LOG.info("return dfp-ata h");
        } else if (manualType.equals("bfgs") && "h".equals(root.getName())) {
            MultiCse multiCse = manualSolution.createMultiCseBfgs();
            mySolution = genManualSolution(template, blockRanges, multiCse, false);
            LOG.info("return bfgs");
        } else if (manualType.equals("bfgs-ata") && "h".equals(root.getName())) {
            MultiCse multiCse = manualSolution.createMultiCseBfgsAta();
            mySolution = genManualSolution(template, blockRanges, multiCse, true);
            LOG.info("return bfgs-ata");
        } else if (manualType.equals("bfgs-ata-dtd") && "h".equals(root.getName())) {
            MultiCse multiCse = manualSolution.createMultiCseBfgsAtaDtd();
            mySolution = genManualSolution(template, blockRanges, multiCse, true);
            LOG.info("return bfgs-ata");
        } else if (manualType.equals("gd-ata")) {
            if (leaves.size() == 6) {
                MultiCse multiCse = manualSolution.createMultiCseGdAtaTheta();
                mySolution = genManualSolution(template, blockRanges, multiCse, true);
                LOG.info("return gd-ata theta");
            }
        } else if (manualType.equals("gd-atb")) {
            VariableSet aSet = new VariableSet();
            aSet.addVariable("theta", null);
            if (leaves.size() == 6) {
                MultiCse multiCse = manualSolution.createMultiCseGdAtbTheta();
                mySolution = genManualSolution(template, blockRanges, multiCse, true);
                LOG.info("return gd-atb theta");
            }
        } else if (manualType.equals("dfp-spores-ata") && leaves.size() == 11) {
            MultiCse multiCse = manualSolution.createMultiCseDfpSporseAta();
            VariableSet aSet = new VariableSet();
            aSet.addVariable("h", null);
            aSet.addVariable("g", null);
            aSet.addVariable("d", null);
            aSet.addVariable("i", null);
            mySolution = genManualSolution(template, blockRanges, multiCse, true);
            LOG.info("return dfp-spores-ata");
        }
        return mySolution;
    }

    private MySolution genManualSolution(Hop template, ArrayList<Range> blockRanges, MultiCse multiCse, boolean liftConstant) {
        LOG.debug(multiCse);
        for (int i = 0; i < multiCse.cses.size(); i++) {
            SingleCse si = multiCse.cses.get(i);
            for (int j = i + 1; j < multiCse.cses.size(); j++) {
                SingleCse sj = multiCse.cses.get(j);
                if (si.conflict(sj)) {
                    LOG.error("cse conflict " + si + " " + sj);
                    System.exit(-1);
                }
            }
        }
        Hop result = createHop(multiCse, template, blockRanges);
        result = deepCopyHopsDag(result);
        rewriteCommonSubexpressionElimination.rewriteHopDAG(result, new ProgramRewriteStatus());

        Program program = constructProgramBlocks(result, statementBlock);
        LOG.info("before lift constant");
        LOG.info(Explain.explain(program));

        CostGraph costGraph = new CostGraph(variablesUpdated, iterationNumber, ec);
        CostModelCommon.MMShowCostFlag = true;
        Triple<NodeCost, NodeCost, OperatorNode> costTriple = costGraph.estimateHopCost(result);
        LOG.info("all cost detail=" + costTriple.getLeft());
        LOG.info("constant cost detail=" + costTriple.getMiddle());
        LOG.info(CostGraph.explainOpNode(costTriple.getRight(), 0));

        MySolution solution;
        if (liftConstant) solution = constantUtilByTag.liftLoopConstant(result);
        else solution = new MySolution(result);
        solution.multiCse = multiCse;
        for (Hop h : solution.preLoopConstants) {
            h.resetVisitStatusForced(new HashSet<>());
        }
        solution.body.resetVisitStatusForced(new HashSet<>());
        solution.cost = costTriple.getLeft().getSummary();
        LOG.info("manual solution:\n" + solution);
        return solution;
    }

    MySolution testBruteForce(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges) {
        // 构造出所有的MultiCse

        ArrayList<MultiCse> multiCses = genMultiCse(singleCses);

        if (multiCses.size() == 0) return null;
        long begin = System.nanoTime();

        CostGraph costGraph = new CostGraph(variablesUpdated, iterationNumber, ec);
        Hop result = null;
        double minCost = Double.MAX_VALUE;
        for (int i = 0; i < multiCses.size(); i++) {
            MultiCse multiCse = multiCses.get(i);
            Hop hop = createHop(multiCse, template, blockRanges);
            Triple<NodeCost, NodeCost, OperatorNode> costTriple = costGraph.estimateHopCost(hop);
            if (i % 1000 == 0) {
                LOG.info("i=" + i + ", costdetail=" + costTriple.getLeft());
            }
            if (result == null || minCost > costTriple.getLeft().getSummary()) {
                result = hop;
                minCost = costTriple.getLeft().getSummary();
            }
        }
        long end = System.nanoTime();
        LOG.info("brute force cost time = " + ((end - begin) / 1e9) + "s");
        costGraph.nodeCostEstimator.printCache();
        MySolution mySolution = constantUtilByTag.liftLoopConstant(result);
        return mySolution;
    }

    MySolution testDynamicProgramming(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges) {
        try {
            ArrayList<Hop> hops = createHops(template, blockRanges);
            SingleCse emptyCse = new SingleCse();
            emptyCse.hash = HashKey.of(0L, 0L);
            Hop emptyHop = createHop(emptyCse, template, blockRanges);
            Pair<SingleCse, Hop> emptyPair = Pair.of(emptyCse, emptyHop);
            ArrayList<Pair<SingleCse, Hop>> list = genHopFromSingleCses(singleCses, template, blockRanges);
            list.add(emptyPair);

            ArrayList<SinglePlan> singlePlans = new ArrayList<>();
            for (Pair<SingleCse, Hop> p : list) {
                SinglePlan singlePlan = new SinglePlan();
                singlePlan.hop = p.getRight();
                singlePlan.singleCse = p.getLeft();
                singlePlans.add(singlePlan);
            }
            CostGraph costGraph = new CostGraph(variablesUpdated, iterationNumber, ec);

            MySolution mySolution = null;
            ArrayList<OperatorNode> operatorNodeArrayList = costGraph.testOperatorGraph(singlePlans, emptyPair, blockRanges, leaves, hops);

//            costGraph.nodeCostEstimator.range2mmnode.forEach((key, value) -> {
//                System.out.println(key + " -> " + value);
//            });
//            costGraph.nodeCostEstimator.drange2multiplycost.forEach((key, value) -> {
//                System.out.println(key + " -> " + value);
//            });

            long start = System.nanoTime();
            //ArrayList<MultiCse> multiCseArrayList = new ArrayList<>();
            int bestId = -1;
//            for (int i = 0; i < 200 && i < operatorNodeArrayList.size(); i++) {
                int i=0;
                OperatorNode operatorNode = operatorNodeArrayList.get(i);
                MultiCse multiCse = createMultiCseFromOperatorNode(operatorNode);
                if (multiCse != null) {
                    //      multiCseArrayList.add(multiCse);
                    Hop hop = createHop(multiCse, template, blockRanges);
                    hop = copyAndEliminateHop(hop);
                    double dpcost = operatorNode.accCost;
                    Triple<NodeCost, NodeCost, OperatorNode> costTriple = costGraph.estimateHopCost(hop);
                    NodeCost cost3 = costTriple.getLeft();
                    double hcost = cost3.getSummary();
                    double rate = (dpcost - hcost) / hcost;
                    LOG.info("candidate multi cse:  rcost=" + hcost + ", dpcost=" + dpcost + ", rate=" + rate + "\n" + operatorNode.accCostDetails + "\n" + multiCse);
                    LOG.info("rcostdetail=" + cost3 + ", dpcostdetail=" + operatorNode.accCostDetails);
                    LOG.info(CostGraph.explainOpNode(operatorNode, 0));
                    MySolution solution = constantUtilByTag.liftLoopConstant(hop);
                    solution.multiCse = multiCse;
                    if (mySolution == null || mySolution.cost < dpcost) {
                        mySolution = solution;
                        bestId = i;
                    }
                }
//            }
            costGraph.nodeCostEstimator.printCache();
            LOG.info("bestId=" + bestId);
            long end = System.nanoTime();
            LOG.info("dynamic programming: ");
            LOG.info(mySolution);
            System.out.println("dynamic programming: ");
            System.out.println(mySolution);

//            if (mySolution.body.getName().equals("h")) System.exit(-1);
            return mySolution;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private MultiCse createMultiCseFromOperatorNode(OperatorNode node) {
        if (node == null) return null;
        MultiCse multiCse = new MultiCse();
        multiCse.cses = new ArrayList<>(node.dependencies);
        multiCse.cses.addAll(node.oldDependencies);
        return multiCse;
    }

    private int findAllLeaf(Hop hop, ArrayList<Integer> path, int depth, HashMap<Long, Integer> hopId2LeafIndex, DisjointSet djs) {
        //  System.out.println("findAllLeaf visit: " + hop.getHopID() + " " + hop.getName());
        if (isLeafMatrix(hop)
                || (HopRewriteUtils.isTransposeOperation(hop) && isLeafMatrix(hop.getInput().get(0)))
            // || hop.getParent().size() > 1)
        ) {
            int index = leaves.size();
            hopId2LeafIndex.put(hop.getHopID(), index);
            Leaf leaf = new Leaf(hop, path, depth);
            leaves.add(leaf);
            return index;
        } else {
            if (HopRewriteUtils.isMatrixMultiply(hop)) {
                if (path.size() <= depth) path.add(0);
                else path.set(depth, 0);
                int l = findAllLeaf(hop.getInput().get(0), path, depth + 1, hopId2LeafIndex, djs);
                path.set(depth, 1);
                int r = findAllLeaf(hop.getInput().get(1), path, depth + 1, hopId2LeafIndex, djs);
                if (l >= 0 && r >= 0) {
                    djs.merge(l, r);
                }
                return r;
            } else {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    if (path.size() <= depth) path.add(i);
                    else path.set(depth, i);
                    findAllLeaf(hop.getInput().get(i), path, depth + 1, hopId2LeafIndex, djs);
                }
                return -1;
            }
        }
    }

    private void genBlocksSameBlockAttention(DisjointSet djs, ArrayList<Range> blockRanges, HashMap<HashKey, ArrayList<HashSet<Range>>> hash2Rangesset) {
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
        if (ranges.size() > 0) {
            result.subList(0, ranges.size()).clear();
        }
        return result;
    }

    private ArrayList<SingleCse> genSingleCseSameBlockAttention(DisjointSet djs, ArrayList<Range> blockRanges) {
        HashMap<HashKey, ArrayList<HashSet<Range>>> hash2Ranges = new HashMap<>();
        // 划分 块
        genBlocksSameBlockAttention(djs, blockRanges, hash2Ranges);
        hash2Ranges.forEach((x,y)->{
            for (HashSet<Range> hs: y) {
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
                if (i % 1000 == 0) {
                    LOG.info("i: " + i + ", multicse number: " + result.size());
                }
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


    private MySolution genSolution(MultiCse multiCse, Hop template, ArrayList<Range> blockRanges) {
        try {
            Hop hop = createHop(multiCse, template, blockRanges);
            hop = copyAndEliminateHop(hop);
            MySolution solution = new MySolution(multiCse, hop);
            return solution;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private Hop copyAndEliminateHop(Hop hop) {
        // deep copy 是为了清除无用的父节点,返回一个干净的hop
        hop = deepCopyHopsDag(hop);
        rewriteCommonSubexpressionElimination.rewriteHopDAG(hop, new ProgramRewriteStatus());
        return hop;
    }


    private ArrayList<MySolution> genSolutions(ArrayList<MultiCse> multiCses, boolean liftConstant,
                                               Hop template, ArrayList<Range> blockRanges) {
        long constructHopTime = 0;
        long start = System.nanoTime();
        ArrayList<MySolution> solutions = new ArrayList<>();
        HashMap<Pair<Long, Long>, ArrayList<Hop>> filter = new HashMap<>();
//        LOG.debug("temlate:");
//        LOG.debug(Explain.explain(template));
        for (int i = 0; i < multiCses.size(); i++) {
            MultiCse c;
            Hop hop;
            try {
                c = multiCses.get(i);
//                if (i == 5) {
//                    System.out.println("x");
//                }
                hop = createHop(c, template, blockRanges);
//                LOG.debug("hop:");
//                MyExplain.explain_iter2(hop);
//                LOG.debug(Explain.explain(hop));
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
                    Hop copy = deepCopyHopsDag(hop);
                    MySolution solution = constantUtilByTag.liftLoopConstant(copy);
                    solution.multiCse = c;
                    solutions.add(solution);
                }
            } catch (Exception e) {
//                System.out.println(multiCses.get(i));
                e.printStackTrace();
            }
        }
        long end = System.nanoTime();
        constructHopTime += end - start;
        LOG.info("solution size = " + solutions.size());
        LOG.info("construct hop time =" + (constructHopTime / 1e6) + "ms");
        return solutions;
    }

    @Deprecated
    private MySolution selectSolution(ArrayList<MySolution> solutions) {
        long estimateCostTime = 0;
        long start, end;
        int id = -1;
        start = System.nanoTime();
        MySolution bestSolution = new MySolution();

//        FakeCostEstimator2.miniumCostBoundery = Double.MAX_VALUE;
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
            }
        }
        LOG.info("minium cost = " + bestSolution.cost);
        if (bestSolution.body != null) {
//            FakeCostEstimator2.miniumCostBoundery = Double.MAX_VALUE;
            bestSolution.cost = estimate(bestSolution, true);
        } else {
            return null;
        }
        end = System.nanoTime();
        estimateCostTime += end - start;
        LOG.info("estimate cost time =" + (estimateCostTime / 1e6) + "ms");
        LOG.info("minium cost multicse id = " + id);
        if (bestSolution.multiCse != null) {
            LOG.info(bestSolution.multiCse);
        }
        bestSolution.body = copyAndEliminateHop(bestSolution.body);

        return bestSolution;
    }


    @Deprecated
    private double estimate(MySolution solution, boolean showDetails) {
        // 代价估计
        double cost = 0;
        if (showDetails) {
            LOG.debug("runtime program<<<");
            CostModelCommon.MMShowCostFlag = true;
        }
        try {
            FakeCostEstimator2.ec = ec;
            double preCost = 0;
            double bodyCost = 0;
            double preloopShuffleCost = 0;
            double preloopBroadcastCost = 0;
            double preloopComputeCost = 0;
            double preloopCollectCost = 0;
            double bodyShuffleCost = 0;
            double bodyBroadcastCost = 0;
            double bodyComputeCost = 0;
            double bodyCollectCost = 0;

            for (Hop h : solution.preLoopConstants) {
                Program program = constructProgramBlocks(h, statementBlock);
                if (showDetails)
                    LOG.debug(Explain.explain(program));
                preCost += FakeCostEstimator2.estimateRuntimeProgram(program);
                preloopShuffleCost += FakeCostEstimator2.shuffleCostSummary;
                preloopBroadcastCost += FakeCostEstimator2.broadcastCostSummary;
                preloopComputeCost += FakeCostEstimator2.computeCostSummary;
                preloopCollectCost += FakeCostEstimator2.collectCostSummary;
//                if (preCost > FakeCostEstimator2.miniumCostBoundery) break;
            }
            Program programBlocks = constructProgramBlocks(solution.body, statementBlock);
            if (showDetails)
                LOG.debug(Explain.explain(programBlocks));
//            cost = CostEstimationWrapper.getTimeEstimate(programBlocks, ec);
//            if (preCost <= FakeCostEstimator2.miniumCostBoundery) {
            bodyCost = FakeCostEstimator2.estimateRuntimeProgram(programBlocks);
            bodyShuffleCost += FakeCostEstimator2.shuffleCostSummary;
            bodyBroadcastCost += FakeCostEstimator2.broadcastCostSummary;
            bodyComputeCost += FakeCostEstimator2.computeCostSummary;
            bodyCollectCost += FakeCostEstimator2.collectCostSummary;
//            }
            cost = preCost + bodyCost * iterationNumber;
            solution.cost = cost;
            solution.preCost = preCost;
            solution.bodyCost = bodyCost;
            solution.preloopShuffleCost = preloopShuffleCost;
            solution.preloopBroadcastCost = preloopBroadcastCost;
            solution.preloopComputeCost = preloopComputeCost;
            solution.preloopCollectCost = preloopCollectCost;
            solution.bodyShuffleCost = bodyShuffleCost;
            solution.bodyBroadcastCost = bodyBroadcastCost;
            solution.bodyComputeCost = bodyComputeCost;
            solution.bodyCollectCost = bodyCollectCost;

//                FakeCostEstimator2.miniumCostBoundery = Math.min(FakeCostEstimator2.miniumCostBoundery,cost);
            if (showDetails) {
//                LOG.debug("preCOst=" + preCost + " bodyCost=" + bodyCost + " allcost=" + cost +   " cse=" + solution.multiCse);
                LOG.debug("multicse=" + solution.multiCse);
                LOG.debug("allCost=" + cost);
                LOG.debug("preCost=" + preCost);
                LOG.debug("bodyCost=" + bodyCost);
                LOG.debug("allshuffleCost=" + (preloopShuffleCost + bodyShuffleCost * iterationNumber));
                LOG.debug("allBroadcastCost=" + (preloopBroadcastCost + bodyBroadcastCost * iterationNumber));
                LOG.debug("allComputeCost=" + (preloopComputeCost + bodyComputeCost * iterationNumber));
                LOG.debug("allCollectCost=" + (preloopCollectCost + bodyCollectCost * iterationNumber));
                LOG.debug("preloopShuffleCost=" + preloopShuffleCost);
                LOG.debug("preloopBroadcastCost=" + preloopBroadcastCost);
                LOG.debug("preloopComputeCost=" + preloopComputeCost);
                LOG.debug("preloopCollectCost=" + preloopCollectCost);
                LOG.debug("bodyShuffleCost=" + bodyShuffleCost);
                LOG.debug("bodyBroadcastCost=" + bodyBroadcastCost);
                LOG.debug("bodyComputeCost=" + bodyComputeCost);
                LOG.debug("bodyCollectCost=" + bodyCollectCost);
            }

        } catch (Exception e) {
            //  e.printStackTrace();
            cost = Double.MAX_VALUE;
        }
        if (showDetails) {
            LOG.debug("runtime program>>>");
            CostModelCommon.MMShowCostFlag = false;
        }
        //  FakeCostEstimator2.cleanUnusedMMNode();
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

    private ArrayList<Pair<SingleCse, Hop>> genHopFromSingleCses(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges) {
        HashMap<Pair<Long, Long>, Pair<SingleCse, Hop>> filter = new HashMap<>();
        for (SingleCse sc : singleCses) {
            Hop h = createHop(sc, template, blockRanges);
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

    private Hop createHop(SingleCse sc, Hop template, ArrayList<Range> blockRanges) {
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

    private Hop createHop(MultiCse multiCse, Hop template, ArrayList<Range> blockRanges) {
        try {
            ArrayList<RangeTree> list = new ArrayList<>();
            for (SingleCse sc : multiCse.cses) {
                sc.prototype = null;
                sc.protoRange = null;
                for (Range range : sc.ranges) {
                    list.add(new RangeTree(range.left, range.right, sc, range.transpose));
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
            for (int i = 0; i < leaves.size(); i++) {
                boolean ok = true;
                for (RangeTree rangeTree : list) {
                    if (rangeTree.left == i && rangeTree.right == i) {
                        ok = false;
                        break;
                    }
                }
                if (!ok) continue;
                SingleCse sc = new SingleCse(Range.of(i, i, false), leaves.get(i).hop);
                sc.name = getRangeName(i, i);
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
            return null;
        }
        return null;
    }

    private Hop rCreateHop(RangeTree rt) {
        if (rt.left == rt.right) {
            return rt.singleCse.prototype;
//            return leaves.get(rt.left).hop;
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

    private Hop build_binary_tree_naive(ArrayList<Hop> mmChain) {
        assert mmChain.size() > 0;
        if (mmChain.size() == 1) return mmChain.get(0);
        Hop ret = mmChain.get(mmChain.size() - 1);
        for (int i = mmChain.size() - 2; i >= 0; i--) {
            ret = HopRewriteUtils.createMatrixMultiply(mmChain.get(i), ret);
        }
        return ret;
    }

    void testCreateHops(Hop template, ArrayList<Range> blockRanges) {
        ArrayList<Hop> hops = createHops(template, blockRanges);
        if (hops != null) {
            HashSet<Pair<Long, Long>> ss = new HashSet<>();
            for (Hop hop : hops) {
//                    System.out.println(Explain.explain(hop));
                Pair<Long, Long> key = Hash.hashHopDag(hop);
                ss.add(key);
            }
            LOG.info("all trees size=" + hops.size());
            LOG.info("hash set size=" + ss.size());

        } else {
            System.exit(-100);
        }
    }

    private ArrayList<Hop> createHops(Hop template, ArrayList<Range> blockRanges) {
        try {
            ArrayList<RangeTree> list = new ArrayList<>();
            return createHopInner_s(list, template, blockRanges);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private ArrayList<Hop> createHopInner_s(ArrayList<RangeTree> list, Hop template, ArrayList<Range> blockRanges) {
        try {
            // 准备区间数组
            for (int i = 0; i < leaves.size(); i++) {
                boolean ok = true;
                for (RangeTree rangeTree : list) {
                    if (rangeTree.left == i && rangeTree.right == i) {
                        ok = false;
                        break;
                    }
                }
                if (!ok) continue;
                SingleCse sc = new SingleCse(Range.of(i, i, false), leaves.get(i).hop);
                sc.name = getRangeName(i, i);
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
            ArrayList<Hop> ret = new ArrayList<>();

            for (RangeTree rt : stack) {
                try {
                    ArrayList<Hop> block_hops = rCreateHop_s(rt);
                    assert block_hops != null;
                    for (Hop block_hop : block_hops) {
                        // 找到这个块的位置
                        Hop copy = deepCopyHopsDag(template);
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
                        } else {
                            copy = block_hop;
                        }
                        ret.add(copy);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return ret;
        } catch (Exception e) {
            LOG.warn("construct hops error");
            return null;
        }
    }


    private ArrayList<Hop> rCreateHop_s(RangeTree rt) {
        ArrayList<Hop> children = new ArrayList<>();
        for (RangeTree son : rt.children) {
            Hop s = rCreateHop(son);
            if (s == null) return null;
            children.add(s);
        }
        ArrayList<Pair<HashSet<Triple<Integer, Integer, Integer>>, Hop>> hops = build_binary_trees_all(children);
        ArrayList<Hop> ret = new ArrayList<>();
        HashSet<Triple<Integer, Integer, Integer>> set = new HashSet<>();
        for (Pair<HashSet<Triple<Integer, Integer, Integer>>, Hop> h : hops) {
            int before = set.size();
            set.addAll(h.getKey());
            int after = set.size();
            if (before != after) {
                ret.add(h.getValue());
            }
        }
        LOG.info("rCreateHop_s " + children.size() + " -> " + ret.size());
        return ret;
    }


    private ArrayList<Pair<HashSet<Triple<Integer, Integer, Integer>>, Hop>> build_binary_trees_all(ArrayList<Hop> mmChain) {
        if (mmChain.size() == 0) {
            return new ArrayList<>();
        }
        if (mmChain.size() == 1) {
            ArrayList<Pair<HashSet<Triple<Integer, Integer, Integer>>, Hop>> hops = new ArrayList<>();
            HashSet<Triple<Integer, Integer, Integer>> ss = new HashSet<>();
            ss.add(Triple.of(0, 0, 0));
            hops.add(Pair.of(ss, mmChain.get(0)));
            return hops;
        }
        // dp(i,i) = mmc(i)
        //  for l
        //    for i
        //        j = i + l-1
        //      for k
        //       dp(i,j) += dp(i,k)*dp(k+1,j)
        int n = mmChain.size();
        ArrayList<ArrayList<ArrayList<Pair<HashSet<Triple<Integer, Integer, Integer>>, Hop>>>> hops = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            hops.add(new ArrayList<>());
            for (int j = 0; j < n; j++) {
                hops.get(i).add(new ArrayList<>());
            }
        }
        for (int i = 0; i < n; i++) {
            HashSet<Triple<Integer, Integer, Integer>> ss = new HashSet<>();
            ss.add(Triple.of(i, i, i));
            hops.get(i).get(i).add(Pair.of(ss, mmChain.get(i)));
        }
        for (int l = 2; l <= n; l++) {
            for (int i = 0; i + l - 1 < n; i++) {
                int j = i + l - 1;
                ArrayList<Pair<HashSet<Triple<Integer, Integer, Integer>>, Hop>> tmp = new ArrayList<>();
                for (int k = i; k < j; k++) {
//                    ArrayList<Hop> a1 = hops.get(i).get(k);
//                    ArrayList<Hop> a2 = hops.get(k+1).get(j);
                    for (Pair<HashSet<Triple<Integer, Integer, Integer>>, Hop> h1 : hops.get(i).get(k)) {
                        for (Pair<HashSet<Triple<Integer, Integer, Integer>>, Hop> h2 : hops.get(k + 1).get(j)) {
                            Hop h = HopRewriteUtils.createMatrixMultiply(h1.getRight(), h2.getRight());
                            HashSet<Triple<Integer, Integer, Integer>> key = new HashSet<>();
                            key.addAll(h1.getKey());
                            key.addAll(h2.getKey());
                            key.add(Triple.of(i, k, j));
                            tmp.add(Pair.of(key, h));
                        }
                    }
                }
                hops.get(i).set(j, tmp);
            }
        }
        LOG.info("build_binary_trees_all " + mmChain.size() + " -> " + hops.get(0).get(n - 1).size());

        return hops.get(0).get(n - 1);
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

    public RewriteCoordinate(ExecutionContext executionContext, StatementBlock statementBlock) {
        ec = executionContext;
        this.statementBlock = statementBlock;
        FakeCostEstimator2.ec = executionContext;
        DistributedScratch.ec = executionContext;
        //   onlySearchConstantSubExp = false;
    }

    public RewriteCoordinate(ExecutionContext executionContext, StatementBlock statementBlock, VariableSet variablesUpdated) {
        ec = executionContext;
        this.statementBlock = statementBlock;
        this.variablesUpdated = variablesUpdated;
        FakeCostEstimator2.ec = executionContext;
        DistributedScratch.ec = executionContext;
        // onlySearchConstantSubExp = true;
        constantUtilByTag = new ConstantUtilByTag();
//        constantUtil = new ConstantUtil(variablesUpdated);
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


}
