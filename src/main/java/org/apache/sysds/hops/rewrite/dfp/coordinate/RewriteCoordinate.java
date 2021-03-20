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


    public ArrayList<Leaf> leaves = new ArrayList<>();
    public HashMap<Long, Integer> hopId2LeafIndex = new HashMap<>();

    private ExecutionContext ec;
    public StatementBlock statementBlock;

    private RewriteMatrixMultChainOptimization rewriteMatrixMultChainOptimization = new RewriteMatrixMultChainOptimization();
    private RewriteCommonSubexpressionElimination rewriteCommonSubexpressionElimination = new RewriteCommonSubexpressionElimination();
    //  private  final Log LOG = LogFactory.getLog(RewriteCoordinate.class.getName());

    // public boolean onlySearchConstantSubExp = false;
    public VariableSet variablesUpdated = null;
    public ConstantUtilByTag constantUtilByTag = null; // new ConstantUtil(variablesUpdated);
//    public ConstantUtil constantUtil = null;
    public long iterationNumber = 2;

    // <configuration>
    private boolean showBlock = false;
    private boolean showSingleCse = false;
    private boolean showMultiCse = false;
    private boolean showCost = false;
    private boolean showMinCostHop = true;
    private boolean showOriginHop = true;

    // private int hMultiCseId = 6392;

    private int maxMultiCseNumber = 100000; // 设为-1,则生成所有的；设为正数，则最多生成那么多个

    // private static long epoch = 100;

    private boolean useDirectPolicy = false;
    private boolean useDynamicProgramPolicy = true;
    private boolean useBruceForcePolicy = true;


    // </configuration>

    public static long allGenerateOptionsTime = 0;
    public static long allGenerateCombinationsTime = 0;

    public MySolution rewiteHopDag(Hop root) {
//        System.out.println("rootname"+root.getName());
//        System.out.println("xx");


        MySolution originalSolution = new MySolution(root);

        if (root.getName().equals("h")) {
            System.out.println(Explain.explain(root));
            CostGraph costGraph = new CostGraph(variablesUpdated,iterationNumber);
            costGraph.estimateHopCost(root);
          //  System.exit(0);
        } else {
          //  return originalSolution;
        }

        try {
            LOG.trace(ec.getVariables().keySet());
//            FakeCostEstimator2.miniumCostBoundery = Double.MAX_VALUE;
            long start2 = System.nanoTime();
            originalSolution.cost = estimate(originalSolution, true);
            long end2 = System.nanoTime();
            allGenerateCombinationsTime += end2 - start2;
            CostGraph.estimateTime += end2 - start2;
//            if (!"h".equals(root.getName())) {
//                return originalSolution;
//            }
            long start = System.nanoTime();
            LOG.info("original cost = " + originalSolution.cost);

            // 1. 深拷贝，格式化
            Hop template = deepCopyHopsDag(root);
            template = reorder(template);

            template.resetVisitStatusForced(new HashSet<>());
            LOG.debug("template: \n" + Explain.explain(template));

            LOG.debug("start coordinate " + MyExplain.myExplain(root));

            LOG.info("origin cost=" + originalSolution.cost);

            LOG.debug("after reorder    " + MyExplain.myExplain(template));

            DisjointSet djs = new DisjointSet(1000);
            ArrayList<Range> blockRanges = new ArrayList<>();

            hopId2LeafIndex = new HashMap<>();
            leaves = new ArrayList<>();

            // 找到所有的叶子节点
            findAllLeaf(template, new ArrayList<>(), 0, hopId2LeafIndex, djs);
            if (leaves.size() < 4) return originalSolution;
            LOG.info("number of leaves = " + leaves.size());
//            for (int i = 0; i < leaves.size(); i++) {
//                System.out.println("leavesID: " + i);
//                System.out.println(Explain.explain(leaves.get(i).hop));
//            }

            // 生成singleCes
            ArrayList<SingleCse> singleCses = genSingleCse(djs, blockRanges);

            if (showOriginHop) {
                root.resetVisitStatusForced(new HashSet<>());
                LOG.debug("before coordinate");
                LOG.debug(Explain.explain(root));
            }

            long end = System.nanoTime();
            allGenerateOptionsTime += end - start;
//            LOG.info("generate options time = " + ((end - start) / 1e9) + "s");
//            System.out.println("generate options time = " + ((end - start) / 1e9) + "s");

            start = System.nanoTime();
            MySolution mySolution = null;
            try {
//                if (useDirectPolicy) {
//                    mySolution = testDirectSolution(template, blockRanges);
//                } else if (useDynamicProgramPolicy) {
                    mySolution = testCostTree(singleCses, template, blockRanges);
//                } else if (useBruceForcePolicy) {
//                    mySolution = testBruteForce(singleCses, template, blockRanges);
//                }
//                testCreateHops(template,blockRanges);
            } catch (Exception e) {
                e.printStackTrace();
                mySolution = null;
                //  System.out.println("x");
            }
            end = System.nanoTime();
            allGenerateCombinationsTime += end - start;
//            LOG.info("generate combinations time = " + ((end - start) / 1e9) + "s");
//            System.out.println("generate combinations time = " + ((end - start) / 1e9) + "s");

            if (mySolution != null && mySolution.cost < originalSolution.cost) {
                LOG.info("return rewrited solution");
                System.out.println("return rewrited solution");
                return mySolution;
            } else {
                LOG.info("return original solution");
                System.out.println("return original solution");
            }

//            testusefulCse(singleCses, template);

            //   testCostTree(singleCses, template);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        LOG.debug("final return original hop");
        return originalSolution;

    }


    MySolution testDirectSolution(Hop template, ArrayList<Range> blockRanges) {
        MultiCse multiCse = createBestMultiCse1386();
        LOG.debug(multiCse);
        Hop result = createHop(multiCse, template, blockRanges);
        result = deepCopyHopsDag(result);
        rewriteCommonSubexpressionElimination.rewriteHopDAG(result, new ProgramRewriteStatus());
        LOG.debug("return best template");
        LOG.debug(Explain.explain(result));
        return new MySolution(result);
    }

    MySolution testBruteForce(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges) {
        // 构造出所有的MultiCse

        ArrayList<MultiCse> multiCses = genMultiCse(singleCses);

        if (multiCses.size() == 0) return null;

        // 构造计划
        ArrayList<MySolution> solutions = genSolutions(multiCses, false, template, blockRanges);

        // 估代价并返回代价最小的计划
        MySolution solution = selectSolution(solutions);
//                Hop result = genAndSelectHop(singleCses, multiCses, template, root);
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
        return null;
    }


    void testusefulCse(ArrayList<SingleCse> cses, Hop template, ArrayList<Range> blockRanges) {
        MySolution solution = genSolution(new MultiCse(), template, blockRanges);
        System.out.println(solution);


        //   System.exit(-1);
    }


    MySolution testCostTree(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges) {
        try {
//        ArrayList<Pair<Hop, SingleCse>> pairs = new ArrayList<>();
//        for (SingleCse singleCse : singleCses) {
//            MultiCse multiCse = new MultiCse();
//            multiCse.cses.add(singleCse);
//            Hop hop = createHop(multiCse, template);
//            pairs.add(Pair.of(hop, singleCse));
//        }
//            System.out.println("blockRanges:" + blockRanges);
//            for (int i = 0; i < leaves.size(); i++) {
//                Hop h = leaves.get(i).hop;
//                System.out.println("(" + i + "," + h.getDim1() + "," + h.getDim2() + ")");
//            }


            ArrayList<Hop> hops = createHops(template,blockRanges);

            ArrayList<Pair<SingleCse, Hop>> list = genHopFromSingleCses(singleCses, template, blockRanges);
            SingleCse emptyCse = new SingleCse();
            emptyCse.hash = HashKey.of(0L, 0L);
            Hop emptyHop = createHop(emptyCse, template, blockRanges);
            Pair<SingleCse, Hop> pair = Pair.of(emptyCse, emptyHop);
            list.add(pair);

            ArrayList<SinglePlan> singlePlans = new ArrayList<>();
            for (Pair<SingleCse, Hop> p : list) {
                SinglePlan singlePlan = new SinglePlan();
                singlePlan.hop = p.getRight();
                singlePlan.singleCse = p.getLeft();
                singlePlans.add(singlePlan);
            }

            CostGraph costGraph = new CostGraph(variablesUpdated, iterationNumber);
//        costGraph.testCostTree(list);
//            ACNode bestacnode = costGraph.testOperatorGraph(singlePlans, pair, blockRanges, leaves);
//            ArrayList<OperatorNode> operatorNodeArrayList = new ArrayList<>();
//            if (bestacnode.minAC!=null) operatorNodeArrayList.add(bestacnode.minAC);
//            if (bestacnode.certainAC!=null) operatorNodeArrayList.add(bestacnode.certainAC);
//            operatorNodeArrayList.addAll(bestacnode.uncertainACs.values());
//            operatorNodeArrayList.sort(Comparator.comparingDouble(a -> a.accCost));

            ArrayList<OperatorNode> operatorNodeArrayList = costGraph.testOperatorGraph(singlePlans, pair, blockRanges, leaves,hops);
            long start = System.nanoTime();
            ArrayList<MultiCse> multiCseArrayList = new ArrayList<>();
            for (int i = 0; i < operatorNodeArrayList.size(); i++) {
                OperatorNode operatorNode = operatorNodeArrayList.get(i);
                MultiCse multiCse = createMultiCseFromOperatorNode(operatorNode);
                if (multiCse != null) {
                    multiCseArrayList.add(multiCse);
                    Hop hop = createHop(multiCse, template, blockRanges);
                    hop = copyAndEliminateHop(hop);
//                    solution.multiCse = multiCse;
                    double dpcost = operatorNode.accCost;
                    Pair<NodeCost,OperatorNode> pair3 = costGraph.estimateHopCost(hop);
                    NodeCost cost3 = pair3.getLeft();
//                    cost3.computeCost*=iterationNumber;
//                    cost3.shuffleCost*=iterationNumber;
//                    cost3.broadcastCost*=iterationNumber;
//                    cost3.collectCost*=iterationNumber;
//                    cost2 = NodeCost.add(cost2,cost3);
//                    for (Hop h: solution.preLoopConstants) {
//                        Pair<NodeCost,OperatorNode> pair4 =costGraph.estimateHopCost(h);
//                        cost3 = pair4.getLeft();
//                        cost2 = NodeCost.add(cost2,cost3);
//                    }
                    double hcost = cost3.getSummary();
                    double rate = (dpcost-hcost)/hcost;
                    LOG.info("candidate multi cse:  rcost=" +hcost+", dpcost=" + dpcost +", rate="+rate +"\n" + operatorNode.accCostDetails+"\n" + multiCse);
                    LOG.info(CostGraph.explainOpNode(operatorNode,0));
//                    MySolution    solution = constantUtilByTag.liftLoopConstant(hop);
//                    estimate(solution,true);
//                    if (rate>0.1) {
//                        LOG.info("RATE GREATER THAN 0.1");
//                        LOG.info(CostGraph.explainOpNode(operatorNode,0));
//                        LOG.info("------------------");
//                        LOG.info(CostGraph.explainOpNode(pair3.getRight(),0));
//                    }
                   }
                if (i==200){
                    break;
                }
            }

            LOG.info("candidate muti cse size = " + multiCseArrayList.size());
           // System.exit(100);

            ArrayList<MySolution> mySolutions = genSolutions(multiCseArrayList, true, template, blockRanges);
            MySolution mySolution = selectSolution(mySolutions);
            long end = System.nanoTime();
            CostGraph.estimateTime += end - start;
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

    private void genBlocks(DisjointSet djs, ArrayList<Range> blockRanges, HashMap<HashKey, ArrayList<HashSet<Range>>> hash2Rangesset) {
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
        HashMap<HashKey, ArrayList<HashSet<Range>>> hash2Ranges = new HashMap<>();
        // 划分 块
        genBlocks(djs, blockRanges, hash2Ranges);

        // 构造出所有的SingleCse
        long start = System.nanoTime();
        ArrayList<SingleCse> result = new ArrayList<>();
        for (Map.Entry<HashKey, ArrayList<HashSet<Range>>> e : hash2Ranges.entrySet()) {
            ArrayList<SingleCse> singleCses = genSingleCseFromRanges1(e.getKey(), e.getValue());
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


//    private ArrayList<SingleCse> genSingleCseFromRanges(HashKey hash, ArrayList<HashSet<Range>> ranges, ArrayList<Range> blockRanges) {
//
////        ArrayList<Range> ranges2 = new ArrayList<>();
////        for (ArrayList<Range> ranges1: hash2blockranges.values()) {
////            Range block = ranges1.get(0);
////            ranges2.add(block);
////        }
////        ArrayList<SingleCse>  singleCses =  genSingleCseFromRanges1( hash, ranges2);
//
//        return null;
//    }

    private ArrayList<SingleCse> genSingleCseFromRanges1(HashKey hash, ArrayList<HashSet<Range>> rangesets) {
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
            for (SingleCse singleCse: result) {
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

    private MultiCse createBestMultiCse546() {
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

    private MultiCse createBestMultiCse1386() {
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


    private double estimate(MySolution solution, boolean showDetails) {
        // 代价估计
        double cost = 0;
        if (showDetails) {
            LOG.debug("runtime program<<<");
            FakeCostEstimator2.MMShowCostFlag = true;
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
                preCost += FakeCostEstimator2.estimate(program);
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
            bodyCost = FakeCostEstimator2.estimate(programBlocks);
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
            FakeCostEstimator2.MMShowCostFlag = false;
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

    void testCreateHops( Hop template, ArrayList<Range> blockRanges) {
            ArrayList<Hop> hops = createHops(template,blockRanges);
            if (hops!=null) {
                HashSet<Pair<Long,Long>> ss = new HashSet<>();
                for (Hop hop : hops) {
//                    System.out.println(Explain.explain(hop));
                     Pair<Long, Long> key =  Hash.hashHopDag(hop);
                     ss.add(key);
                }
                LOG.info( "all trees size="+hops.size());
                LOG.info( "hash set size="+ss.size());

            }else {
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
                }catch ( Exception e) {
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
        ArrayList<Pair< HashSet<Triple<Integer, Integer,Integer>>,Hop>> hops = build_binary_trees_all(children);
        ArrayList<Hop> ret = new ArrayList<>();
        HashSet<Triple<Integer, Integer,Integer>> set = new HashSet<>();
        for (Pair< HashSet<Triple<Integer, Integer,Integer>>,Hop> h: hops) {
            int before = set.size();
            set.addAll(h.getKey());
            int after = set.size();
            if (before!=after) {
                ret.add(h.getValue());
            }
        }
        LOG.info("rCreateHop_s "+children.size()+" -> "+ret.size());
        return ret;
    }


    private ArrayList<Pair<HashSet<Triple<Integer, Integer,Integer>>,Hop>> build_binary_trees_all(ArrayList<Hop> mmChain) {
        if (mmChain.size()==0) {
            return new ArrayList<>();
        }
        if (mmChain.size()==1) {
            ArrayList<Pair<HashSet<Triple<Integer, Integer,Integer>>,Hop>> hops = new ArrayList<>();
            HashSet< Triple<Integer, Integer,Integer>> ss = new HashSet<>();
            ss.add(Triple.of(0,0,0));
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
        ArrayList<ArrayList<ArrayList< Pair<HashSet<Triple<Integer, Integer,Integer>>,Hop>  >>> hops = new ArrayList<>();
        for (int i=0;i<n;i++) {
            hops.add(new ArrayList<>());
            for (int j=0;j<n;j++) {
                hops.get(i).add(new ArrayList<>());
            }
        }
        for (int i=0;i<n;i++) {
            HashSet< Triple<Integer, Integer,Integer>> ss = new HashSet<>();
            ss.add(Triple.of(i,i,i));
            hops.get(i).get(i).add(  Pair.of(ss   ,mmChain.get(i) ) );
        }
        for (int l=2;l<=n;l++) {
            for (int i=0;i+l-1<n;i++) {
                int j = i + l-1;
                ArrayList<Pair<HashSet<Triple<Integer, Integer,Integer>>,Hop>> tmp = new ArrayList<>();
                for (int k=i;k<j;k++) {
//                    ArrayList<Hop> a1 = hops.get(i).get(k);
//                    ArrayList<Hop> a2 = hops.get(k+1).get(j);
                    for (Pair<HashSet<Triple<Integer, Integer,Integer>>,Hop> h1: hops.get(i).get(k)) {
                        for (Pair<HashSet<Triple<Integer, Integer,Integer>>,Hop> h2 : hops.get(k+1).get(j)) {
                            Hop h = HopRewriteUtils.createMatrixMultiply(h1.getRight(), h2.getRight());
                            HashSet<Triple<Integer,Integer,Integer>> key = new HashSet<>();
                            key.addAll(h1.getKey());
                            key.addAll(h2.getKey());
                            key.add(Triple.of(i,k,j));
                            tmp.add(Pair.of(key,h));
                        }
                    }
                }
                hops.get(i).set(j,tmp);
            }
        }
        LOG.info("build_binary_trees_all "+mmChain.size()+" -> "+hops.get(0).get(n-1).size());

        return hops.get(0).get(n-1);
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
