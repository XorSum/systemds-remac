package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.*;
import org.apache.sysds.hops.rewrite.dfp.AnalyzeSymmetryMatrix;
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
import org.apache.sysds.runtime.controlprogram.parfor.util.IDSequence;
import org.apache.sysds.utils.Explain;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.CreateRuntimeProgram.constructProgramBlocks;
import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;

public class RewriteCoordinate extends StatementBlockRewriteRule {

    protected static final Log LOG = LogFactory.getLog(RewriteCoordinate.class.getName());

    public static String manualType = null;
    public Coordinate coordinate = new Coordinate();

    private ExecutionContext ec;
    public StatementBlock statementBlock;

    private RewriteCommonSubexpressionElimination rewriteCommonSubexpressionElimination = new RewriteCommonSubexpressionElimination();
    //  private  final Log LOG = LogFactory.getLog(RewriteCoordinate.class.getName());

    // public boolean onlySearchConstantSubExp = false;
    //    public ConstantUtil constantUtil = null; // new ConstantUtil(variablesUpdated);
    public ConstantUtilByTag constantUtilByTag;
    public long iterationNumber = 2;
    static int threadNum = 24;

    // <configuration>
    private boolean showMultiCse = false;
    private boolean showCost = false;
    private boolean showMinCostHop = true;
    private boolean showOriginHop = true;

    // private int hMultiCseId = 6392;

    private int maxMultiCseNumber = -1; // 设为-1,则生成所有的；设为正数，则最多生成那么多个

    // private static long epoch = 100;

    private static boolean useManualPolicy = false;
    public static boolean useDynamicProgramPolicy = true;
    private static boolean useBruceForcePolicy = false;
    private static boolean useBruceForcePolicyMultiThreads = false;
    private static boolean BruteForceMultiThreadsPipeline = true;

    // </configuration>

    public static long allGenerateOptionsTime = 0;
    public static long allGenerateCombinationsTime = 0;
//    public static long estimateTime = 0;

    public MySolution rewiteHopDag(Hop root) {
        MySolution originalSolution = new MySolution(root);

        try {
            LOG.debug("start coordinate " + MyExplain.myExplain(root));
            LOG.debug("root: \n" + Explain.explain(root));

            Triple<Hop, ArrayList<Range>, ArrayList<SingleCse>> triple = coordinate.generateOptions(root);

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
                } else if (useBruceForcePolicyMultiThreads) {
                    mySolution = testBruteForceMultiThreads(singleCses, template, blockRanges,BruteForceMultiThreadsPipeline);
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

    MySolution testManualAll(Hop root, Hop template, ArrayList<Range> blockRanges) {
        MySolution mySolution = null;
        ManualSolution manualSolution = new ManualSolution(this.coordinate);
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
            if (coordinate.leaves.size() == 6) {
                MultiCse multiCse = manualSolution.createMultiCseGdAtaTheta();
                mySolution = genManualSolution(template, blockRanges, multiCse, true);
                LOG.info("return gd-ata theta");
            }
        } else if (manualType.equals("gd-atb")) {
            VariableSet aSet = new VariableSet();
            aSet.addVariable("theta", null);
            if (coordinate.leaves.size() == 6) {
                MultiCse multiCse = manualSolution.createMultiCseGdAtbTheta();
                mySolution = genManualSolution(template, blockRanges, multiCse, true);
                LOG.info("return gd-atb theta");
            }
        } else if (manualType.equals("dfp-spores-ata") && coordinate.leaves.size() == 11) {
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
        Hop result = coordinate.createHop(multiCse, template, blockRanges);
        result = deepCopyHopsDag(result);
        rewriteCommonSubexpressionElimination.rewriteHopDAG(result, new ProgramRewriteStatus());

        Program program = constructProgramBlocks(result, statementBlock);
        LOG.info("before lift constant");
        LOG.info(Explain.explain(program));

        CostGraph costGraph = new CostGraph(coordinate.variablesUpdated, iterationNumber, ec);
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

    MySolution testBruteForceMultiThreads(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges,boolean pipeline) {
        CostGraph costGraph = new CostGraph(coordinate.variablesUpdated, iterationNumber, ec);
        ConcurrentLinkedQueue<MultiCse> multiCses = genMultiCseMultiThreads(singleCses, template, costGraph, blockRanges,pipeline);
        if (!pipeline) {
            MySolution best_solution = null;
            long start_force_estimate = System.nanoTime();
            while (!multiCses.isEmpty()) {
                MultiCse multiCse = multiCses.poll();
//        for (MultiCse multiCse : multiCses) {
                Hop hop = coordinate.createHop(multiCse, template, blockRanges);
                Triple<NodeCost, NodeCost, OperatorNode> costTriple = costGraph.estimateHopCost(hop);
                MySolution solution = constantUtilByTag.liftLoopConstant(hop);
                solution.multiCse = multiCse;
                double cost = costTriple.getLeft().getSummary();
                solution.cost = cost;
                if (best_solution == null || best_solution.cost > cost) {
                    best_solution = solution;
                }
                if (multiCse.id % 1000 == 0) {
                    LOG.info("estimate " + multiCse + " " + costTriple.getLeft());
                }
            }
            long end_force_estimate = System.nanoTime();
            LOG.info("force estimante time = " + ((end_force_estimate - start_force_estimate) / 1e9) + "s");
            LOG.info("force best solution:"+best_solution);
            return best_solution;
        } else {
            LOG.info("force best solution:"+bestsolution);
            return bestsolution;
        }

    }

    MySolution testBruteForce(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges) {
        // 构造出所有的MultiCse

        ArrayList<MultiCse> multiCses = genMultiCse(singleCses);

        if (multiCses.size() == 0) return null;
        long begin = System.nanoTime();

        CostGraph costGraph = new CostGraph(coordinate.variablesUpdated, iterationNumber, ec);
        Hop result = null;
        double minCost = Double.MAX_VALUE;
        for (int i = 0; i < multiCses.size(); i++) {
            MultiCse multiCse = multiCses.get(i);
            Hop hop = coordinate.createHop(multiCse, template, blockRanges);
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
        costGraph.nodeCostEstimator.printCacheStats();
        MySolution mySolution = constantUtilByTag.liftLoopConstant(result);
        return mySolution;
//        return null;
    }

    MySolution testDynamicProgramming(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges) {
        try {
            SingleCse emptyCse = new SingleCse();
            emptyCse.hash = HashKey.of(0L, 0L);
            Hop emptyHop = coordinate.createHop(emptyCse, template, blockRanges);
            ArrayList<Pair<SingleCse, Hop>> list = genHopFromSingleCses(singleCses, template, blockRanges);

//            ArrayList<Hop> hops = createNecessaryHops(singleCses, emptyHop, blockRanges);
            ArrayList<Hop> hops = createHops(template, blockRanges);

//            // debug
//            ArrayList<Hop> hops2 = createHops(template, blockRanges);
//            TreeSet<String> set = new TreeSet<>();
//            TreeSet<String> set2 = new TreeSet<>();
//            TreeSet<String> dSet = new TreeSet<>();
//            TreeSet<String> dSet2 = new TreeSet<>();
//            for (Hop hop : hops) {
//                set.addAll(Explain.explainBlocks(hop));
//            }
//            for (Hop hop : hops2) {
//                set2.addAll(Explain.explainBlocks(hop));
//            }
//            dSet.addAll(set);
//            dSet2.addAll(set2);
//            dSet.removeAll(set2);
//            dSet2.removeAll(set);

            ArrayList<SinglePlan> placePlans = new ArrayList<>();
            for (Hop h: hops) {
                SinglePlan singlePlan = new SinglePlan();
                singlePlan.hop = h;
                placePlans.add(singlePlan);
            }

            ArrayList<SinglePlan> singlePlans = new ArrayList<>();
            for (Pair<SingleCse, Hop> p : list) {
                SinglePlan singlePlan = new SinglePlan();
                singlePlan.hop = p.getRight();
                singlePlan.singleCse = p.getLeft();
                singlePlans.add(singlePlan);
            }
            CostGraph costGraph = new CostGraph(coordinate.variablesUpdated, iterationNumber, ec);

            costGraph.nodeCostEstimator.resetCacheCounter();
            MySolution mySolution = null;
            ArrayList<OperatorNode> operatorNodeArrayList = costGraph.testOperatorGraph(singlePlans,emptyCse, emptyHop, placePlans );

//            costGraph.nodeCostEstimator.range2mmnode.forEach((key, value) -> {
//                System.out.println(key + " -> " + value);
//            });
//            costGraph.nodeCostEstimator.drange2multiplycost.forEach((key, value) -> {
//                System.out.println(key + " -> " + value);
//            });

            long start = System.nanoTime();
            //ArrayList<MultiCse> multiCseArrayList = new ArrayList<>();
            int bestId = -1;
            for (int i = 0; i < 200 && i < operatorNodeArrayList.size(); i++) {
                OperatorNode operatorNode = operatorNodeArrayList.get(i);
                MultiCse multiCse = createMultiCseFromOperatorNode(operatorNode);
                LOG.info(i);
                LOG.info(multiCse);
                LOG.info(operatorNode.accCostDetails);
//                LOG.info(CostGraph.explainOpNode(operatorNode,0));
            }
            int i = 0;
            OperatorNode operatorNode = operatorNodeArrayList.get(i);
            MultiCse multiCse = createMultiCseFromOperatorNode(operatorNode);
            if (multiCse != null) {
                //      multiCseArrayList.add(multiCse);
                Hop hop = coordinate.createHop(multiCse, template, blockRanges);
                hop = copyAndEliminateHop(hop);
                double dpcost = operatorNode.accCost;
//                Triple<NodeCost, NodeCost, OperatorNode> costTriple = costGraph.estimateHopCost(hop);
//                NodeCost cost3 = costTriple.getLeft();
//                double hcost = cost3.getSummary();
//                double rate = (dpcost - hcost) / hcost;
//                LOG.info("candidate multi cse:  rcost=" + hcost + ", dpcost=" + dpcost + ", rate=" + rate + "\n" + operatorNode.accCostDetails + "\n" + multiCse);
//                LOG.info("rcostdetail=" + cost3 + ", dpcostdetail=" + operatorNode.accCostDetails);
                LOG.info("dpcost=" + dpcost + ", \ndpcostdetail=" + operatorNode.accCostDetails);
                LOG.info(CostGraph.explainOpNode(operatorNode, 0));
                MySolution solution = constantUtilByTag.liftLoopConstant(hop);
                solution.multiCse = multiCse;
                if (mySolution == null || mySolution.cost < dpcost) {
                    mySolution = solution;
                    bestId = i;
                }
            }
//            }
            costGraph.nodeCostEstimator.printCacheStats();
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
        ArrayList<MultiCse> result = new ArrayList<>();
        for (int j = 0; j < singleCse.size(); j++) {
            MultiCse c = new MultiCse();
            c.cses.add(singleCse.get(j));
            c.last_index = j;
            result.add(c);
        }
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
        long end = System.nanoTime();
        LOG.info("number of multi cse = " + result.size());
        LOG.info("bfs search all multi cses cost " + ((end - start) / 1e6) + "ms");
        return result;
    }


    private ConcurrentLinkedQueue<MultiCse> genMultiCseMultiThreads(ArrayList<SingleCse> singleCses,
                                                                    Hop template,
                                                                    CostGraph costGraph,
                                                                    ArrayList<Range> blockRanges,
                                                                    boolean pipeline) {
        long start = System.nanoTime();
        // 按长度，降序
        singleCses.sort((SingleCse a, SingleCse b) -> b.cseLength() - a.cseLength());
        //ArrayList<MultiCse> result = new ArrayList<>();
        ConcurrentLinkedQueue<MultiCse> queue = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<MultiCse> generated_multicses = new ConcurrentLinkedQueue<>();

        IDSequence idSequence = new IDSequence();

        for (int j = 0; j < singleCses.size(); j++) {
            queue.add(new MultiCse(singleCses.get(j), j, idSequence.getNextID()));
        }
        CountDownLatch latch = new CountDownLatch(threadNum);
        bestsolution = new MySolution(template);
        ExecutorService fixedThreadPool = Executors.newCachedThreadPool();
        for (int i = 0; i < threadNum; i++) {
            Coordinate ci = coordinate.clone();
            fixedThreadPool.execute(new GenMultiCseRunnable(queue, singleCses, latch, idSequence,
                    template, blockRanges, costGraph, ci, generated_multicses,pipeline));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        LOG.info("number of multi cse = " + generated_multicses.size());
        LOG.info("multi threads force generate combinations time = " + ((end - start) / 1e9) + "s");

        return  generated_multicses;
    }

    MySolution bestsolution ;

    class GenMultiCseRunnable implements Runnable {

        ConcurrentLinkedQueue<MultiCse> result;
        ArrayList<SingleCse> singleCse;
        CountDownLatch latch;
        IDSequence idSequence;
        Hop template;
        ArrayList<Range> blockRanges;
        CostGraph costGraph;
        Coordinate threadCoordinate;
        ConcurrentLinkedQueue<MultiCse> generated_multicses;
        boolean pipeline;

        public GenMultiCseRunnable(ConcurrentLinkedQueue<MultiCse> result,
                                   ArrayList<SingleCse> singleCse,
                                   CountDownLatch latch,
                                   IDSequence idSequence,
                                   Hop template,
                                   ArrayList<Range> blockRanges,
                                   CostGraph costGraph,
                                   Coordinate coordinate,
                                   ConcurrentLinkedQueue<MultiCse> generated_multicses,
                                   boolean pipeline
        ) {
            this.result = result;
            this.singleCse = singleCse;
            this.latch = latch;
            this.idSequence = idSequence;
            this.template = template;
            this.blockRanges = blockRanges;
            this.costGraph = costGraph;
            this.threadCoordinate = coordinate;
            this.generated_multicses = generated_multicses;
            this.pipeline = pipeline;
        }

        @Override
        public void run() {
            LOG.info("GenMultiCseRunnable start");
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    MultiCse frontMC = null;
                    for (int i = 0; i < 3; i++) {
                        frontMC = result.poll();
                        if (frontMC != null) {
                            break;
                        } else {
                            Thread.sleep(1000);
                        }
                    }
                    if (frontMC == null) {
                        break;
                    } else {
                        if (frontMC.id % 100 == 0) {
                            LOG.info(result.size() + " " + frontMC);
                        }
                        generated_multicses.add(frontMC);
                        if (pipeline) {
                            Hop hop = threadCoordinate.createHop_copy_sc(frontMC, template, blockRanges);
                            Triple<NodeCost, NodeCost, OperatorNode> costTriple = costGraph.estimateHopCost(hop);
                            MySolution solution = constantUtilByTag.liftLoopConstant(hop);
                            solution.multiCse = frontMC;
                            double cost = costTriple.getLeft().getSummary();
                            solution.cost = cost;
                            synchronized (bestsolution) {
                                if (bestsolution == null || bestsolution.cost > cost) {
                                    bestsolution = solution;
                                }
                            }
                            if (frontMC.id % 1000 == 0) {
                                LOG.info("estimate " + frontMC + " " + costTriple.getLeft());
                            }
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
                                newMC.id = idSequence.getNextID();
                                result.add(newMC);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                LOG.info("GenMultiCseRunnable stop");
                latch.countDown();
            }
        }
    }


    private MySolution genSolution(MultiCse multiCse, Hop template, ArrayList<Range> blockRanges) {
        try {
            Hop hop = coordinate.createHop(multiCse, template, blockRanges);
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
                hop = coordinate.createHop(c, template, blockRanges);
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

    private ArrayList<Pair<SingleCse, Hop>> genHopFromSingleCses(ArrayList<SingleCse> singleCses, Hop template, ArrayList<Range> blockRanges) {
        HashMap<Pair<Long, Long>, Pair<SingleCse, Hop>> filter = new HashMap<>();
        for (SingleCse sc : singleCses) {
            Hop h = coordinate.createHop(sc, template, blockRanges);
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

    // deprecated
    private ArrayList<Hop> createHops(Hop template, ArrayList<Range> blockRanges) {
        ArrayList<RangeTree> list = new ArrayList<>();
        return createHopInner_s(list, template, blockRanges);
    }

    private ArrayList<Hop> createHopInner_s(ArrayList<RangeTree> list, Hop template, ArrayList<Range> blockRanges) {
        // 准备区间数组
        for (int i = 0; i < coordinate.leaves.size(); i++) {
            SingleCse sc = new SingleCse(Range.of(i, i, false), coordinate.leaves.get(i).hop);
            sc.name = coordinate.getRangeName(i, i);
            RangeTree rangeTree = new RangeTree(i, i, sc, false);
            list.add(rangeTree);
        }

        ArrayList<RangeTree> blockList = new ArrayList<>();
        for (Range r : blockRanges) {
            boolean ok = true;
            for (RangeTree rangeTree : list) {
                if (rangeTree.left == r.left && rangeTree.right == r.right) {
                    ok = false;
                    break;
                }
            }
            if (!ok) continue;
            SingleCse sc = new SingleCse(Range.of(r.left, r.right, false), null);
            sc.name = coordinate.getRangeName(r);
            blockList.add(new RangeTree(r.left, r.right, sc, coordinate.isTranspose(r.left, r.right)));
        }
        list.addAll(blockList);

        // 把区间数组排序，先按右端点升序，再按左端点降序
        list.sort((RangeTree ra, RangeTree rb) -> {
            if (ra.right != rb.right)
                return ra.right - rb.right;
            return rb.left - ra.left;
        });

        // 用栈建起RangeTree
        Stack<RangeTree> stack = new Stack<>();
        for (RangeTree cur : list) {
            while (!stack.empty() && stack.peek().left >= cur.left) {
                cur.children.add(stack.pop());
            }
            Collections.reverse(cur.children);
            stack.push(cur);
        }
        ArrayList<Hop> ret = new ArrayList<>();

        // 创建并替换block的hop
        for (RangeTree rt : stack) {
            try {
                ArrayList<Hop> block_hops = rCreateHop_s(rt);
                assert block_hops != null;
                for (Hop block_hop : block_hops) {
                    // 找到这个块的位置
                    Hop copy = deepCopyHopsDag(template);
                    Hop cur = copy;
                    Hop parent = null;
                    Leaf leaf1 = coordinate.leaves.get(rt.left);
                    Leaf leaf2 = coordinate.leaves.get(rt.right);
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
    }

    private ArrayList<Hop> createNecessaryHops(ArrayList<SingleCse> singleCses, Hop template,
                                               ArrayList<Range> blockRanges) {
        if (singleCses.size() == 0) {
            return new ArrayList<>(0);
        }

        ArrayList<Hop> ret = new ArrayList<>();

        TreeSet<Range> cseRangeSet = new TreeSet<>();
        for (SingleCse cse : singleCses) {
            cseRangeSet.addAll(cse.ranges);
        }
        ArrayList<Range> cseRanges = new ArrayList<>(cseRangeSet);

        blockRanges.sort(Comparator.comparingInt((Range a) -> a.left));
        int idx = 0;
        for (Range blockRange : blockRanges) {
            int preIdx = idx;
            while (idx < cseRanges.size() && cseRanges.get(idx).right <= blockRange.right) {
                idx++;
            }
            ret.addAll(createNecessaryHopsPerBlock(blockRange, cseRanges.subList(preIdx, idx), template));
        }

        return ret;
    }

    private ArrayList<Hop> createNecessaryHopsPerBlock(Range blockRange, List<Range> cseRanges, Hop template) {
        ArrayList<Hop> ret = new ArrayList<>();

        ArrayList<RangeTree> dataList = new ArrayList<>(blockRange.size());
        for (int i = blockRange.left; i <= blockRange.right; i++) {
            SingleCse sc = new SingleCse(Range.of(i, i, false), coordinate.leaves.get(i).hop);
            sc.name = coordinate.getRangeName(i, i);
            dataList.add(new RangeTree(i, i, sc, false));
        }

        for (Range cseRange : cseRanges) {
            ArrayList<RangeTree> children = new ArrayList<>(cseRange.size());
            for (int i = cseRange.left; i <= cseRange.right; i++) {
                children.add(dataList.get(i - blockRange.left));
            }
            RangeTree cseTree = createOptimalRangeTrees(children, false).get(0);

            children = new ArrayList<>(blockRange.size() - cseRange.size() + 1);
            for (int i = blockRange.left; i <= blockRange.right; i++) {
                if (i == cseRange.left) {
                    children.add(cseTree);
                    i += cseRange.size() - 1;
                } else {
                    children.add(dataList.get(i - blockRange.left));
                }
            }
            ArrayList<RangeTree> blockTrees = createOptimalRangeTrees(children, true);

            for (RangeTree tree : blockTrees) {
                Hop hop = coordinate.rCreateHop(tree);
                Hop copy = deepCopyHopsDag(template);
                Hop cur = copy;
                Hop parent = null;
                Leaf leaf1 = coordinate.leaves.get(tree.left);
                Leaf leaf2 = coordinate.leaves.get(tree.right);

                for (int i = 0; i < leaf1.depth && i < leaf2.depth; i++) {
                    if (leaf1.path.get(i).equals(leaf2.path.get(i))) {
                        parent = cur;
                        assert cur != null;
                        cur = cur.getInput().get(leaf1.path.get(i));
                    } else {
                        break;
                    }
                }

                if (parent != null) {
                    HopRewriteUtils.replaceChildReference(parent, cur, hop);
                } else {
                    copy = hop;
                }

                ret.add(copy);
            }
        }

        return ret;
    }

    private ArrayList<RangeTree> createOptimalRangeTrees(ArrayList<RangeTree> treesList, boolean mult) {
        Queue<ArrayList<RangeTree>> queue = new LinkedList<>();
        ArrayList<RangeTree> ret = new ArrayList<>();

        queue.offer(treesList);
        while (!queue.isEmpty()) {
            ArrayList<RangeTree> trees = queue.poll();
            ArrayList<ArrayList<RangeTree>> list = createOptimalRangeTreesList(trees, mult);
            for (ArrayList<RangeTree> l : list) {
                if (l.size() == 1) {
                    ret.add(l.get(0));
                } else {
                    queue.offer(l);
                }
            }
        }

        return ret;
    }

    private ArrayList<ArrayList<RangeTree>> createOptimalRangeTreesList(ArrayList<RangeTree> trees, boolean mult) {
        ArrayList<ArrayList<RangeTree>> ret = new ArrayList<>();

        if (trees.size() < 2) {
            ret.add(trees);
            return ret;
        }

        Long[] costs = new Long[trees.size() - 1];
        for (int i = 0; i < trees.size() - 1; i++) {
            RangeTree a = trees.get(i);
            RangeTree b = trees.get(i + 1);
            long nRow = coordinate.leaves.get(a.left).hop.getCharacteristics().getRows();
            long nMiddle = coordinate.leaves.get(a.left).hop.getCharacteristics().getCols();
            long nCol = coordinate.leaves.get(b.right).hop.getCharacteristics().getCols();
            costs[i] = nRow * nMiddle * nCol;
        }

        long min = Collections.min(Arrays.asList(costs));
        ArrayList<Integer> idxList = new ArrayList<>();
        for (int i = 0; i < costs.length; i++) {
            if (costs[i] == min) {
                idxList.add(i);
                if (!mult) {
                    break;
                }
            }
        }

        for (int idx : idxList) {
            ArrayList<RangeTree> list = new ArrayList<>(costs.length);
            for (int i = 0; i < trees.size(); i++) {
                RangeTree a = trees.get(i);
                if (i == idx) {
                    i++;
                    RangeTree b = trees.get(i);
                    SingleCse sc = new SingleCse(Range.of(a.left, b.right, false), null);
                    sc.name = coordinate.getRangeName(Range.of(a.left, b.right, false));
                    RangeTree newTree = new RangeTree(a.left, b.right, sc, coordinate.isTranspose(a.left, b.right));
                    newTree.children.add(a);
                    newTree.children.add(b);
                    list.add(newTree);
                } else {
                    list.add(a);
                }
            }
            ret.add(list);
        }

        return ret;
    }

    private ArrayList<Hop> rCreateHop_s(RangeTree rt) {
        ArrayList<Hop> children = new ArrayList<>();
        for (RangeTree son : rt.children) {
            Hop s = coordinate.rCreateHop(son);
            if (s == null) throw new NullPointerException();
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


    public boolean isSame(int l1, int r1, int l2, int r2) {

        boolean result = true;
        if (r1 - l1 != r2 - l2) result = false;
        for (int i = l1, j = l2; result && i <= r1 && j <= r2; i++, j++) {
            Hop h1 = coordinate.leaves.get(i).hop;
            Hop h2 = coordinate.leaves.get(j).hop;
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
        if (result == false && coordinate.getRangeName(l1, r1).equals(coordinate.getRangeName(l2, r2))) {
            LOG.debug("issame " + l1 + " " + r1 + " " + l2 + " " + r2 + " " + result);
            LOG.debug(coordinate.getRangeName(l1, r1) + " | " + coordinate.getRangeName(l2, r2));
            result = true;
            for (int i = l1, j = l2; result && i <= r1 && j <= r2; i++, j++) {
                Hop h1 = coordinate.leaves.get(i).hop;
                Hop h2 = coordinate.leaves.get(j).hop;
                if (HopRewriteUtils.isTransposeOperation(h1) && AnalyzeSymmetryMatrix.querySymmetry(h1.getName()))
                    h1 = h1.getInput().get(0);
                if (HopRewriteUtils.isTransposeOperation(h2) && AnalyzeSymmetryMatrix.querySymmetry(h2.getName()))
                    h2 = h2.getInput().get(0);
                LOG.debug(h1.getName() + " " + h2.getName());
            }


        }

        return result;
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
        this.coordinate.variablesUpdated = variablesUpdated;
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
