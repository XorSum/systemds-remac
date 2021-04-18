package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.NaryOp;
import org.apache.sysds.hops.TernaryOp;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.coordinate.Range;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;
import org.apache.sysds.hops.rewrite.dfp.costmodel.CostModelCommon;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class CostGraph {
    protected static final Log LOG = LogFactory.getLog(CostGraph.class.getName());

    public CostGraph(VariableSet variablesUpdated, long iterationNumber, ExecutionContext ec) {
        this.variablesUpdated = variablesUpdated;
        this.iterationNumber = iterationNumber;
        this.ec = ec;
        this.nodeCostEstimator = new NodeCostEstimator((SparkExecutionContext) ec);
    }

    private ExecutionContext ec;
    public NodeCostEstimator nodeCostEstimator;
    long iterationNumber = 2;
    public VariableSet variablesUpdated = null;

    public static long dynamicProgramTime = 0;

    public ArrayList<OperatorNode> testOperatorGraph(ArrayList<SinglePlan> singlePlans,
                                                     SingleCse emptyCse,
                                                     Hop emptyHop,
                                                     ArrayList<SinglePlan> placePlans) {

        // 构建 cost graph
        build_cost_graph(singlePlans, emptyCse, emptyHop, placePlans);

        nodeCostEstimator.printCacheStats();

        // 回收mnc使用的内存
        for (MMNode mmNode : nodeCostEstimator.range2mmnodeCache.values()) {
            mmNode.setSynopsis(null);
        }
        nodeCostEstimator.range2mmnodeCache.clear();
        nodeCostEstimator.drange2multiplycostCache.clear();
        System.gc();

//        System.out.println("after build cost graph");
//        PrintGcTime.printGcTime();

        CseStateMaintainer MAINTAINER = new CseStateMaintainer();
        MAINTAINER.initRangeCounter(range2acnode);
        MAINTAINER.initCseState(singlePlans);

        // 动态规划
        ArrayList<OperatorNode> result = selectBest(MAINTAINER);

//        System.out.println("after dynamic programming");
//        PrintGcTime.printGcTime();

        LOG.info("end test Operator Graph");
        return result;
    }

    void build_cost_graph(ArrayList<SinglePlan> singlePlans,
                          SingleCse emptyCse,
                          Hop emptyHop,
                          ArrayList<SinglePlan> placePlans) {
        LOG.info("begin test Operator Graph");

//        System.out.println("before build cost graph");
//        PrintGcTime.printGcTime();

        CostModelCommon.MMShowCostFlag = true;

        long start_build_graph = System.nanoTime();
        LOG.info("build cost graph start");

        OperatorNode emptyNode = createOperatorGraph(emptyHop, false);
        analyzeOperatorRange(emptyNode, emptyCse, new MutableInt(0));
        HashSet<Pair<Integer, Integer>> ranges = new HashSet<>();
        rGetRanges(emptyNode, ranges);


        for (SinglePlan p : placePlans) {
            OperatorNode node = createOperatorGraph(p.hop, false);
            analyzeOperatorRange(node, null, new MutableInt(0));
            p.node = node;
        }

        for (SinglePlan p : singlePlans) {
            SingleCse cse = p.singleCse;
            Hop hop = p.hop;
            OperatorNode node = createOperatorGraph(hop, false);
            analyzeOperatorRange(node, cse, new MutableInt(0));
            boolean certainused = rCheckCertainUsed(cse, ranges);
            if (cse.isConstant) {
                p.tag = SinglePlan.SinglePlanTag.constant;
            } else {
                p.tag = certainused ? SinglePlan.SinglePlanTag.Useful : SinglePlan.SinglePlanTag.uncertain;
            }
            analyzeOperatorConstant(node);
            p.node = node;
        }

//        for (Map.Entry<DRange,HashSet<DRange>>  e: commonDranges.entrySet()) {
//            LOG.info(e.getKey()+" "+e.getValue().size());
//        }

        for (DRange dRange: nodeCostEstimator.dRangeDisjointSet.keys()) {
            HashSet<DRange> values = nodeCostEstimator.dRangeDisjointSet.elements(dRange);
            LOG.info(dRange+" "+values.size());
        }
        for (Pair<Integer, Integer> range: nodeCostEstimator.rangeDisjointSet.keys()) {
           HashSet<Pair<Integer, Integer>> values = nodeCostEstimator.rangeDisjointSet.elements(range);
            LOG.info(range+" "+values.size());
        }

        analyzeOperatorCostTemplate(emptyNode);

        for (SinglePlan singlePlan: placePlans) {
            analyzeOperatorCostTemplate(singlePlan.node);
        }

        for (SinglePlan singlePlan: singlePlans) {
            analyzeOperatorCost(singlePlan.node);
        }

        range2acnode.forEach((range,acnode)->{
            LOG.info("range: "+range);
            acnode.drange2emptynodes.keySet().forEach(dRange -> {LOG.info("      "+dRange);});
        });

        long end_build_graph = System.nanoTime();
        LOG.info("build cost graph end");
        LOG.info("build cost graph time = " + ((end_build_graph - start_build_graph) / 1e9));

    }


    boolean rCheckCertainUsed(SingleCse cse, HashSet<Pair<Integer, Integer>> ranges) {
        boolean ans = true;
        for (Range cr : cse.ranges) {
            if (!ranges.contains(Pair.of(cr.left, cr.right))) {
                ans = false;
                break;
            }
        }
        return ans;
    }

    void rGetRanges(OperatorNode node, HashSet<Pair<Integer, Integer>> ranges) {
        ranges.add(node.dRange.getRange());
        for (int i = 0; i < node.inputs.size(); i++) {
            rGetRanges(node.inputs.get(i), ranges);
        }
    }


    void showBest(Pair<Integer, Integer> range) {
        System.out.println("range: " + range);
        ArrayList<OperatorNode> list2 = new ArrayList<>();
        //            if (e.getValue().dependencies.size() > 2) {
        //                System.out.println("dependencies size > 2");
        //            }
        list2.addAll(range2acnode.get(range).uncertainACs.values());
        list2.sort(Comparator.comparingDouble(a -> a.accCost));
//        && list2.get(i).accCost <= bestsinglecsenode.accCost
        for (int i = 0; i < 30 && i < list2.size(); i++) {
            System.out.println(list2.get(i));
        }
    }


    public static String explainOpNode(OperatorNode node, int d) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < d; i++) sb.append(" ");
        sb.append("{").append(node).append("\n");
        for (int i = 0; i < node.inputs.size(); i++) {
            sb.append(explainOpNode(node.inputs.get(i), d + 1)).append("\n");
        }
        for (int i = 0; i < d; i++) sb.append(" ");
        sb.append("}");
        return sb.toString();
    }


    public static String explainOpNodeJson(OperatorNode node, int d) {
        StringBuilder sb = new StringBuilder();
        //  for (int i = 0; i < d; i++) sb.append(" ");
        sb.append("{").append("\"op\":\"").append(node).append("\"");
        for (int i = 0; i < node.inputs.size(); i++) {
            sb.append(",\"in").append(i).append("\":");
            sb.append(explainOpNodeJson(node.inputs.get(i), d + 1));
        }
        //  for (int i = 0; i < d; i++) sb.append(" ");
        sb.append("}");
        return sb.toString();
    }


    OperatorNode createOperatorGraph(Hop hop, boolean transpose) {
//        System.out.println(hop.getHopID()+" "+hop.getName()+" "+hop.getOpString());
//        if (mp.containsKey(hop)) {
//            return mp.get(hop);
//        }
        OperatorNode node = null;  //= new OperatorNode();
        if (Judge.isWrite(hop)) {
            node = createOperatorGraph(hop.getInput().get(0), transpose);
        } else if (Judge.isLeafMatrix(hop)) {
            node = new OperatorNode();
            node.isTranspose = transpose;
        } else if (HopRewriteUtils.isTransposeOperation(hop)) {
            node = createOperatorGraph(hop.getInput().get(0), !transpose);
        } else if (hop instanceof LiteralOp) {
            return null;
        } else if (HopRewriteUtils.isUnary(hop, Types.OpOp1.CAST_AS_SCALAR)) {
            node = createOperatorGraph(hop.getInput().get(0), transpose);
        } else if (hop instanceof TernaryOp || hop instanceof NaryOp) {
            ArrayList<OperatorNode> operatorNodes = new ArrayList<>();
            if (!transpose) {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    if (!(hop.getInput().get(i) instanceof LiteralOp)) {
                        OperatorNode tmp = createOperatorGraph(hop.getInput().get(i), transpose);
                        operatorNodes.add(tmp);
                    }
                }
            } else {
                for (int i = hop.getInput().size() - 1; i >= 0; i--) {
                    if (!(hop.getInput().get(i) instanceof LiteralOp)) {
                        OperatorNode tmp = createOperatorGraph(hop.getInput().get(i), transpose);
                        operatorNodes.add(tmp);
                    }
                }
            }
            if (operatorNodes.size() < 1) return null;
            node = operatorNodes.get(0);
            for (int i = 1; i < operatorNodes.size(); i++) {
                OperatorNode right = operatorNodes.get(i);
                OperatorNode sum = new OperatorNode();
                sum.inputs.add(node);
                sum.inputs.add(right);
                sum.hops.add(hop);
                sum.isTranspose = transpose;
                node = sum;
            }
        } else {
            ArrayList<OperatorNode> tmpNodes = new ArrayList<>();
            if (!transpose) {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    OperatorNode tmp = createOperatorGraph(hop.getInput().get(i), transpose);
                    if (tmp == null) continue;
                    tmpNodes.add(tmp);
                }
            } else {
                for (int i = hop.getInput().size() - 1; i >= 0; i--) {
                    OperatorNode tmp = createOperatorGraph(hop.getInput().get(i), transpose);
                    if (tmp == null) continue;
                    tmpNodes.add(tmp);
                }
            }
            if (tmpNodes.size() == 1) {
                node = tmpNodes.get(0);
            } else if (tmpNodes.size() > 1) {
                node = new OperatorNode();
                node.inputs = tmpNodes;
            } else {
                return null;
            }
            node.isTranspose = transpose;
        }
        // System.out.println("put " + node);
        if (node != null) {
            if (node.hops.size() == 0 || !node.hops.contains(hop)) {
                node.hops.add(hop);
            }
        }
//        else {
//            System.out.println(Explain.explain(hop));
//           System.exit(-3);
//        }
        return node;
    }



//    HashMap<DRange, Range> dRange2RangeHashMap = new HashMap<>();


    void analyzeOperatorRange(OperatorNode root, SingleCse cse, MutableInt opIndex) {
        HashSet<Pair<DRange, Range>> cseNodes = new HashSet<>();
        analyzeOperatorRangeInner(root, cse, opIndex, cseNodes);
        assert cse.ranges.size() == cseNodes.size();
//        for (Pair<DRange, Range> p : cseNodes) {
//            DRange key = p.getLeft();
//            if (!dRange2RangeHashMap.containsKey(key)) {
//                dRange2RangeHashMap.put(key, p.getRight());
//            }
//        }
        for (Pair<DRange, Range> p1 : cseNodes) {
            nodeCostEstimator.rangepair2rangeclass.put(p1.getLeft().getRange(),p1.getRight());
            for (Pair<DRange, Range> p2 : cseNodes) {
                nodeCostEstimator.dRangeDisjointSet.merge(p1.getLeft(),p2.getLeft());
                nodeCostEstimator.rangeDisjointSet.merge(p1.getLeft().getRange(),p2.getLeft().getRange());
            }
        }
    }


    void analyzeOperatorRangeInner(OperatorNode root,
                                   SingleCse cse,
                                   MutableInt opIndex,
                                   HashSet<Pair<DRange, Range>> cseNodes) {
        ArrayList<Integer> index = new ArrayList<>();
        int begin = opIndex.getValue();
        if (root.inputs.size() > 0) {
            for (int i = 0; i < root.inputs.size(); i++) {
                index.add(opIndex.getValue());
                analyzeOperatorRangeInner(root.inputs.get(i), cse, opIndex, cseNodes);
                //  root.dependencies.addAll(root.inputs.get(i).dependencies);
            }
        } else {
            opIndex.increment();
        }
        int end = opIndex.getValue() - 1;
        index.add(end);
        if (root.dRange == null) {
            root.dRange = new DRange(index);
//        System.out.println("analyze range: " + root.range);
            if (cse != null) {
                for (Range range : cse.ranges) {
                    if ((range.left == begin && range.right == end) ||
                            (range.left == end && range.right == begin)) {
                        root.dependencies.add(cse);
                        root.dRange.cseRangeTransposeType = range.transpose;
                        cseNodes.add(Pair.of(root.dRange, range));
                        break;
                    }
                }
            }
        }
    }


    boolean analyzeOperatorConstant(OperatorNode node) {
        if (variablesUpdated == null) return false;
        boolean ans = true;
        for (int i = 0; i < node.inputs.size(); i++) {
            if (!analyzeOperatorConstant(node.inputs.get(i))) {
                ans = false;
            }
        }
        for (Hop h : node.hops) {
            if (Judge.isRead(h)) {
                if (variablesUpdated.containsVariable(h.getName())) {
                    ans = false;
                }
            }
        }
        //  System.out.println(node.hop.getName() + " " + ans);
        node.isConstant = ans;
        return ans;
    }

    void analyzeOperatorCostTemplate(OperatorNode node) {
        if (node.inputs.size() == 0) {
            node.accCost = 0;
            node.accCostDetails = NodeCost.ZERO();
        }
        NodeCost thisCostDetail = this.nodeCostEstimator.getNodeCost(node);
        node.thisCost = thisCostDetail.getSummary();
        node.thisCostDetails = thisCostDetail;
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorCostTemplate(node.inputs.get(i));
        }
        if (!range2acnode.containsKey(node.dRange.getRange())) {
            ACNode acNode = new ACNode();
            acNode.range = node.dRange.getRange();
            acNode.emptyOpnode = node;
            acNode.addEmptyOperatorNode(node);
            acNode.addOperatorNode(node);
            range2acnode.put(node.dRange.getRange(), acNode);
        } else {
            ACNode acNode = range2acnode.get(node.dRange.getRange());
            acNode.addEmptyOperatorNode(node);
            acNode.addOperatorNode(node);
        }
    }

//    void analyzeOperatorCost(OperatorNode node) {
//        if (node.inputs.size() == 0) {
//            node.accCost = 0;
//            node.accCostDetails = NodeCost.ZERO();
//        }
//        NodeCost thisCostDetail = this.nodeCostEstimator.getNodeCost(node);
//        for (int i = 0; i < node.inputs.size(); i++) {
//            analyzeOperatorCost(node.inputs.get(i));
//        }
//        int csesize = 1;
//        for (SingleCse singleCse : node.dependencies) {
//            for (int i = 0; i < singleCse.ranges.size(); i++) {
//                if (singleCse.ranges.get(i).left == node.dRange.getLeft()
//                        && singleCse.ranges.get(i).right == node.dRange.getRight()) {
//                    csesize = singleCse.ranges.size();
//                    break;
//                }
//            }
//        }
//        if (csesize > 0) {
////            thisCost = thisCost / csesize;
//            thisCostDetail.multiply(1.0 / csesize);
//            //   accCost = accCost / csesize;
//        }
//        if (node.isConstant) {
//            thisCostDetail.multiply(1.0 / iterationNumber);
//        }
//        //  accCost += thisCost;
//        //  node.accCost = accCost;
//        node.thisCostDetails = thisCostDetail;
//        node.thisCost = thisCostDetail.getSummary();
//        // if (node.range.getLeft()==2&&node.range.getRight()==3)   System.out.println(thisCost);
//        //  System.out.println(node);
//        range2acnode.get(node.dRange.getRange()).addOperatorNode(node);
////        if (node.range.getLeft()==2&&node.range.getRight()==3) {
////            System.out.println("node(2,3): "+ node);
////        }
////        System.out.println("add node "+node.range+" "+node);
//    }


    void analyzeOperatorCost(OperatorNode node) {
        if (node.inputs.size() == 0) {
            return;
        }
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorCost(node.inputs.get(i));
        }
        if (!node.dependencies.isEmpty()) {
            for (SingleCse singleCse : node.dependencies) {
                int  csesize = singleCse.ranges.size();
                Collection<OperatorNode> operators = range2acnode.get(node.dRange.getRange()).getEmptyOperatorNodes();
                for (OperatorNode n : operators) {
                    OperatorNode node1 =  n.copy();
                    NodeCost thisCostDetail = n.thisCostDetails.clone();
                    if (csesize > 0) {
                        thisCostDetail.multiply(1.0 / csesize);
                    }
                    if (node.isConstant) {
                        thisCostDetail.multiply(1.0 / iterationNumber);
                    }
                    node1.thisCostDetails = thisCostDetail;
                    node1.thisCost = thisCostDetail.getSummary();
                    node1.dependencies.add(singleCse);
                    range2acnode.get(node1.dRange.getRange()).addOperatorNode(node1);
                }
            }
        }
    }

   HashMap<Pair<Integer, Integer>, ACNode> range2acnode = new HashMap<>();


    void classifyOperatorNode(CseStateMaintainer MAINTAINER, ArrayList<OperatorNode> allResults, ACNode acNode) {
        acNode.uncertainACs = new HashMap<>();
        int removed1 = 0, removed2 = 0;
        for (OperatorNode node : allResults) {
            boolean hasUselessCse = MAINTAINER.hasUselessCse(node.dependencies);
            boolean hasUncertainCse = MAINTAINER.hasUncertain(node.dependencies);
            if (hasUselessCse) {
                removed1++;
                continue;
            }
            for (Iterator<SingleCse> cseIterator = node.dependencies.iterator(); cseIterator.hasNext(); ) {
                SingleCse cse = cseIterator.next();
                if (MAINTAINER.getCseState(cse) == CseStateMaintainer.CseState.certainlyUseful) {
                    node.oldDependencies.add(cse);
                    cseIterator.remove();
                }
            }
            if (hasUncertainCse) {
                acNode.addUncertainAC(node);
            } else {
                removed2++;
                if (acNode.certainAC == null
                        || acNode.certainAC.accCost > node.accCost
                        || ((Math.abs(acNode.certainAC.accCost - node.accCost) < 0.001)
                        && acNode.certainAC.dependencies.size() + acNode.certainAC.oldDependencies.size() < node.dependencies.size() + node.oldDependencies.size())) {
                    acNode.certainAC = node;
                }
            }
            if (acNode.minAC == null
                    || acNode.minAC.accCost > node.accCost
                    || ((Math.abs(acNode.minAC.accCost - node.accCost) < 0.001)
                    && acNode.minAC.dependencies.size() + acNode.minAC.oldDependencies.size() < node.dependencies.size() + node.oldDependencies.size())) {
                acNode.minAC = node;
            }
        }
//        System.out.println(acNode.range + " remove " + removed1 + " " + removed2);
    }

    public static boolean parallelDynamicProgramming = true;

    ArrayList<OperatorNode> selectBest(CseStateMaintainer MAINTAINER) {
        long start = System.nanoTime();
        LOG.info("start dp");

        ArrayList<Pair<Integer, Integer>> sortedRanges = new ArrayList<>(range2acnode.keySet());
        sortedRanges.sort(Comparator.comparingInt((Pair<Integer, Integer> a) -> (a.getRight() - a.getLeft())).thenComparingInt(Pair::getLeft));
        LOG.info(sortedRanges);
        for (Pair<Integer, Integer> boundery : sortedRanges) {
            LOG.info("boundery: " + boundery);
//            if (boundery.getRight()-boundery.getLeft()>2) break;
//            if (boundery.getLeft() == 1 && boundery.getRight() == 19) {
//                System.out.println("x");
//            }

            ACNode acNode = range2acnode.get(boundery);
            ArrayList<OperatorNode> allResults = new ArrayList<>();

            for (DRange drange : acNode.drange2operatornodes.keySet()) {
                Pair<Integer, Integer> lRange = drange.getLeftRange();
                Pair<Integer, Integer> rRange = drange.getRightRange();
                if (lRange == null || rRange == null) continue;
                if (lRange.equals(boundery) || rRange.equals(boundery)) continue;
                ACNode lac = range2acnode.get(lRange);
                ACNode rac = range2acnode.get(rRange);
                if (lac == null || rac == null) continue;
                ArrayList<OperatorNode> lops = lac.getOperatorNodes(MAINTAINER);
                ArrayList<OperatorNode> rops = rac.getOperatorNodes(MAINTAINER);
                ArrayList<OperatorNode> mids = new ArrayList<>(acNode.drange2operatornodes.get(drange).values());
//                System.out.println();
                LOG.info("  " + lRange + " " + rRange + " " + lops.size() + " " + rops.size() + " " + mids.size());
//                System.out.println(lops);
//                System.out.println(rops);
//                System.out.println(mids);

                Stream<OperatorNode> opstream = parallelDynamicProgramming ? lops.parallelStream() : lops.stream();

                List<OperatorNode> tmp = opstream
                        .flatMap(lop -> {
                            ArrayList<Triple<OperatorNode, OperatorNode, OperatorNode>> arrayList = new ArrayList<>();
                            for (OperatorNode mid : mids) {
                                for (OperatorNode rop : rops) {
                                    if (check(lop, rop, mid.dependencies)) {
                                        arrayList.add(Triple.of(lop, mid, rop));
                                    }
                                }
                            }
                            return arrayList.stream();
                        })
                        .map(triple -> createOperatorNode(triple.getLeft(), lRange, triple.getRight(), rRange, triple.getMiddle(), boundery))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                allResults.addAll(tmp);
            }

            classifyOperatorNode(MAINTAINER, allResults, acNode);

            if (sortedRanges.indexOf(boundery) < sortedRanges.size() - 1) {
                LOG.info("update matainer state");
                MAINTAINER.updateCseState(acNode, range2acnode);

                for (SingleCse singleCse : MAINTAINER.map.keySet()) {
                    boolean in = true;
                    for (Range range : singleCse.ranges) {
                        if (range.left < boundery.getLeft() || range.right > boundery.getRight()) {
                            in = false;
                            break;
                        }
                    }
                    if (in && MAINTAINER.getCseState(singleCse) == CseStateMaintainer.CseState.uncertain) {
                        if (acNode.minAC != null && acNode.minAC.dependencies.contains(singleCse)) {
                            MAINTAINER.setCseState(singleCse, CseStateMaintainer.CseState.certainlyUseful);
                        } else {
                            MAINTAINER.setCseState(singleCse, CseStateMaintainer.CseState.certainlyUseless);
                        }
                    }
                }
            }
            if (!range2acnode.containsKey(boundery)) {
                range2acnode.put(boundery, new ACNode());
            } else {
                LOG.info("boundery: " + boundery + " uncertainACs size: " + range2acnode.get(boundery).uncertainACs.size());
            }

            MAINTAINER.printCseNumStats();

        }

        ArrayList<OperatorNode> resultNodes = null;
        if (sortedRanges.size() > 0) {
            resultNodes = range2acnode.get(sortedRanges.get(sortedRanges.size() - 1)).getOperatorNodes(MAINTAINER);
            resultNodes.sort(Comparator.comparingDouble(a -> a.accCost));
        }
        LOG.info("end dp");
        long end = System.nanoTime();
        dynamicProgramTime += end - start;
        return resultNodes;
    }


    boolean check(OperatorNode operatorNode1,
                  OperatorNode operatorNode2,
                  HashSet<SingleCse> midcses) {

        if (!checkConflict(operatorNode1.dependencies, operatorNode2.dependencies)) return false;
        if (!checkAAA(operatorNode1.dependencies, operatorNode1.dRange.getRange(),
                operatorNode2.dependencies, operatorNode2.dRange.getRange()))
            return false;

        if (!checkOOOO(operatorNode1, operatorNode2, midcses)) {
////            System.out.println(midcses);
////            System.out.println(operatorNode1);
////            System.out.println(operatorNode2);
////            System.out.println("------------------------");
            return false;
        }

        if (!checkIIII(operatorNode1, operatorNode2)) {
//            System.out.println(operatorNode1);
//            System.out.println(operatorNode2);
//            System.out.println("------------------------");
            return false;
        }

        return true;
    }

    boolean checkOOOO(OperatorNode operatorNode1, OperatorNode operatorNode2,
                      HashSet<SingleCse> midcses) {
        if (!checkConflict(midcses, operatorNode1.dependencies)) return false;
        if (!checkConflict(midcses, operatorNode2.dependencies)) return false;
//        if (!checkConflict(midcses, operatorNode1.oldDependencies)) return false;
//        if (!checkConflict(midcses, operatorNode2.oldDependencies)) return false;
        return true;
    }

    boolean checkIIII(OperatorNode operatorNode1, OperatorNode operatorNode2) {
//        if (!checkConflict(operatorNode1.dependencies, operatorNode2.oldDependencies)) return false;
//        if (!checkConflict(operatorNode1.oldDependencies, operatorNode2.dependencies)) return false;
        if (!checkConflict(operatorNode1.oldDependencies, operatorNode2.oldDependencies)) return false;

//        if (!checkAAA(operatorNode1.dependencies,operatorNode1.range, operatorNode2.oldDependencies,operatorNode2.range)) return false;
//        if (!checkAAA(operatorNode1.oldDependencies,operatorNode1.range, operatorNode2.dependencies,operatorNode2.range)) return false;

        if (!checkAAA(operatorNode1.oldDependencies, operatorNode1.dRange.getRange(),
                operatorNode2.oldDependencies, operatorNode2.dRange.getRange()))
            return false;
        return true;
    }


    boolean checkConflict(HashSet<SingleCse> singleCses1, HashSet<SingleCse> singleCses2) {
        if (singleCses1.isEmpty() || singleCses2.isEmpty()) return true;
        for (SingleCse lcse : singleCses1) {
            if (lcse.ranges.size() == 0) continue;
            for (SingleCse rcse : singleCses2) {
                if (rcse.ranges.size() == 0) continue;
                if (lcse.hash == rcse.hash && lcse != rcse) return false;
                if (lcse.conflict(rcse)) return false;
                if (lcse.intersect(rcse) && !(lcse.contain(rcse) || rcse.contain(lcse))) return false;
            }
        }
        return true;
    }

    boolean checkAAA(HashSet<SingleCse> lcses, Pair<Integer, Integer> lRange,
                     HashSet<SingleCse> rcses, Pair<Integer, Integer> rRange) {
        for (SingleCse lcse : lcses) {
            boolean intersect = false;
            for (Range range : lcse.ranges) {
                if (Math.max(range.left, rRange.getLeft()) <= Math.min(range.right, rRange.getRight())) {
                    intersect = true;
                    break;
                }
            }
            if (intersect != rcses.contains(lcse)) return false;
        }
        for (SingleCse rcse : rcses) {
            boolean intersect = false;
            for (Range range : rcse.ranges) {
                if (Math.max(range.left, lRange.getLeft()) <= Math.min(range.right, lRange.getRight())) {
                    intersect = true;
                    break;
                }
            }
            if (intersect != lcses.contains(rcse)) return false;
        }
        return true;
    }


    OperatorNode createOperatorNode(OperatorNode lNode, Pair<Integer, Integer> lRange,
                                    OperatorNode rNode, Pair<Integer, Integer> rRange,
                                    OperatorNode originNode, Pair<Integer, Integer> midRange) {
        if (lNode == null || rNode == null || originNode == null) return null;
        if ( // lNode.accCost >= Double.MAX_VALUE / 2 ||
            //   rNode.accCost >= Double.MAX_VALUE / 2 ||
                lNode.thisCost >= Double.MAX_VALUE / 2 ||
                        rNode.thisCost >= Double.MAX_VALUE / 2
        ) {
//            System.out.println("cost error");
//            System.out.println(originNode);
//            System.out.println(lNode);
//            System.out.println(rNode);
            System.exit(0);
        }

        if (lRange.equals(midRange) || rRange.equals(midRange)) {
//            System.out.println("chong fu ");
//            System.out.println(lNode);
//            System.out.println(rNode);
//            System.out.println(originNode);
            System.exit(0);
        }
        OperatorNode node = new OperatorNode();
        ArrayList<Integer> index = new ArrayList<>();
        index.add(lRange.getLeft());
        index.add(rRange.getLeft());
        index.add(rRange.getRight());
        node.dRange = new DRange(index);

        node.method = originNode.method;
        node.mmNode = originNode.mmNode;

        //   node.range = originNode.range; //Pair.of(lNode.range.getLeft(), rNode.range.getRight());
//        LOG.info(lNode.range + " " + rNode.range + " " + node.range +"\n"
//                +lNode.thisCostDetails+" "+lNode.accCostDetails+"\n"
//                +originNode.thisCostDetails+" "+originNode.accCostDetails+"\n"
//                +rNode.thisCostDetails+" "+rNode.accCostDetails+"\n"
//        );
        node.inputs.add(lNode);
        node.inputs.add(rNode);

        node.dependencies.addAll(originNode.dependencies);
        node.dependencies.addAll(lNode.dependencies);
        node.dependencies.addAll(rNode.dependencies);


        node.oldDependencies.addAll(originNode.oldDependencies);
        node.oldDependencies.addAll(lNode.oldDependencies);
        node.oldDependencies.addAll(rNode.oldDependencies);

        node.thisCost = originNode.thisCost;
        node.hops.addAll(originNode.hops);

        node.accCost = lNode.accCost + rNode.accCost + node.thisCost;

        node.thisCostDetails = originNode.thisCostDetails;

        node.accCostDetails = NodeCost.add(lNode.accCostDetails, originNode.thisCostDetails, rNode.accCostDetails);

        if (node.isXtXv) {
            if (!node.isTranspose) {
                node.accCost -= rNode.thisCost;
                node.accCostDetails = NodeCost.minus(node.accCostDetails, rNode.thisCostDetails);
            } else {
                node.accCost -= lNode.thisCost;
                node.accCostDetails = NodeCost.minus(node.accCostDetails, lNode.thisCostDetails);
            }
        }

//        if (node.accCost < Double.MAX_VALUE / 2 && Math.abs(node.accCost - node.accCostDetails.getSummary()) > 1000) {
//            System.out.println("acc cost error");
//            System.out.println(explainOpNode(node, 0));
//            System.exit(-117);
//        }

//        System.out.println(node);
//        System.out.println("cost : "+lNode.accCost+" "+rNode.accCost+" "+node.thisCost);
        return node;
    }


    public Triple<NodeCost, NodeCost, OperatorNode> estimateHopCost(Hop hop) {
        OperatorNode node = createOperatorGraph(hop, false);
        if (node == null) {
            return Triple.of(NodeCost.ZERO(), NodeCost.ZERO(), null);
        }
//        System.out.println(node);
        MutableInt mutableInt = new MutableInt(0);
        try {
            analyzeOperatorRange(node, new SingleCse(), mutableInt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        NodeCost constantCost = NodeCost.ZERO();
        NodeCost cost = analyzeHopCost(node, new HashSet<>(), constantCost);
//        System.out.println("all cost = "+cost);
        return Triple.of(cost, constantCost, node);
    }


    private NodeCost analyzeHopCost(OperatorNode node, HashSet<Hop> visited, NodeCost constantCost) {
//        System.out.println(node);
        boolean hasCons = false;
        for (Hop hop : node.hops) {
            if (visited.contains(hop)) {
                //  System.out.println("replicate: " + node);
                return NodeCost.ZERO();
            }
            if (hop.isConstant) {
                hasCons = true;
            }
        }
        NodeCost ans = NodeCost.ZERO();
//        System.out.println(explainOpNode(node,0));
//        System.out.println("x");
        NodeCost thisCostDetail = this.nodeCostEstimator.getNodeCost(node);
        if (hasCons) {
            thisCostDetail.multiply(1.0 / iterationNumber);
        }
        node.thisCostDetails = thisCostDetail;
        node.thisCost = thisCostDetail.getSummary();
//        LOG.info(node);
        for (int i = 0; i < node.inputs.size(); i++) {
            NodeCost tmp = analyzeHopCost(node.inputs.get(i), visited, constantCost);
            ans = NodeCost.add(ans, tmp);
        }
        ans = NodeCost.add(ans, thisCostDetail);
        if (hasCons) {
            constantCost.plus(ans);
        }
        visited.addAll(node.hops);
        return ans;
    }


}
