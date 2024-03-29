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
import org.apache.sysds.hops.rewrite.dfp.coordinate.Coordinate;
import org.apache.sysds.hops.rewrite.dfp.coordinate.MultiCse;
import org.apache.sysds.hops.rewrite.dfp.coordinate.Range;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;
import org.apache.sysds.hops.rewrite.dfp.costmodel.CostModelCommon;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;

import java.util.*;
import java.util.stream.Collectors;


public class CostGraph {
    protected static final Log LOG = LogFactory.getLog(CostGraph.class.getName());

    public CostGraph(VariableSet variablesUpdated, long iterationNumber, ExecutionContext ec,Coordinate coordinate) {
        this.variablesUpdated = variablesUpdated;
        this.iterationNumber = iterationNumber;
        this.ec = ec;
        this.nodeCostEstimator = new NodeCostEstimator((SparkExecutionContext) ec);
        this.coordinate = coordinate;
    }

    private ExecutionContext ec;
    public final NodeCostEstimator nodeCostEstimator;
    long iterationNumber = 2;
    public VariableSet variablesUpdated = null;
    private Coordinate coordinate = null;
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
        analyzeOperatorConstantTemplate(emptyNode);

        for (SinglePlan p : placePlans) {
            OperatorNode node = createOperatorGraph(p.hop, false);
            analyzeOperatorRange(node, emptyCse, new MutableInt(0));
            analyzeOperatorConstantTemplate(node);
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
//                        cseNodes.add(Pair.of(root.dRange, range));
                        break;
                    }
                }
            }
        }
    }


    void analyzeOperatorConstantTemplate(OperatorNode node) {
        if (variablesUpdated == null) return;
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorConstantTemplate(node.inputs.get(i));
        }
        if (node.inputs.size()>0) {
            node.isConstant = coordinate.isConstant(node.dRange.getLeft(), node.dRange.getLeft());
            if (node.isConstant) {
                node.hops.get(0).isConstant = true;
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
                        thisCostDetail.divide( csesize);
                    }
                    if (node.isConstant) {
                        thisCostDetail.divide( iterationNumber);
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
                if (acNode.certainAC == null || node.lessThan(acNode.certainAC)) {
                    acNode.certainAC = node;
                }
            }
            if (acNode.minAC == null || node.lessThan(acNode.minAC)) {
                acNode.minAC = node;
            }
        }
//        System.out.println(acNode.range + " remove " + removed1 + " " + removed2);
    }

    void classifyOperatorNodeParallel(CseStateMaintainer MAINTAINER, ArrayList<OperatorNode> allResults, ACNode acNode) {
        long start = System.nanoTime();
        acNode.certainAC = allResults
                .parallelStream()
                .filter(node -> !MAINTAINER.hasUncertain(node.dependencies))
                .reduce(null, (node1, node2) -> {
                    if (node1 == null) return node2;
                    if (node2 == null) return node1;
                    if (node1.lessThan(node2)) return node1;
                    return node2;
                });

        acNode.minAC = allResults
                .parallelStream()
                .reduce(null, (node1, node2) -> {
                    if (node1 == null) return node2;
                    if (node2 == null) return node1;
                    if (node1.lessThan(node2)) return node1;
                    return node2;
                });

        acNode.uncertainACs = new HashMap<>();
        allResults
                .stream()
                .filter(node -> MAINTAINER.hasUncertain(node.dependencies))
                .forEach(node -> {
                        acNode.addUncertainAC(node);
                });
        long end = System.nanoTime();
        classifytime += end - start;
    }
   public static  long classifytime = 0;

    public static boolean parallelDynamicProgramming = true;
    public static int all_old_combine_num = 0;
    public static int all_new_combine_num = 0;

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

                Collection<Pair<ArrayList<OperatorNode>, ArrayList<OperatorNode>>> groupedOps =
                        groupOperatorNodes(lops,rops,lRange,rRange,boundery);

                LOG.info(" groupby size: "+groupedOps.size());
                int old_combine_number = lops.size()* rops.size()*mids.size();
                int new_combine_number = groupedOps
                        .stream()
                        .mapToInt(pair->pair.getLeft().size()*pair.getRight().size())
                        .sum() * mids.size();
                LOG.info("combine num: "+old_combine_number+" -> "+new_combine_number);
                all_old_combine_num += old_combine_number;
                all_new_combine_num += new_combine_number;

                List<OperatorNode> tmp = groupedOps
                        .parallelStream()
                        .flatMap(pair -> {
                            ArrayList<OperatorNode> llops = pair.getLeft();
                            ArrayList<OperatorNode> rrops = pair.getRight();
                            ArrayList<Triple<OperatorNode, OperatorNode, OperatorNode>> arrayList = new ArrayList<>();
                            for (OperatorNode mid : mids) {
                                for (OperatorNode lop : llops) {
                                    for (OperatorNode rop : rrops) {
                                        if (check(lop, rop, mid.dependencies)) {
                                            arrayList.add(Triple.of(lop, mid, rop));
                                        }
                                    }
                                }
                            }
                            return arrayList.stream();
                        })
                        .map(triple -> createOperatorNode(triple.getLeft(), lRange, triple.getRight(), rRange, triple.getMiddle(), boundery))
                        .filter(Objects::nonNull)
                        .filter(node -> !MAINTAINER.hasUselessCse(node.dependencies))
                        .peek(node -> {
                            for (Iterator<SingleCse> cseIterator = node.dependencies.iterator(); cseIterator.hasNext(); ) {
                                SingleCse cse = cseIterator.next();
                                if (MAINTAINER.getCseState(cse) == CseStateMaintainer.CseState.certainlyUseful) {
                                    node.oldDependencies.add(cse);
                                    cseIterator.remove();
                                }
                            }
                        })
                        .collect(Collectors.toList());
                allResults.addAll(tmp);
            }

//            classifyOperatorNode(MAINTAINER, allResults, acNode);
            classifyOperatorNodeParallel(MAINTAINER,allResults,acNode);

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

    HashSet<SingleCse> liftKey(OperatorNode node,Pair<Integer, Integer> targetRange,Pair<Integer, Integer> boundery) {
        HashSet<SingleCse> ans = new HashSet<>();
        for (SingleCse singleCse: node.dependencies) {
            ArrayList<Range> ranges = new ArrayList<>();
            for (Range range: singleCse.ranges) {
                if (Math.max(range.left,targetRange.getLeft())<=Math.min(range.right,targetRange.getRight())) {
                    ranges.add(range);
                }
            }
            if (!ranges.isEmpty()) {
                ranges.clear();
                for (Range range: singleCse.ranges) {
                    if (Math.max(range.left,boundery.getLeft())<=Math.min(range.right,boundery.getRight())) {
                        ranges.add(range);
                    }
                }
                SingleCse singleCse1 = new SingleCse();
                singleCse1.hash = singleCse.hash;
                singleCse1.name = singleCse.name;
                singleCse1.isConstant = singleCse.isConstant;
                singleCse1.ranges = ranges;
                ans.add(singleCse1);
            }
        }
        return ans;
    }

    public static   long grouptime = 0;

    Collection<Pair<ArrayList<OperatorNode>, ArrayList<OperatorNode>>> groupOperatorNodes(ArrayList<OperatorNode> lops,
                                                                                          ArrayList<OperatorNode> rops,
                                                                                          Pair<Integer, Integer> lRange,
                                                                                          Pair<Integer, Integer> rRange,
                                                                                          Pair<Integer, Integer> boundery) {
        long start = System.nanoTime();
        HashMap<HashSet<SingleCse>, Pair<ArrayList<OperatorNode>, ArrayList<OperatorNode>>> joinTable = new HashMap<>();
        for (OperatorNode lop : lops) {
            HashSet<SingleCse> key = liftKey(lop, rRange, boundery);
            joinTable.putIfAbsent(key, Pair.of(new ArrayList<>(), new ArrayList<>()));
            joinTable.get(key).getLeft().add(lop);
        }
        for (OperatorNode rop : rops) {
            HashSet<SingleCse> key = liftKey(rop, lRange, boundery);
            joinTable.putIfAbsent(key, Pair.of(new ArrayList<>(), new ArrayList<>()));
            joinTable.get(key).getRight().add(rop);
        }
        long end = System.nanoTime();
        grouptime += end - start;

        return joinTable.values();
    }

    boolean check(OperatorNode operatorNode1,
                  OperatorNode operatorNode2,
                  HashSet<SingleCse> midcses) {
        if (isConflict(operatorNode1.dependencies, operatorNode2.dependencies)) return false;
        if (isConflict(midcses, operatorNode1.dependencies)) return false;
        if (isConflict(midcses, operatorNode2.dependencies)) return false;
        if (isConflict(operatorNode1.oldDependencies, operatorNode2.oldDependencies)) return false;
        if (isConflict(midcses, operatorNode1.oldDependencies)) return false;
        if (isConflict(midcses, operatorNode2.oldDependencies)) return false;
        if (!isCompatible(operatorNode1.dependencies, operatorNode1.dRange.getRange(),
                operatorNode2.dependencies, operatorNode2.dRange.getRange()))
            return false;
        if (!isCompatible(operatorNode1.oldDependencies, operatorNode1.dRange.getRange(),
                operatorNode2.oldDependencies, operatorNode2.dRange.getRange()))
            return false;
        return true;
    }


    boolean isConflict(HashSet<SingleCse> singleCses1, HashSet<SingleCse> singleCses2) {
        if (singleCses1.isEmpty() || singleCses2.isEmpty()) return false;
        for (SingleCse lcse : singleCses1) {
            if (lcse.ranges.size() == 0) continue;
            for (SingleCse rcse : singleCses2) {
                if (rcse.ranges.size() == 0) continue;
                if (lcse.hash == rcse.hash && lcse != rcse) return true;
                if (lcse.conflict(rcse)) return true;
                if (lcse.intersect(rcse) && !(lcse.contain(rcse) || rcse.contain(lcse))) return true;
            }
        }
        return false;
    }

    boolean isCompatible(HashSet<SingleCse> lcses, Pair<Integer, Integer> lRange,
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


    public Triple<NodeCost, NodeCost, OperatorNode> estimateHopCost(Hop hop, MultiCse multiCse) {
        OperatorNode node = createOperatorGraph(hop, false);
        if (node == null) {
            return Triple.of(NodeCost.ZERO(), NodeCost.ZERO(), null);
        }

        analyzeOperatorRange_a(node, multiCse, new MutableInt(0));

        NodeCost constantCost = NodeCost.ZERO();
        NodeCost allCost = NodeCost.ZERO();
        CostModelCommon.MMShowCostFlag = true;
        analyzeHopCost_a(allCost, constantCost, node);

        return Triple.of(allCost, constantCost, node);
    }


    void analyzeOperatorRange_a(OperatorNode root, MultiCse multiCse, MutableInt opIndex) {
        HashMap<SingleCse, HashSet<Pair<DRange, Range>>> cseNodesMap = new HashMap<>();
        analyzeOperatorRangeInner_a(root, multiCse, opIndex, cseNodesMap);
    }


    void analyzeOperatorRangeInner_a(OperatorNode root,
                                     MultiCse multiCse,
                                     MutableInt opIndex,
                                     HashMap<SingleCse, HashSet<Pair<DRange, Range>>> cseNodesMap) {
        ArrayList<Integer> index = new ArrayList<>();
        int begin = opIndex.getValue();
        if (root.inputs.size() > 0) {
            for (int i = 0; i < root.inputs.size(); i++) {
                index.add(opIndex.getValue());
                analyzeOperatorRangeInner_a(root.inputs.get(i), multiCse, opIndex, cseNodesMap);
            }
        } else {
            opIndex.increment();
        }
        int end = opIndex.getValue() - 1;
        index.add(end);
        if (root.dRange == null) {
            root.dRange = new DRange(index);
            for (SingleCse cse : multiCse.cses) {
//                HashSet<Pair<DRange, Range>> cseNodes = cseNodesMap.getOrDefault(cse, new HashSet<>());
                for (Range range : cse.ranges) {
                    if ((range.left == begin && range.right == end) ||
                            (range.left == end && range.right == begin)) {
                        root.dependencies.add(cse);
                        root.dRange.cseRangeTransposeType = range.transpose;
//                        cseNodes.add(Pair.of(root.dRange, range));
                        break;
                    }
                }
//                cseNodesMap.put(cse, cseNodes);
            }
        }
    }


    private void analyzeHopCost_a(NodeCost allCost, NodeCost constantCost, OperatorNode node) {
        if (node.inputs.size() == 0) {
            node.thisCostDetails = NodeCost.ZERO();
            return;
        }
        NodeCost thisCostDetail = this.nodeCostEstimator.getNodeCost(node);
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeHopCost_a(allCost, constantCost, node.inputs.get(i));
        }
        boolean constant = false;
        int csesize = 1;
        for (SingleCse singleCse : node.dependencies) {
            csesize = singleCse.ranges.size();
            if (singleCse.isConstant) constant = true;
        }
        if (csesize > 0) {
            thisCostDetail.divide(csesize);
        }
        if (constant) {
            thisCostDetail.divide(iterationNumber);
            constantCost.plus(thisCostDetail);
        }
        allCost.plus(thisCostDetail);
        node.thisCostDetails = thisCostDetail;
    }

    public Triple<NodeCost, NodeCost, OperatorNode> estimateHopCost_b(Hop hop) {
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
        NodeCost cost = analyzeHopCost_b(node, new HashSet<>(), constantCost);
//        System.out.println("all cost = "+cost);
        return Triple.of(cost, constantCost, node);
    }

    private NodeCost analyzeHopCost_b(OperatorNode node, HashSet<Hop> visited, NodeCost constantCost) {
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
            thisCostDetail.divide(iterationNumber);
        }
        node.thisCostDetails = thisCostDetail;
        node.thisCost = thisCostDetail.getSummary();
//        LOG.info(node);
        for (int i = 0; i < node.inputs.size(); i++) {
            NodeCost tmp = analyzeHopCost_b(node.inputs.get(i), visited, constantCost);
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
