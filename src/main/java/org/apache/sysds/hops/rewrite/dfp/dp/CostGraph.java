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
import org.apache.sysds.hops.rewrite.dfp.Leaf;
import org.apache.sysds.hops.rewrite.dfp.coordinate.Range;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.sysds.utils.Statistics.*;

public class CostGraph {
    protected static final Log LOG = LogFactory.getLog(CostGraph.class.getName());

    public CostGraph(VariableSet variablesUpdated, long iterationNumber,ExecutionContext ec) {
        this.variablesUpdated = variablesUpdated;
        this.iterationNumber = iterationNumber;
        this.ec = ec;
        this.nodeCostEstimator = new NodeCostEstimator((SparkExecutionContext)ec);
    }

    private ExecutionContext ec;
    NodeCostEstimator nodeCostEstimator;
    long iterationNumber = 2;
    public VariableSet variablesUpdated = null;

    public static long estimateTime = 0;
    public static long dynamicProgramTime = 0;

    public ArrayList<OperatorNode> testOperatorGraph(ArrayList<SinglePlan> pairs,
                                                     Pair<SingleCse, Hop> emptyPair,
                                                     ArrayList<Range> blockRanges,
                                                     ArrayList<Leaf> leaves,
                                                     ArrayList<Hop> hops) {
        LOG.info("begin test Operator Graph");
        System.out.println("before build cost graph");
        System.out.println("Total JVM GC count:\t\t" + getJVMgcCount() + ".\n");
        System.out.println("Total JVM GC time:\t\t" + ((double) getJVMgcTime()) / 1000 + " sec.\n");

        int maxIndex = 0;
        HashSet<Pair<Integer, Integer>> ranges = new HashSet<>();

        OperatorNode emptyNode = createOperatorGraph(emptyPair.getRight(), false);
        emptyPair.getRight().resetVisitStatusForced(new HashSet<>());
        //  System.out.println(Explain.explain(emptyPair.getRight()));
        //explainOperatorNode(emptyNode,0);
        analyzeOperatorRange(emptyNode, emptyPair.getLeft(), new MutableInt(0));
        analyzeOperatorCostTemplate(emptyNode);
        rGetRanges(emptyNode, ranges);

        for (Hop hop : hops) {
            OperatorNode node = createOperatorGraph(hop, false);
            MutableInt mutableInt = new MutableInt(0);
            analyzeOperatorRange(node, emptyPair.getLeft(), mutableInt);
            analyzeOperatorCostTemplate(node);
//            LOG.info(CostGraph.explainOpNode(node,0));
        }

        for (SinglePlan p : pairs) {
            SingleCse cse = p.singleCse;
            Hop hop = p.hop;
            //    System.out.println("========================");
            //    System.out.println(cse);
//            System.out.println(Explain.explain(hop));
            OperatorNode node = createOperatorGraph(hop, false);
            MutableInt mutableInt = new MutableInt(0);
            analyzeOperatorRange(node, cse, mutableInt);
            boolean certainused = rCheckCertainUsed(cse, ranges);
            if (cse.isConstant) {
                p.tag = SinglePlan.SinglePlanTag.constant;
                //     System.out.println("Constant Cse: " + cse);
            } else {
                if (certainused) {
                    p.tag = SinglePlan.SinglePlanTag.Useful;
                    //        System.out.println("Certainly Useful: " + cse);
                    // continue;
                } else {
                    p.tag = SinglePlan.SinglePlanTag.uncertain;
                    //      System.out.println("Uncertain: " + cse);
                }
            }
            maxIndex = Math.max(maxIndex, mutableInt.getValue() - 1);
            analyzeOperatorConstant(node);
            analyzeOperatorCost(node);
//            LOG.info(explainOpNode(node, 0));
//            LOG.info(explainOpNodeJson(node,0));
            p.node = node;
        }

//        if (emptyPair.getRight().getName().equals("h")) {
//            System.exit(0);
//        }

        // 回收mnc使用的内存
        for (MMNode mmNode : nodeCostEstimator.range2mmnode.values()) {
            mmNode.setSynopsis(null);
        }
        nodeCostEstimator.range2mmnode.clear();

        System.gc();

        System.out.println("after build cost graph");
        System.out.println("Total JVM GC count:\t\t" + getJVMgcCount() + ".\n");
        System.out.println("Total JVM GC time:\t\t" + ((double) getJVMgcTime()) / 1000 + " sec.\n");

        CseStateMaintainer MAINTAINER = new CseStateMaintainer();
        MAINTAINER.initRangeCounter(range2acnode);
        MAINTAINER.initCseState(pairs);

        long start = System.nanoTime();
        LOG.info("start dp");
        ArrayList<OperatorNode> result = selectBest(MAINTAINER);
        //showBest(Pair.of(0, maxIndex));
        result.sort(Comparator.comparingDouble(a -> a.accCost));
        LOG.info("end dp");
        long end = System.nanoTime();
        dynamicProgramTime += end - start;

        System.out.println("after dynamic programming");
        System.out.println("Total JVM GC count:\t\t" + getJVMgcCount() + ".\n");
        System.out.println("Total JVM GC time:\t\t" + ((double) getJVMgcTime()) / 1000 + " sec.\n");

//        && list2.get(i).accCost <= bestsinglecsenode.accCost
//        for (int i = 0; i < 30 && i < result.size(); i++) {
//            System.out.println(result.get(i));
//        }

//        System.out.println(Explain.explain(emptyPair.getRight()));

//        System.out.println("done");

//        return range2acnode.get(Pair.of(0, maxIndex));
        LOG.info("end test Operator Graph");

//        if (emptyPair.getRight().getName().equals("h"))
//            System.exit(0);
        return result;
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
        ranges.add(node.range);
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
//        node.thisCost = NodeCostEstimator.getNodeCost(node);
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

    void analyzeOperatorRange(OperatorNode root, SingleCse cse, MutableInt opIndex) {
        int begin = opIndex.getValue();
//        if (root==null||root.inputs==null) {
//            System.out.println(root);
//            System.exit(-4);
//        }
        if (root.inputs.size() > 0) {
            for (int i = 0; i < root.inputs.size(); i++) {
                analyzeOperatorRange(root.inputs.get(i), cse, opIndex);
                //  root.dependencies.addAll(root.inputs.get(i).dependencies);
            }
        } else {
            opIndex.increment();
        }
        int end = opIndex.getValue() - 1;
        if (root.range == null) {
            root.range = Pair.of(begin, end);
//        System.out.println("analyze range: " + root.range);
            for (Range range : cse.ranges) {
                if ((range.left == begin && range.right == end) || (range.left == end && range.right == begin)) {
                    root.dependencies.add(cse);
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
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorCostTemplate(node.inputs.get(i));
        }
        long start = System.nanoTime();
        NodeCost thisCostDetail = this.nodeCostEstimator.getNodeCost(node);
        long end = System.nanoTime();
        estimateTime += end - start;
        node.thisCost = thisCostDetail.getSummary();
        node.thisCostDetails = thisCostDetail;
        if (!range2acnode.containsKey(node.range)) {
            ACNode acNode = new ACNode();
            acNode.range = node.range;
            acNode.emptyOpnode = node;
            range2acnode.put(node.range, acNode);
        }
    }

    void analyzeOperatorCost(OperatorNode node) {
        if (node.inputs.size() == 0) {
            node.accCost = 0;
            node.accCostDetails = NodeCost.ZERO();
        }
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorCost(node.inputs.get(i));
        }

        long start = System.nanoTime();
        NodeCost thisCostDetail = this.nodeCostEstimator.getNodeCost(node);
        long end = System.nanoTime();
        estimateTime += end - start;

        int csesize = 1;
        for (SingleCse singleCse : node.dependencies) {
            for (int i = 0; i < singleCse.ranges.size(); i++) {
                if (singleCse.ranges.get(i).left == node.range.getLeft()
                        && singleCse.ranges.get(i).right == node.range.getRight()) {
                    csesize = singleCse.ranges.size();
                    break;
                }
            }
        }
        if (csesize > 0) {
//            thisCost = thisCost / csesize;
            thisCostDetail.multiply(1.0 / csesize);
            //   accCost = accCost / csesize;
        }
        if (node.isConstant) {
            thisCostDetail.multiply(1.0 / iterationNumber);
        }
        //  accCost += thisCost;
        //  node.accCost = accCost;
        node.thisCostDetails = thisCostDetail;
        node.thisCost = thisCostDetail.getSummary();
        // if (node.range.getLeft()==2&&node.range.getRight()==3)   System.out.println(thisCost);
        //  System.out.println(node);
        range2acnode.get(node.range).addOperatorNode(node);
//        if (node.range.getLeft()==2&&node.range.getRight()==3) {
//            System.out.println("node(2,3): "+ node);
//        }
//        System.out.println("add node "+node.range+" "+node);
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

    ArrayList<OperatorNode> selectBest(CseStateMaintainer MAINTAINER) {
//        dp = new HashMap<>();
        ArrayList<Pair<Integer, Integer>> sortedRanges = new ArrayList<>(range2acnode.keySet());
//        ranges.sort(Comparator.comparingInt((Pair<Integer,Integer> a)->(a.getRight()-a.getLeft())));
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

            for (Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> drange : acNode.drange2operatornodes.keySet()) {
                Pair<Integer, Integer> lRange = drange.getLeft();
                Pair<Integer, Integer> rRange = drange.getRight();
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

                Stream<Triple<OperatorNode, OperatorNode, OperatorNode>> tmp3 = lops
                        .parallelStream()
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
                        });

                List<OperatorNode> tmp = tmp3
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

        for (Pair<Integer, Integer> range : sortedRanges) {
            if (!range2acnode.containsKey(range)) {
//                System.out.println(range + " " + 0);
            } else {
//                System.out.println(range + " " + range2acnode.get(range).uncertainACs.size());
            }
        }
        if (sortedRanges.size() > 0) {
            ArrayList<OperatorNode> opnodess = range2acnode.get(sortedRanges.get(sortedRanges.size() - 1)).getOperatorNodes(MAINTAINER);
            return opnodess;
        } else {
            return null;
        }
    }


    boolean check(OperatorNode operatorNode1,
                  OperatorNode operatorNode2,
                  HashSet<SingleCse> midcses) {

        if (!checkConflict(operatorNode1.dependencies, operatorNode2.dependencies)) return false;
        if (!checkAAA(operatorNode1.dependencies, operatorNode1.range, operatorNode2.dependencies, operatorNode2.range))
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

        if (!checkAAA(operatorNode1.oldDependencies, operatorNode1.range, operatorNode2.oldDependencies, operatorNode2.range))
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
        node.range = midRange;
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
                node.accCostDetails.minus(rNode.thisCostDetails);
            } else {
                node.accCost -= lNode.thisCost;
                node.accCostDetails.minus(lNode.thisCostDetails);
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


    public Triple<NodeCost,NodeCost, OperatorNode> estimateHopCost(Hop hop) {
        OperatorNode node = createOperatorGraph(hop, false);
        System.out.println(node);
        MutableInt mutableInt = new MutableInt(0);
        analyzeOperatorRange(node, new SingleCse(), mutableInt);
        NodeCost constantCost = NodeCost.ZERO();
        long start = System.nanoTime();
        NodeCost cost = analyzeHopCost(node, new HashSet<>(), constantCost);
        long end = System.nanoTime();
        estimateTime += end - start;
//        System.out.println("all cost = "+cost);
        //    NodeCost cost = NodeCost.ZERO();
        return Triple.of(cost,constantCost, node);
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
