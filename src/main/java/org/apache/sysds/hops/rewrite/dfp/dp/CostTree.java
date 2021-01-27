package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.BinaryOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.coordinate.Range;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.runtime.matrix.operators.Operator;
import scala.Int;

import java.util.*;

public class CostTree {

    public CostTree(VariableSet variablesUpdated) {
        this.variablesUpdated = variablesUpdated;
    }

    public VariableSet variablesUpdated = null;

    // HashMap<Pair<Integer, Integer>, ArrayList<OperatorNode>> range2OperatoeNode = new HashMap<>();


    public void testOperatorGraph(ArrayList<Pair<SingleCse, Hop>> pairs) {
        ArrayList<OperatorNode> list = new ArrayList<>();
        int maxIndex = 0;
        for (Pair<SingleCse, Hop> p : pairs) {
            //  HopCostEstimator.buildMMNodeTree(p.getRight());
            // System.out.println(Explain.explain(p.getRight()));
            OperatorNode node = createOperatorGraph(p.getRight(), false);
            MutableInt mutableInt = new MutableInt(0);
            analyzeOperatorRanges(node, p.getLeft(), mutableInt);
            //  System.out.println("mutableInt=" + mutableInt.getValue());
            maxIndex = Math.max(maxIndex, mutableInt.getValue() - 1);
            analyzeOperatorConstant(node);
            analyzeOperatorCost(node, new HashSet<>());
            addOperatorNodeToTable(node, new HashSet<>());
            list.add(node);
            //   explain(node, 0);
            System.out.println("========================");
//            System.out.println(node);
        }

//        for (Map.Entry<Pair<Integer,Integer>,ArrayList<OperatorNode>> e: range2OperatoeNode.entrySet()) {
//            System.out.println(e.getKey());
//            System.out.println(e.getValue().size());
//        }

//        System.exit(0);

        filterOperatorNode0();
        selectBest();

//        for (Pair<Integer, Integer> range : range2OperatoeNode.keySet()) {
//            showBest(range);
//        }

        showBest(Pair.of(0, maxIndex));
        System.out.println("done");
    }

    void showBest(Pair<Integer, Integer> range) {
        System.out.println("range: " + range);
        ArrayList<OperatorNode> list2 = new ArrayList<>();
        //            if (e.getValue().dependencies.size() > 2) {
        //                System.out.println("dependencies size > 2");
        //            }
        list2.addAll(range2acnode.get(range).uncertainACs.values());
        list2.sort(Comparator.comparingDouble(a -> a.accCost));
        for (int i = 0; i < 200 && i < list2.size(); i++) {
            System.out.println(list2.get(i));
        }

    }


    void explain(OperatorNode node, int d) {
        for (int i = 0; i < d; i++) System.out.print(" ");
        System.out.print("{");
        System.out.print(node);
//        if (Judge.isRead(node.hop)) System.out.print(node.hop.getName()+" ");
//        System.out.print(node.isConstant);
        for (int i = 0; i < node.inputs.size(); i++) {
            explain(node.inputs.get(i), d + 1);
        }
        for (int i = 0; i < d; i++) System.out.print(" ");
        System.out.println("}");
    }


    OperatorNode createOperatorGraph(Hop hop, boolean transpose) {
//        if (mp.containsKey(hop)) {
//            return mp.get(hop);
//        }
        OperatorNode node = null;  //= new OperatorNode();
        if (Judge.isWrite(hop)) {
            node = createOperatorGraph(hop.getInput().get(0), transpose);
        } else if (Judge.isLeafMatrix(hop)) {
            node = new OperatorNode();
        } else if (HopRewriteUtils.isTransposeOperation(hop)) {
            node = createOperatorGraph(hop.getInput().get(0), !transpose);
        } else if (hop instanceof LiteralOp) {
            return null;
        } else if (HopRewriteUtils.isUnary(hop, Types.OpOp1.CAST_AS_SCALAR)) {
            node = createOperatorGraph(hop.getInput().get(0), transpose);
        } else {
            ArrayList<OperatorNode> tmpNodes = new ArrayList<>();
            if (!transpose) {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    OperatorNode tmp = createOperatorGraph(hop.getInput().get(i), transpose);
                    if (tmp == null) continue;
                    tmpNodes.add(tmp);
                    // node.accCost += tmp.accCost;
                }
            } else {
                for (int i = hop.getInput().size() - 1; i >= 0; i--) {
                    OperatorNode tmp = createOperatorGraph(hop.getInput().get(i), transpose);
                    if (tmp == null) continue;
                    tmpNodes.add(tmp);
                    //   node.accCost += tmp.accCost;
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
        }
//        node.thisCost = NodeCostEstimator.getNodeCost(node);
        // System.out.println("put " + node);
        if (node != null) {
            if (node.hops.size() == 0 || !node.hops.contains(hop)) {
                node.hops.add(hop);
            }
        }
        return node;
    }

    void analyzeOperatorRanges(OperatorNode root, SingleCse cse, MutableInt opIndex) {
        int begin = opIndex.getValue();
        if (root.inputs.size() > 0) {
            for (int i = 0; i < root.inputs.size(); i++) {
                analyzeOperatorRanges(root.inputs.get(i), cse, opIndex);
                root.dependencies.addAll(root.inputs.get(i).dependencies);
            }
        } else {
            opIndex.increment();
        }
        int end = opIndex.getValue() - 1;
        root.range = Pair.of(begin, end);

        for (Range range : cse.ranges) {
            if ((range.left == begin && range.right == end) || (range.left == end && range.right == begin)) {
                root.dependencies.add(cse);
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

    void analyzeOperatorCost(OperatorNode node, HashSet<OperatorNode> visited) {
        if (visited.contains(node)) return;
        node.accCost = 0;
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorCost(node.inputs.get(i), visited);
            node.accCost += node.inputs.get(i).accCost;
        }
        node.thisCost = NodeCostEstimator.getNodeCost(node);
        node.accCost += node.thisCost;
//        if (node.ranges.size() > 0) {
//            node.thisCost = node.thisCost / node.ranges.size();
//            node.accCost = node.accCost / node.ranges.size();
//        }
        if (node.isConstant) {
            node.thisCost /= 100;
            node.accCost /= 100;
        }
        System.out.println(node);
        visited.add(node);
    }

    void addOperatorNodeToTable(OperatorNode node, HashSet<OperatorNode> visited) {
        // node.accCost = node.thisCost;
        if (node.inputs.size() == 0) {
            Pair<Integer, Integer> range = node.range;
            if (!range2acnode.containsKey(range)) {
                ACNode acNode = new ACNode();
                acNode.range = range;
                acNode.operatorNodes.add(node);
                range2acnode.put(range, acNode);
            }
        } else {
            for (int i = 0; i < node.inputs.size(); i++) {
                addOperatorNodeToTable(node.inputs.get(i), visited);
            }
            Pair<Integer, Integer> range = node.range;
            if (range2acnode.containsKey(range)) {
                ACNode acNode = range2acnode.get(range);
                acNode.operatorNodes.add(node);
            } else {
                ACNode acNode = new ACNode();
                acNode.range = range;
                acNode.operatorNodes.add(node);
                range2acnode.put(range, acNode);
            }
        }
//     if (!visited.contains(node)) {
//        ArrayList<OperatorNode> nodes;
//        if (range2OperatoeNode.containsKey(range)) {
//            nodes = range2OperatoeNode.get(range);
//        } else {
//            nodes = new ArrayList<>();
//        }
//        nodes.add(node);
//        range2OperatoeNode.put(range, nodes);
        //   }
        visited.add(node);
    }


    public double estimateCost(Hop hop) {
        HopCostEstimator.buildMMNodeTree(hop);
        return HopCostEstimator.estimate(hop);
//        double cost;
//        if (HopRewriteUtils.isMatrixMultiply(hop)) {
//            Hop l = hop.getInput().get(0);
//            Hop r = hop.getInput().get(1);
//            cost = l.getDim1() * l.getDim2() * r.getDim2();
//        } else {
//            cost = hop.getDim1() * hop.getDim2();
//        }
//        return cost;
    }

    boolean isCse(Integer begin, Integer end, SingleCse singleCse) {
        boolean isCse = false;
        for (Range range : singleCse.ranges) {
            if ((range.left == begin && range.right == end) ||
                    (range.left == end && range.right == begin)) {
                isCse = true;
            }
        }
        return isCse;
    }

    void filterOperatorNode0() {
        for (Pair<Integer, Integer> range : range2acnode.keySet()) {
            ArrayList<OperatorNode> a1 = range2acnode.get(range).operatorNodes;
            if (a1.size() < 2) continue;
            OperatorNode operatorNode = a1.get(0);
            if (operatorNode.hops.size()<1) continue;
            Hop hop = operatorNode.hops.get(0);
            if (hop instanceof BinaryOp) {
                operatorNode.dependencies = new HashSet<>();
                range2acnode.get(range).operatorNodes.clear();
                range2acnode.get(range).operatorNodes.add(operatorNode);
                continue;
            }
            ArrayList<OperatorNode> a2 = new ArrayList<>();
            for (OperatorNode node1 : a1) {
                boolean ok = true;
                for (OperatorNode node2 : a2) {
                    if (node1.dependencies.equals(node2.dependencies)) {
                        ok = false;
                        break;
                    }
                }
                if (ok) a2.add(node1);
            }
            System.out.println("range=" + range + " size(a1)=" + a1.size() + " size(a2)=" + a2.size());
            a2.sort(Comparator.comparing(c -> c.accCost));
            range2acnode.get(range).operatorNodes = a2;
        }
        //  System.exit(0);
    }

    void filterOperatorNode2() {
        for (Pair<Integer, Integer> range : range2acnode.keySet()) {
            ArrayList<OperatorNode> a1 = range2acnode.get(range).operatorNodes;
            if (a1.size() < 2) continue;
            HashMap<Pair< Pair<Integer, Integer>, Pair<Integer, Integer>>,OperatorNode> map = new HashMap<>();
            for (OperatorNode node: a1) {
                if (node.inputs.size()!=2) continue;
                Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> key = Pair.of(node.inputs.get(0).range,node.inputs.get(1).range);
                if (!map.containsKey(key)) {
                    map.put(key,node);
                }else {
                    if (map.get(key).accCost>node.accCost) {
                        map.put(key,node);
                    }
                }
            }
            System.out.println("range=" + range + " size(a1)=" + a1.size() + " size(a2)=" + map.size());
            range2acnode.get(range).operatorNodes = new ArrayList<OperatorNode>(map.values());
        }
        //  System.exit(0);
    }

    void filterOperatorNode3() {
        for (Pair<Integer, Integer> range : range2acnode.keySet()) {
            ArrayList<OperatorNode> a1 = range2acnode.get(range).operatorNodes;
            if (a1.size() < 2) continue;
            ArrayList<OperatorNode> a2 = new ArrayList<>();
            for (OperatorNode node1 : a1) {
                boolean ok = true;
                for (OperatorNode node2 : a2) {
                    if (node1.dependencies.equals(node2.dependencies)) {
                        ok = false;
                        break;
                    }
                }
                if (ok) a2.add(node1);
            }
            System.out.println("range=" + range + " size(a1)=" + a1.size() + " size(a2)=" + a2.size());
            a2.sort(Comparator.comparing(c -> c.accCost));
            ArrayList<OperatorNode> a3=new ArrayList<>();
            for (int i=0;i<a2.size()&&i<20;i++) a3.add(a2.get(i));
            range2acnode.get(range).operatorNodes = a3;
        }
        //  System.exit(0);
    }

    void filterOperatorNode4() {
        for (Pair<Integer, Integer> range : range2acnode.keySet()) {
            ArrayList<OperatorNode> a1 = range2acnode.get(range).operatorNodes;
            if (a1.size() < 2) continue;
            ArrayList<OperatorNode> a2 = new ArrayList<>();
            for (OperatorNode node1 : a1) {
                boolean ok = true;
                Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> key1 = Pair.of(node1.inputs.get(0).range,node1.inputs.get(1).range);
                for (OperatorNode node2 : a2) {
                    Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> key2 = Pair.of(node2.inputs.get(0).range,node2.inputs.get(1).range);
                    if (key1.equals(key2)) {
                        if (node1.accCost>node2.accCost) {
                            ok=false;
                        }
                    }
                }
                if (ok) a2.add(node1);
            }
            System.out.println("range=" + range + " size(a1)=" + a1.size() + " size(a2)=" + a2.size());
            a2.sort(Comparator.comparing(c -> c.accCost));
            range2acnode.get(range).operatorNodes = a2;
        }
        //  System.exit(0);
    }


//    HashMap<Pair<Integer, Integer>, HashMap<HashSet<SingleCse>, OperatorNode>> dp;

    HashMap<Pair<Integer, Integer>, ACNode> range2acnode = new HashMap<>();

    Counter<Pair<Integer, Integer>> rangeCounter = new Counter<>();
    Counter<SingleCse> cseCounter = new Counter<>();

    void initRangeCounter() {
        for (ACNode acNode : range2acnode.values()) {
            for (OperatorNode node : acNode.operatorNodes) {
                for (OperatorNode in : node.inputs) {
                    rangeCounter.increment(in.range);
                }
            }
        }
    }


    void selectBest() {
        initRangeCounter();
//        dp = new HashMap<>();
        ArrayList<Pair<Integer, Integer>> ranges = new ArrayList<>(range2acnode.keySet());
      //  ranges.sort(Comparator.comparingInt((Pair<Integer,Integer> a)->(a.getRight()-a.getLeft())));
        ranges.sort(Comparator.comparingInt((Pair<Integer, Integer> a) -> (a.getRight() - a.getLeft())).thenComparingInt(Pair::getLeft));
        System.out.println(ranges);
        for (Pair<Integer, Integer> boundery : ranges) {
         //   if (boundery.getRight()-boundery.getLeft()>2) break;
            ACNode acNode = range2acnode.get(boundery);
            ArrayList<OperatorNode> operatorNodes = acNode.operatorNodes;
            ArrayList<OperatorNode> allResults = new ArrayList<>();
            for (OperatorNode operatorNode : operatorNodes) {
                //  System.out.println("Operator Node " + operatorNode.range);
                if (operatorNode.inputs.size() == 2) {
                    Pair<Integer, Integer> lRange = operatorNode.inputs.get(0).range;
                    Pair<Integer, Integer> rRange = operatorNode.inputs.get(1).range;
                   // if (lRange.getRight() + 1 != rRange.getLeft()) continue;
                    if (lRange == null || rRange == null) continue;
                    if (lRange.equals(boundery) || rRange.equals(boundery)) continue;
                    ACNode lac = range2acnode.get(lRange);
                    ACNode rac = range2acnode.get(rRange);
                    if (lac == null || rac == null) continue;
                    OperatorNode tmp = createOperatorNode(lac.certainAC, lRange, rac.certainAC, rRange, operatorNode, operatorNode.range);
                    if (tmp != null) allResults.add(tmp);
                    ArrayList<OperatorNode> lops = new ArrayList<>();
                    ArrayList<OperatorNode> rops = new ArrayList<>();
                    lops.addAll(lac.uncertainACs.values());
                    lops.addAll(lac.operatorNodes);
                    rops.addAll(rac.uncertainACs.values());
                    rops.addAll(rac.operatorNodes);
                  //  System.out.println("lRange:" + lRange + " rRange:" + rRange + " lops:" + lops.size() + " rops:" + rops.size());
                    for (OperatorNode operatorNode1 : lops) {
                        for (OperatorNode operatorNode2 : rops) {
                            if (check(lRange, operatorNode1.dependencies, rRange, operatorNode2.dependencies, operatorNode.dependencies)) {
                                tmp = createOperatorNode(operatorNode1, lRange, operatorNode2, rRange, operatorNode, boundery);
                                if (tmp != null) allResults.add(tmp);
                            }
                        }
                    }
                }
            }
            acNode.uncertainACs = new HashMap<>();
            int removed = 0;
            for (OperatorNode node : allResults) {
                boolean ok = true;
                for (SingleCse cse: node.dependencies) {
                    if (cseCounter.getValue(cse)==0) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    acNode.addUncertainAC(node);
                }else {
                    removed++;
                }

                if (acNode.minAC == null || acNode.minAC.accCost > node.accCost) {
                    acNode.minAC = node;
                }
            }
            System.out.println( boundery+ " counter remove "+removed);

            if (acNode.minAC != null) {
                for (SingleCse cse : acNode.minAC.dependencies) {
                    cseCounter.increment(cse);
                }
            }
            for (OperatorNode operatorNode : operatorNodes) {
                for (OperatorNode node : operatorNode.inputs) {
                    rangeCounter.decrement(node.range);
                    if (rangeCounter.getValue(node.range) == 0) {
                        ACNode acNode1 = range2acnode.get(node.range);
                        if (acNode1.minAC != null) {
                            for (SingleCse cse : acNode1.minAC.dependencies) {
                                cseCounter.decrement(cse);
                            }
                        }
                    }
                }
            }


            if (!range2acnode.containsKey(boundery)) {
                range2acnode.put(boundery, new ACNode());
            } else {
                System.out.println("boundery="+boundery + " = " + range2acnode.get(boundery).uncertainACs.size());
              //  System.out.println(range2acnode.get(boundery).uncertainACs);
//                if (boundery.getLeft()==24 && boundery.getRight()==25) {
//                    System.exit(0);
//                }
            }
        }

        for (Pair<Integer, Integer> range : range2acnode.keySet()) {
            if (!range2acnode.containsKey(range)) {
                System.out.println(range + " " + 0);
            } else {
                System.out.println(range + " " + range2acnode.get(range).uncertainACs.size());
            }
        }

    }

    boolean check(Pair<Integer, Integer> lRange, HashSet<SingleCse> lcses,
                  Pair<Integer, Integer> rRange, HashSet<SingleCse> rcses,
                  HashSet<SingleCse> midcses) {
        for (SingleCse m : midcses) {
            for (SingleCse l : lcses) {
                if (l.hash == m.hash) return false;
                if (l.conflict(m) || m.conflict(l)) return false;
                if (l.intersect(m) && !(l.contain(m) || m.contain(l))) return false;
            }
            for (SingleCse r : rcses) {
                if (r.hash == m.hash) return false;
                if (m.conflict(r) || r.conflict(m)) return false;
                if (r.intersect(m) && !(r.contain(m) || m.contain(r))) return false;
            }
        }
        for (SingleCse lcse : lcses) {
            for (SingleCse rcse : rcses) {
                if (lcse.hash == rcse.hash && lcse != rcse) return false;
                if (lcse.conflict(rcse)) return false;
                if (lcse.intersect(rcse) && !(lcse.contain(rcse) || rcse.contain(lcse))) return false;
            }
        }
        for (SingleCse lcse : lcses) {
            if (!rcses.contains(lcse)) {
                for (Range range : lcse.ranges) {
                    if (Math.max(range.left, rRange.getLeft()) <= Math.min(range.right, rRange.getRight())) {
                        return false;
                    }
                }
            }
        }
        for (SingleCse rcse : rcses) {
            if (!lcses.contains(rcse)) {
                for (Range range : rcse.ranges) {
                    if (Math.max(range.left, lRange.getLeft()) <= Math.min(range.right, lRange.getRight())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    OperatorNode createOperatorNode(OperatorNode lNode, Pair<Integer, Integer> lRange,
                                    OperatorNode rNode, Pair<Integer, Integer> rRange,
                                    OperatorNode originNode, Pair<Integer, Integer> midRange) {
        if (lNode == null || rNode == null || originNode == null) return null;
        if (lNode.accCost >= Double.MAX_VALUE / 2 ||
                rNode.accCost >= Double.MAX_VALUE / 2 ||
                lNode.thisCost >= Double.MAX_VALUE / 2 ||
                rNode.thisCost >= Double.MAX_VALUE / 2
        ) {
            System.out.println("cost error");
            System.out.println(originNode);
            System.out.println(lNode);
            System.out.println(rNode);
            System.exit(0);
        }

        if (lRange.equals(midRange) || rRange.equals(midRange)) {
            System.out.println("chong fu ");
            System.out.println(lNode);
            System.out.println(rNode);
            System.out.println(originNode);
            System.exit(0);
        }
        OperatorNode node = new OperatorNode();
        node.range = midRange;

        //   node.range = originNode.range; //Pair.of(lNode.range.getLeft(), rNode.range.getRight());
        //  System.out.println(lNode.range + " " + rNode.range + " " + node.range);
        node.inputs.add(lNode);
        node.inputs.add(rNode);

        node.dependencies.addAll(originNode.dependencies);
        node.dependencies.addAll(lNode.dependencies);
        node.dependencies.addAll(rNode.dependencies);

//        if (node.dependencies.size()>2) {
//            System.out.println("dependencies size > 2");
//        }

        node.oldDependencies.addAll(originNode.oldDependencies);
        node.oldDependencies.addAll(lNode.oldDependencies);
        node.oldDependencies.addAll(rNode.oldDependencies);

        node.thisCost = originNode.thisCost;
        node.hops.addAll(originNode.hops);

        node.accCost = lNode.accCost + rNode.accCost + node.thisCost;
//        System.out.println(node);
//        System.out.println("cost : "+lNode.accCost+" "+rNode.accCost+" "+node.thisCost);
        return node;
    }

    void update(OperatorNode lNode, Pair<Integer, Integer> lRange,
                OperatorNode rNode, Pair<Integer, Integer> rRange,
                OperatorNode originNode, Pair<Integer, Integer> midRange) {
        if (lNode.accCost >= Double.MAX_VALUE / 2 ||
                rNode.accCost >= Double.MAX_VALUE / 2 ||
                lNode.thisCost >= Double.MAX_VALUE / 2 ||
                rNode.thisCost >= Double.MAX_VALUE / 2
        ) {
            System.out.println("cost error");
            System.out.println(originNode);
            System.out.println(lNode);
            System.out.println(rNode);
            System.exit(0);
        }


        if (lRange.equals(midRange) || rRange.equals(midRange)) {
            System.out.println("chong fu ");
            System.out.println(lNode);
            System.out.println(rNode);
            System.out.println(originNode);
            System.exit(0);
        }
        OperatorNode node = new OperatorNode();
        node.range = midRange;

        //   node.range = originNode.range; //Pair.of(lNode.range.getLeft(), rNode.range.getRight());
        //  System.out.println(lNode.range + " " + rNode.range + " " + node.range);
        node.inputs.add(lNode);
        node.inputs.add(rNode);

        node.dependencies.addAll(originNode.dependencies);
        node.dependencies.addAll(lNode.dependencies);
        node.dependencies.addAll(rNode.dependencies);

//        if (node.dependencies.size()>2) {
//            System.out.println("dependencies size > 2");
//        }

        node.oldDependencies.addAll(originNode.oldDependencies);
        node.oldDependencies.addAll(lNode.oldDependencies);
        node.oldDependencies.addAll(rNode.oldDependencies);

        node.thisCost = originNode.thisCost;
        node.hops.addAll(originNode.hops);

        node.accCost = lNode.accCost + rNode.accCost + node.thisCost;
//        System.out.println(node);
//        System.out.println("cost : "+lNode.accCost+" "+rNode.accCost+" "+node.thisCost);
        //   insert(node, midRange);
    }
/*
    void insert(OperatorNode node, Pair<Integer, Integer> range) {
        //  removeUnusedSingleCse(node, range);
        boolean add = false;
        if (!dp.containsKey(range)) {
            HashMap<HashSet<SingleCse>, OperatorNode> tmp = new HashMap<>();
            tmp.put(node.dependencies, node);
            add = true;
            dp.put(range, tmp);
        } else {
            HashMap<HashSet<SingleCse>, OperatorNode> tmp = dp.get(range);
            if (tmp.containsKey(node.dependencies)) {
                OperatorNode node1 = tmp.get(node.dependencies);
                if (node1.accCost > node.accCost) {
                    tmp.put(node.dependencies, node);
                    add = true;
                }
            } else {
                //   tmp.put(node.dependencies, node);
                if (tmp.size() < 100) {
                    tmp.put(node.dependencies, node);
                    add = true;
                } else {
                    double maxCost = 0;
                    HashSet<SingleCse> key = null;
                    for (HashMap.Entry<HashSet<SingleCse>, OperatorNode> e : tmp.entrySet()) {
                        if (maxCost < e.getValue().accCost) {
                            maxCost = e.getValue().accCost;
                            key = e.getKey();
                        }
                    }
                    if (maxCost > node.accCost) {
                        tmp.remove(key);
                        tmp.put(node.dependencies, node);
                        add = true;
                    }
                }
            }
        }
//        if (add && node.dependencies.size() > 2) {
//            if (node.range.getRight() - node.range.getLeft() >= 9) {
//                System.out.println("insert " + node);
//            }
//        }
    }
*/

    void removeUnusedSingleCse(OperatorNode node, Pair<Integer, Integer> boundery) {
        for (Iterator<SingleCse> iter = node.dependencies.iterator(); iter.hasNext(); ) {
            SingleCse singleCse = iter.next();
            int min = Integer.MAX_VALUE, max = -1;
            for (Range range : singleCse.ranges) {
                min = Math.min(range.left, min);
                max = Math.max(range.right, max);
            }
            if (boundery.getLeft() <= min && max <= boundery.getRight()) {
                node.oldDependencies.add(singleCse);
                iter.remove();
                //  System.out.println("remove");
            }
        }
    }

}
