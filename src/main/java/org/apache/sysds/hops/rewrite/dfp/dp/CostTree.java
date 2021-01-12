package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.coordinate.Range;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.utils.Explain;

import java.util.*;

public class CostTree {

    public CostTree() {
    }

    public static VariableSet variablesUpdated = null;

    HashMap<Pair<Integer, Integer>, ArrayList<OperatorNode>> range2OperatoeNode = new HashMap<>();


    public void testOperatorGraph(ArrayList<Pair<SingleCse, Hop>> pairs) {
        ArrayList<OperatorNode> list = new ArrayList<>();
        int maxIndex = 0;
        for (Pair<SingleCse, Hop> p : pairs) {
            //  HopCostEstimator.buildMMNodeTree(p.getRight());
            // System.out.println(Explain.explain(p.getRight()));
            OperatorNode node = createOperatorGraph(p.getRight(), new HashMap<>(), false);
            MutableInt mutableInt = new MutableInt(0);
            analyzeOperatorRanges(node, p.getLeft(), mutableInt);
            //  System.out.println("mutableInt=" + mutableInt.getValue());
            maxIndex = Math.max(maxIndex, mutableInt.getValue() - 1);
            analyzeOperatorConstant(node);
            analyzeOperatorCost(node, new HashSet<>());
            addOperatorNodeToTable(node, new HashSet<>());
            list.add(node);
            // explain(node, 0);
            System.out.println("========================");
//            System.out.println(node);
        }

//        for (Map.Entry<Pair<Integer,Integer>,ArrayList<OperatorNode>> e: range2OperatoeNode.entrySet()) {
//            System.out.println(e.getKey());
//            System.out.println(e.getValue().size());
//        }

//        System.exit(0);

//          filterOperatorNode();
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
        for (Map.Entry<HashSet<SingleCse>, OperatorNode> e : dp.get(range).entrySet()) {
            list2.add(e.getValue());
//            if (e.getValue().dependencies.size() > 2) {
//                System.out.println("dependencies size > 2");
//            }
        }
        list2.sort(Comparator.comparingDouble(a -> a.accCost));
        for (int i = 0; i < 20 && i < list2.size(); i++) {
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


    OperatorNode createOperatorGraph(Hop hop, HashMap<Hop, OperatorNode> mp, boolean transpose) {
        if (mp.containsKey(hop)) {
            return mp.get(hop);
        }
        OperatorNode node = null;  //= new OperatorNode();
        if (Judge.isWrite(hop)) {
            node = createOperatorGraph(hop.getInput().get(0), mp, transpose);
            node.hops.add(hop);
        } else if (Judge.isLeafMatrix(hop)) {
            node = new OperatorNode();
            node.hops.add(hop);
        } else if (HopRewriteUtils.isTransposeOperation(hop)) {
            node = createOperatorGraph(hop.getInput().get(0), mp, !transpose);
            node.hops.add(hop);
        } else if (hop instanceof LiteralOp) {
            return null;
        } else if (HopRewriteUtils.isUnary(hop, Types.OpOp1.CAST_AS_SCALAR)) {
            node = createOperatorGraph(hop.getInput().get(0), mp, transpose);
            node.hops.add(hop);
        } else {
            node = new OperatorNode();
            node.hops.add(hop);
            if (!transpose) {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    OperatorNode tmp = createOperatorGraph(hop.getInput().get(i), mp, transpose);
                    if (tmp == null) continue;
                    node.inputs.add(tmp);
                    // node.accCost += tmp.accCost;
                }
            } else {
                for (int i = hop.getInput().size() - 1; i >= 0; i--) {
                    OperatorNode tmp = createOperatorGraph(hop.getInput().get(i), mp, transpose);
                    if (tmp == null) continue;
                    node.inputs.add(tmp);
                    //   node.accCost += tmp.accCost;
                }
            }
        }
//        node.thisCost = NodeCostEstimator.getNodeCost(node);
        // System.out.println("put " + node);
        mp.put(hop, node);
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
        root.ranges.add(Pair.of(begin, end));

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
        if (node.ranges.size() > 0) {
            node.thisCost = node.thisCost / node.ranges.size();
            node.accCost = node.accCost / node.ranges.size();
        }
        if (node.isConstant) {
            node.thisCost /= 100;
            node.accCost /= 100;
        }
        System.out.println(node);
        visited.add(node);
    }

    void addOperatorNodeToTable(OperatorNode node, HashSet<OperatorNode> visited) {
        // node.accCost = node.thisCost;
        for (int i = 0; i < node.inputs.size(); i++) {
            addOperatorNodeToTable(node.inputs.get(i), visited);
        }
        for (int i = 0; i < node.ranges.size(); i++) {
            Pair<Integer, Integer> range = node.ranges.get(i);
            if (!visited.contains(node)) {
                ArrayList<OperatorNode> nodes;
                if (range2OperatoeNode.containsKey(range)) {
                    nodes = range2OperatoeNode.get(range);
                } else {
                    nodes = new ArrayList<>();
                }
                nodes.add(node);
                range2OperatoeNode.put(range, nodes);
            }
        }
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

    void filterOperatorNode() {
        for (Pair<Integer, Integer> range : range2OperatoeNode.keySet()) {
            ArrayList<OperatorNode> a1 = range2OperatoeNode.get(range);
            ArrayList<OperatorNode> a2 = new ArrayList<>();
            for (OperatorNode node : a1) {
                boolean ok = true;
                for (OperatorNode node1 : a2) {
                    if (node1.equals(node)) {
                        ok = false;
                        break;
                    }
                }
                if (ok) a2.add(node);
            }
            range2OperatoeNode.put(range, a2);
        }
    }


    HashMap<Pair<Integer, Integer>, HashMap<HashSet<SingleCse>, OperatorNode>> dp;

    void selectBest() {
        dp = new HashMap<>();
        ArrayList<Pair<Integer, Integer>> ranges = new ArrayList<>(range2OperatoeNode.keySet());
        ranges.sort(Comparator.comparingInt(a -> (a.getRight() - a.getLeft())));
        System.out.println(ranges);
        for (Pair<Integer, Integer> boundery : ranges) {
            ArrayList<OperatorNode> operatorNodes = range2OperatoeNode.get(boundery);
            for (OperatorNode operatorNode : operatorNodes) {
                //  System.out.println("Operator Node " + operatorNode.range);
                if (Judge.isLeafMatrix(operatorNode.hops.get(0))) {
                    insert(operatorNode, boundery);
                } else if (operatorNode.inputs.size() == 2) {
                    //todo Pair<Integer, Integer> lRange = operatorNode.inputs.get(0).ranges.get(0);
                    //todo Pair<Integer, Integer> rRange = operatorNode.inputs.get(1).ranges.get(0);
                    Pair<Integer, Integer> lRange = null, rRange = null;
                    for (Pair<Integer, Integer> tmp : operatorNode.inputs.get(0).ranges) {
                        if (tmp.getLeft().equals(boundery.getLeft())) {
                            lRange = tmp;
                            break;
                        }
                    }
                    for (Pair<Integer, Integer> tmp : operatorNode.inputs.get(1).ranges) {
                        if (tmp.getRight().equals(boundery.getRight())) {
                            rRange = tmp;
                            break;
                        }
                    }
                    if (lRange == null || rRange == null) continue;
                    if (lRange.equals(boundery) || rRange.equals(boundery)) continue;
                    for (Map.Entry<HashSet<SingleCse>, OperatorNode> entry1 : dp.get(lRange).entrySet()) {
                        OperatorNode operatorNode1 = entry1.getValue();
                        for (Map.Entry<HashSet<SingleCse>, OperatorNode> entry2 : dp.get(rRange).entrySet()) {
                            OperatorNode operatorNode2 = entry2.getValue();
                            if (check(lRange, operatorNode1.dependencies, rRange, operatorNode2.dependencies, operatorNode.dependencies)) {
                                update(operatorNode1, lRange, operatorNode2, rRange, operatorNode, boundery);
                            }
                        }
                    }
                    update(operatorNode.inputs.get(0), lRange, operatorNode.inputs.get(1), rRange, operatorNode, boundery);
                } else {
//                    System.out.println("ELSE");
//                    System.out.println(operatorNode);
                }
//                if (dp.containsKey(operatorNode.range)) {
//                    System.out.println(operatorNode.range+" "+  dp.get(operatorNode.range));
//                }
            }
            if (!dp.containsKey(boundery)) {
                dp.put(boundery, new HashMap<>());
            } else {
//                   System.out.println(boundery + " = " + dp.get(boundery).size());
            }
            System.out.println(boundery + " = " + dp.get(boundery).size());
        }

        for (Pair<Integer, Integer> range : range2OperatoeNode.keySet()) {
            if (!dp.containsKey(range)) {
                System.out.println(range + " " + 0);
            } else {
                System.out.println(range + " " + dp.get(range).size());
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
            }
            for (SingleCse r : rcses) {
                if (r.hash == m.hash) return false;
                if (m.conflict(r) || r.conflict(m)) return false;
            }
        }
        for (SingleCse lcse : lcses) {
            for (SingleCse rcse : rcses) {
                if (lcse.hash == rcse.hash && lcse != rcse) return false;
                if (lcse.conflict(rcse)) return false;
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
        node.ranges.add(midRange);

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
        insert(node, midRange);
    }

    void insert(OperatorNode node, Pair<Integer, Integer> range) {
//        removeUnusedSingleCse(node, range);
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
//                tmp.put(node.dependencies, node);
                if (tmp.size() < 200) {
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
//            if (node.ranges.get(0).getRight() - node.ranges.get(0).getLeft() >= 9) {
//                System.out.println("insert " + node);
//            }
//        }
    }


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
