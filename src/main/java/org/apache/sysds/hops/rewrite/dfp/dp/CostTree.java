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

import java.util.*;

public class CostTree {

    public void testCostTree(ArrayList<Pair<SingleCse, Hop>> pairs) {

//        Hop hop = pairs.get(0).getRight();
//        hop.resetVisitStatusForced(new HashSet<>());
//        testHash(hop);
//        System.exit(0);

        long time1 = System.nanoTime();
        int maxIndex = 0;
        for (Pair<SingleCse, Hop> p : pairs) {
            int tmpIndex = addHop(p.getRight(), p.getLeft());
            maxIndex = Math.max(maxIndex, tmpIndex);
//            System.out.println("added");
//            System.out.println("range2Node size = "+range2Node.size());
//            System.out.println(range2Node.keySet());
//            OptionalInt min = range2Node.keySet().stream().mapToInt(p->p.getLeft()).min();
//            OptionalInt max = range2Node.keySet().stream().mapToInt(p->p.getRight()).max();
//            System.out.println(min+"  "+max);
        }
        filterOperatorNode();
//        for (HashMap.Entry<Pair<Integer, Integer>, ArrayList<OperatorNode>> entry : range2OperatoeNode.entrySet()) {
//            System.out.println("range: " + entry.getKey().getLeft() + " " + entry.getKey().getRight() + " " + entry.getValue().size());
//            for (OperatorNode node: entry.getValue()) {
//                System.out.println(node.thisCost);
//            }
//            //  System.out.println(entry.getValue());
//        }
//        System.exit(0);
        selectBest();

        ArrayList<OperatorNode> list = new ArrayList<>();
        for (Map.Entry<HashSet<SingleCse>, OperatorNode> e : dp.get(Pair.of(0, maxIndex)).entrySet()) {
            list.add(e.getValue());
        }
        list.sort(Comparator.comparingDouble(a -> a.accCost));
        for (int i = 0; i < 20 && i < list.size(); i++) {
            System.out.println(list.get(i));
        }
        System.out.println("done");
        long time2 = System.nanoTime();
        System.out.println("time = " + ((time2 - time1) / 1e9));
    }

//    public static class RangeNode {
//        Pair<Integer, Integer> range;
//        ArrayList<OperatorNode> options = new ArrayList<>();
//        ArrayList<OperatorNode> bestOptions = new ArrayList<>();
//    }


    HashMap<Pair<Integer, Integer>, ArrayList<OperatorNode>> range2OperatoeNode = new HashMap<>();

    public static class OperatorNode {
        //        ArrayList<RangeNode> operands = new ArrayList<>();
        Pair<Integer, Integer> range = null;
        ArrayList<Pair<Integer, Integer>> ranges = new ArrayList<>();
        HashSet<SingleCse> dependencies = new HashSet<>();
        HashSet<SingleCse> oldDependencies = new HashSet<>();
        Hop hop = null;
        double thisCost = 1000000;
        double accCost = 0;
        ArrayList<OperatorNode> inputs = new ArrayList<>();

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OperatorNode that = (OperatorNode) o;
            return Double.compare(that.thisCost, thisCost) == 0 &&
                    Double.compare(that.accCost, accCost) == 0 &&
                    range.equals(that.range) &&
                    Objects.equals(dependencies, that.dependencies);// &&
            //  Objects.equals(hop, that.hop) &&
            //   Objects.equals(inputs, that.inputs);
        }


        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("ON{");
            sb.append(hop.getOpString());
            sb.append(",");
            sb.append(thisCost);
            sb.append(",");
            sb.append(accCost);
            sb.append(",[");
            for (SingleCse singleCse : dependencies) {
                sb.append(singleCse.name);
                sb.append(",");
                sb.append(singleCse.ranges);
            }
            sb.append("],");

            sb.append(",[");
            for (SingleCse singleCse : oldDependencies) {
                sb.append(singleCse.name);
                sb.append(",");
                sb.append(singleCse.ranges);
            }
            sb.append("],");

//            sb.append(dependencies);
            sb.append("[");
            for (OperatorNode on : inputs) {
                sb.append(",");
                sb.append(on.range);
            }
            sb.append("]");
            sb.append("}\n");
            return sb.toString();
        }
    }

//    public HashMap<Pair<Integer, Integer>, RangeNode> range2Node = new HashMap<>();

    public CostTree() {
    }

    int addHop2(Hop hop, SingleCse singleCse) {

        return 0;
    }

//    void testHash(Hop hop) {
//        if (hop.isVisited()) return;
//        for (Hop i:hop.getInput()) testHash(i);
//        System.out.println(hop.getHopID()+" "+hop.hashCode());
//        hop.setVisited();
//    }

    public void testOperatorGraph(ArrayList<Pair<SingleCse, Hop>> pairs) {
        ArrayList<OperatorNode> list = new ArrayList<>();
        for (Pair<SingleCse, Hop> p : pairs) {
            OperatorNode node = createOperatorGraph(p.getRight(), new HashMap<>(), false);
            analyzeOperatorRanges(node, p.getLeft(), new MutableInt(0));
            analyzeOperatorCost(node,new HashSet<>());
            addOperatorNodeToTable(node,new HashSet<>());
            list.add(node);
            System.out.println(node);
        }
    }


    OperatorNode createOperatorGraph(Hop hop, HashMap<Hop, OperatorNode> mp, boolean transpose) {
        if (mp.containsKey(hop)) {
            return mp.get(hop);
        }
        OperatorNode node = null;  //= new OperatorNode();
        if (HopRewriteUtils.isTransposeOperation(hop)) {
            return createOperatorGraph(hop.getInput().get(0), mp, !transpose);
        } else if (hop instanceof LiteralOp) {
            return null;
        } else if (HopRewriteUtils.isUnary(hop, Types.OpOp1.CAST_AS_SCALAR) ||
                Judge.isWrite(hop)) {
            return createOperatorGraph(hop.getInput().get(0), mp, transpose);
        } else if (Judge.isLeafMatrix(hop)) {
            node = new OperatorNode();
            node.hop = hop;
            node.thisCost = 0; // todo
        } else {
            node = new OperatorNode();
            node.hop = hop;
            node.thisCost = estimateCost(hop); // todo
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

    void analyzeOperatorCost(OperatorNode node,HashSet<OperatorNode> visited) {
        node.accCost = node.thisCost;
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorCost(node.inputs.get(i),visited);
            node.accCost += node.inputs.get(i).accCost;
        }
        if (node.ranges.size() > 0) {
            node.accCost /= node.ranges.size();
        }
        visited.add(node);
    }

    void addOperatorNodeToTable(OperatorNode node,HashSet<OperatorNode> visited) {
        node.accCost = node.thisCost;
        for (int i = 0; i < node.inputs.size(); i++) {
            addOperatorNodeToTable(node.inputs.get(i),visited);
        }
        for (int i=0;i<node.ranges.size();i++) {
            Pair<Integer,Integer> range = node.ranges.get(i);
            ArrayList<OperatorNode> nodes;
            if (range2OperatoeNode.containsKey(range)) {
                nodes = range2OperatoeNode.get(range);
            } else {
                nodes = new ArrayList<>();
            }
            nodes.add(node);
            range2OperatoeNode.put(range, nodes);
        }
        visited.add(node);
    }


    int count = 0;

    public int addHop(Hop hop, SingleCse singleCse) {
        System.out.println("single cse: " + singleCse);
        HashMap<Hop, ArrayList<Pair<Integer, Integer>>> hop2index = new HashMap<>();
        MutableInt opIndex = new MutableInt(0);
        count = 0;
        rAddHop(hop, opIndex, hop2index, singleCse, false);
        if (count != singleCse.ranges.size()) {
//            System.out.println(singleCse);
//            System.out.println(hop2index);
//            System.out.println(Explain.explain(hop));
            System.out.println("xx");
        }
        return opIndex.getValue() - 1;
    }

    public OperatorNode rAddHop(Hop hop, MutableInt opIndex, HashMap<Hop, ArrayList<Pair<Integer, Integer>>> hop2index, SingleCse singleCse, boolean transpose) {
        int begin, end;
        OperatorNode operatorNode = new OperatorNode();
        operatorNode.hop = hop;
        if (HopRewriteUtils.isTransposeOperation(hop)) {
            return rAddHop(hop.getInput().get(0), opIndex, hop2index, singleCse, !transpose);
        } else if (HopRewriteUtils.isUnary(hop, Types.OpOp1.CAST_AS_SCALAR) ||
                Judge.isWrite(hop)) {
            return rAddHop(hop.getInput().get(0), opIndex, hop2index, singleCse, transpose);
        } else if (hop instanceof LiteralOp) {
            return null;
        } else if (Judge.isLeafMatrix(hop)) {
            begin = opIndex.getValue();
            opIndex.increment();
            end = opIndex.getValue() - 1;
            operatorNode.thisCost = 0;
        } else {
            begin = opIndex.getValue();
            if (!transpose) {
                for (int i = 0; i < hop.getInput().size(); i++) {
                    OperatorNode tmp = rAddHop(hop.getInput().get(i), opIndex, hop2index, singleCse, transpose);
                    if (tmp == null) continue;
                    operatorNode.inputs.add(tmp);
                    operatorNode.dependencies.addAll(tmp.dependencies);
                    operatorNode.accCost += tmp.accCost;
                }
            } else {
                for (int i = hop.getInput().size() - 1; i >= 0; i--) {
                    OperatorNode tmp = rAddHop(hop.getInput().get(i), opIndex, hop2index, singleCse, transpose);
                    if (tmp == null) continue;
                    operatorNode.inputs.add(tmp);
                    operatorNode.dependencies.addAll(tmp.dependencies);
                    operatorNode.accCost += tmp.accCost;
                }
            }
            end = opIndex.getValue() - 1;
            boolean isCse = isCse(begin, end, singleCse);
            double cost = estimateCost(hop); // todo: estimate cost
            if (isCse) {
                count++;
                operatorNode.thisCost = cost / singleCse.ranges.size();
                operatorNode.dependencies.add(singleCse);
                if (isAta(hop)) operatorNode.thisCost /= 100;
            } else {
                operatorNode.thisCost = cost;
            }
            operatorNode.accCost += operatorNode.thisCost;
        }
        Pair<Integer, Integer> range = Pair.of(begin, end);
        // System.out.println(hop.getHopID()+" "+range);
        operatorNode.range = range;
        if (operatorNode != null) {
            ArrayList<OperatorNode> nodes;
            if (range2OperatoeNode.containsKey(range)) {
                nodes = range2OperatoeNode.get(range);
            } else {
                nodes = new ArrayList<>();
            }
            nodes.add(operatorNode);
            range2OperatoeNode.put(range, nodes);
        }
        if (!hop2index.containsKey(hop)) {
            hop2index.put(hop, new ArrayList<>());
        }
        hop2index.get(hop).add(range);
        return operatorNode;
    }

    boolean isAta(Hop hop) {
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
            Hop l = hop.getInput().get(0);
            Hop r = hop.getInput().get(1);
            if (HopRewriteUtils.isTransposeOperation(l) && l.getInput().get(0).equals(r) && r.getName().equals("a") ||
                    HopRewriteUtils.isTransposeOperation(r) && r.getInput().get(0).equals(l) && l.getName().equals("a")) {
                return true;
            }
        }
        return false;
    }

    public double estimateCost(Hop hop) {
        double cost;
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
            Hop l = hop.getInput().get(0);
            Hop r = hop.getInput().get(1);
            cost = l.getDim1() * l.getDim2() * r.getDim2();
        } else {
            cost = hop.getDim1() * hop.getDim2();
        }
        return cost;
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

    HashSet<Pair<Integer, Integer>> visited;

    HashMap<Pair<Integer, Integer>, HashMap<HashSet<SingleCse>, OperatorNode>> dp;

    void selectBest() {
        dp = new HashMap<>();
        visited = new HashSet<>();
        ArrayList<Pair<Integer, Integer>> ranges = new ArrayList<>();
        ranges.addAll(range2OperatoeNode.keySet());
        ranges.sort(Comparator.comparingInt(a -> (a.getRight() - a.getLeft())));
        System.out.println(ranges);
        for (Pair<Integer, Integer> range : ranges) {
            ArrayList<OperatorNode> operatorNodes = range2OperatoeNode.get(range);
            for (OperatorNode operatorNode : operatorNodes) {
                System.out.println("Operator Node " + operatorNode.range);
                if (Judge.isLeafMatrix(operatorNode.hop)) {
                    insert(operatorNode);
                } else if (operatorNode.inputs.size() == 2) {
                    Pair<Integer, Integer> lRange = operatorNode.inputs.get(0).range;
                    Pair<Integer, Integer> rRange = operatorNode.inputs.get(1).range;
                    dp.get(lRange).forEach((singleCse, operatorNode1) -> {
                        dp.get(rRange).forEach((singleCse2, operatorNode2) -> {
                            if (check(lRange, operatorNode1.dependencies, rRange, operatorNode2.dependencies, operatorNode.dependencies)) {
                                update(operatorNode1, operatorNode2, operatorNode);
                            }
                        });
                    });
                    update(operatorNode.inputs.get(0), operatorNode.inputs.get(1), operatorNode);
                }
//                if (dp.containsKey(operatorNode.range)) {
//                    System.out.println(operatorNode.range+" "+  dp.get(operatorNode.range));
//                }
            }
            if (!dp.containsKey(range)) {
                dp.put(range, new HashMap<>());
            } else {
                System.out.println(dp.get(range).size());
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

    void update(OperatorNode lNode, OperatorNode rNode, OperatorNode originNode) {
        if (lNode.range.equals(originNode.range) || rNode.range.equals(originNode.range)) {
            System.out.println("chong fu ");
            System.out.println(lNode);
            System.out.println(rNode);
            System.out.println(originNode);
            System.exit(0);
        }
        OperatorNode node = new OperatorNode();
        node.range = originNode.range; //Pair.of(lNode.range.getLeft(), rNode.range.getRight());
        //  System.out.println(lNode.range + " " + rNode.range + " " + node.range);
        node.inputs.add(lNode);
        node.inputs.add(rNode);

        node.dependencies.addAll(originNode.dependencies);
        node.dependencies.addAll(lNode.dependencies);
        node.dependencies.addAll(rNode.dependencies);

        node.oldDependencies.addAll(originNode.oldDependencies);
        node.oldDependencies.addAll(lNode.oldDependencies);
        node.oldDependencies.addAll(rNode.oldDependencies);

        node.thisCost = originNode.thisCost;
        node.hop = originNode.hop;
        node.accCost = lNode.accCost + rNode.accCost + node.thisCost;
        insert(node);
    }

    void insert(OperatorNode node) {
        //   removeUnusedSingleCse(node);
        if (!dp.containsKey(node.range)) {
            HashMap<HashSet<SingleCse>, OperatorNode> tmp = new HashMap<>();
            tmp.put(node.dependencies, node);
            dp.put(node.range, tmp);
        } else {
            HashMap<HashSet<SingleCse>, OperatorNode> tmp = dp.get(node.range);
            if (tmp.containsKey(node.dependencies)) {
                OperatorNode node1 = tmp.get(node.dependencies);
                if (node1.accCost > node.accCost) {
                    tmp.put(node.dependencies, node);
                }
            } else {
//                tmp.put(node.dependencies, node);
                if (tmp.size() < 100) {
                    tmp.put(node.dependencies, node);
                } else {
                    double maxCost = 0;
                    HashSet<SingleCse> key = null;
                    for (HashMap.Entry<HashSet<SingleCse>, OperatorNode> e : tmp.entrySet()) {
                        if (maxCost < e.getValue().accCost) {
                            maxCost = e.getValue().accCost;
                            key = e.getKey();
                        }
                    }
                    if (key != null) {
                        tmp.remove(key);
                    }
                    tmp.put(node.dependencies, node);
                }
            }
        }
    }


    void removeUnusedSingleCse(OperatorNode node) {
        for (Iterator<SingleCse> iter = node.dependencies.iterator(); iter.hasNext(); ) {
            SingleCse singleCse = iter.next();
            int min = Integer.MAX_VALUE, max = -1;
            for (Range range : singleCse.ranges) {
                min = Math.min(range.left, min);
                max = Math.max(range.right, max);
            }
            if (node.range.getLeft() <= min && max <= node.range.getRight()) {
                node.oldDependencies.add(singleCse);
                iter.remove();
                //  System.out.println("remove");
            }
        }
    }

}
