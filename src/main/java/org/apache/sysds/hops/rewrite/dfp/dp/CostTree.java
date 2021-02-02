package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.expressions.Sin;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.BinaryOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.Leaf;
import org.apache.sysds.hops.rewrite.dfp.coordinate.Range;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;
import org.apache.sysds.hops.rewrite.dfp.utils.Judge;
import org.apache.sysds.parser.VariableSet;

import java.util.*;

public class CostTree {

    public CostTree(VariableSet variablesUpdated) {
        this.variablesUpdated = variablesUpdated;
    }

    public VariableSet variablesUpdated = null;

    // HashMap<Pair<Integer, Integer>, ArrayList<OperatorNode>> range2OperatoeNode = new HashMap<>();


    public void testOperatorGraph(ArrayList<Pair<SingleCse, Hop>> pairs, ArrayList<Leaf> leaves) {
        ArrayList<OperatorNode> list = new ArrayList<>();
        int maxIndex = 0;
        ArrayList<SingleCse> allCses = new ArrayList<>();
        ArrayList<SingleCse> uncertainCses = new ArrayList<>();
        ArrayList<SingleCse> certainusefulCses = new ArrayList<>();
        ArrayList<SingleCse> constantCses = new ArrayList<>();


        for (Pair<SingleCse, Hop> p : pairs) {
            SingleCse cse = p.getLeft();
            Hop hop = p.getRight();
            System.out.println("========================");
            allCses.add(cse);
            OperatorNode node = createOperatorGraph(hop, false);
            MutableInt mutableInt = new MutableInt(0);
            analyzeOperatorRange(node, cse, mutableInt);
            boolean certainused = rCheckCertainUsed(node, cse);
            if (checkConstant(cse, leaves)) {
                constantCses.add(cse);
                System.out.println("Constant Cse: " + cse);
            } else {
                if (certainused) {
                    certainusefulCses.add(cse);
                    System.out.println("Certainly Useful: " + cse);
                    continue;
                } else {
                    uncertainCses.add(cse);
                    System.out.println("Uncertain: " + cse);
                }
            }
            //  System.out.println("x");
            //  System.out.println("mutableInt=" + mutableInt.getValue());
            maxIndex = Math.max(maxIndex, mutableInt.getValue() - 1);
            analyzeOperatorConstant(node, cse, leaves);
            analyzeOperatorCost(node, new HashSet<>());
            addOperatorNodeToTable(node, new HashSet<>());
            list.add(node);
        }

        for (Map.Entry<Pair<Integer, Integer>, ACNode> e : range2acnode.entrySet()) {
            System.out.println(e.getKey());
            System.out.println(e.getValue().operatorNodes.size());
        }

        filterOperatorNode0();
        CseStateMaintainer MAINTAINER = new CseStateMaintainer();
        MAINTAINER.initRangeCounter(range2acnode);
        MAINTAINER.initCseState(allCses,certainusefulCses);
        selectBest( MAINTAINER );


//        for (Pair<Integer, Integer> range : range2OperatoeNode.keySet()) {
//            showBest(range);
//        }

        showBest(Pair.of(0, maxIndex));

//        System.out.println("certainusefulCses: "+certainusefulCses.size());
//        System.out.println("uncertainCses:"+uncertainCses.size());
//
//        System.out.println(certainusefulCses.stream().map(SingleCse::toString).reduce((x,y)->x+"\n"+y));
//        System.out.println("============");
//        System.out.println(uncertainCses.stream().map(SingleCse::toString).reduce((x,y)->x+"\n"+y));
//        System.out.println("============");
//
//
//        System.out.println("constant cse: "+constantCses.size());
//        System.out.println(constantCses.stream().map(SingleCse::toString).reduce((x,y)->x+"\n"+y));


        System.out.println("done");
    }

    boolean checkConstant(SingleCse cse, ArrayList<Leaf> leaves) {
        boolean cons = true;
        if (cse.ranges.size() < 1) return false;
        for (int i = cse.ranges.get(0).left; i <= cse.ranges.get(0).right; i++) {
            Hop hop = leaves.get(i).hop;
            if (HopRewriteUtils.isTransposeOperation(hop)) {
                hop = hop.getInput().get(0);
            }
            if (variablesUpdated.containsVariable(hop.getName())) {
                cons = false;
                break;
            }
        }
        return cons;
    }


    boolean rCheckCertainUsed(OperatorNode node, SingleCse cse) {
        for (int i = 0; i < cse.ranges.size(); i++) {
            int cl = cse.ranges.get(i).left;
            int cr = cse.ranges.get(i).right;
            int nl = node.range.getLeft();
            int nr = node.range.getRight();
            int intersect = Math.min(cr, nr) - Math.max(cl, nl);
            if (intersect <= 0) continue;
            if (intersect != Math.min(cr - cl, cr - nl)) return false;
        }
        for (int i = 0; i < node.inputs.size(); i++) {
            boolean tmp = rCheckCertainUsed(node.inputs.get(i), cse);
            if (!tmp) return false;
        }
        return true;
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


    void explainOperatorNode(OperatorNode node, int d) {
        for (int i = 0; i < d; i++) System.out.print(" ");
        System.out.print("{");
        System.out.print(node);
//        if (Judge.isRead(node.hop)) System.out.print(node.hop.getName()+" ");
//        System.out.print(node.isConstant);
        for (int i = 0; i < node.inputs.size(); i++) {
            explainOperatorNode(node.inputs.get(i), d + 1);
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

    void analyzeOperatorRange(OperatorNode root, SingleCse cse, MutableInt opIndex) {
        int begin = opIndex.getValue();
        if (root.inputs.size() > 0) {
            for (int i = 0; i < root.inputs.size(); i++) {
                analyzeOperatorRange(root.inputs.get(i), cse, opIndex);
                root.dependencies.addAll(root.inputs.get(i).dependencies);
            }
        } else {
            opIndex.increment();
        }
        int end = opIndex.getValue() - 1;
        root.range = Pair.of(begin, end);
//        System.out.println("analyze range: " + root.range);
        for (Range range : cse.ranges) {
            if ((range.left == begin && range.right == end) || (range.left == end && range.right == begin)) {
                root.dependencies.add(cse);
            }
        }
    }


    void analyzeOperatorConstant(OperatorNode node, SingleCse cse, ArrayList<Leaf> leves) {
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorConstant(node.inputs.get(i), cse, leves);
        }
        //  System.out.println(node.hop.getName() + " " + ans);
        node.isConstant = checkConstant(cse, leves);
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
            node.thisCost = node.thisCost / csesize;
            node.accCost = node.accCost / csesize;
        }
        if (node.isConstant) {
            node.thisCost /= 100;
            node.accCost /= 100;
        }
        //  System.out.println(node);
        visited.add(node);
    }

    void addOperatorNodeToTable(OperatorNode node, HashSet<OperatorNode> visited) {
        // node.accCost = node.thisCost;
        System.out.println("add node to table: " + node.range);
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
//    }
        visited.add(node);
    }

    void filterOperatorNode0() {
        for (Pair<Integer, Integer> range : range2acnode.keySet()) {
            ArrayList<OperatorNode> a1 = range2acnode.get(range).operatorNodes;
            if (a1.size() < 2) continue;
            OperatorNode operatorNode = a1.get(0);
            if (operatorNode.hops.size() < 1) continue;
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
            HashMap<Pair<Pair<Integer, Integer>, Pair<Integer, Integer>>, OperatorNode> map = new HashMap<>();
            for (OperatorNode node : a1) {
                if (node.inputs.size() != 2) continue;
                Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> key = Pair.of(node.inputs.get(0).range, node.inputs.get(1).range);
                if (!map.containsKey(key)) {
                    map.put(key, node);
                } else {
                    if (map.get(key).accCost > node.accCost) {
                        map.put(key, node);
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
            ArrayList<OperatorNode> a3 = new ArrayList<>();
            for (int i = 0; i < a2.size() && i < 20; i++) a3.add(a2.get(i));
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
                Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> key1 = Pair.of(node1.inputs.get(0).range, node1.inputs.get(1).range);
                for (OperatorNode node2 : a2) {
                    Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> key2 = Pair.of(node2.inputs.get(0).range, node2.inputs.get(1).range);
                    if (key1.equals(key2)) {
                        if (node1.accCost > node2.accCost) {
                            ok = false;
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


    void classifyOperatorNode(ArrayList<OperatorNode> operatorNodes) {

    }

    void selectBest(CseStateMaintainer MAINTAINER) {
//        dp = new HashMap<>();
        ArrayList<Pair<Integer, Integer>> sortedRanges = new ArrayList<>(range2acnode.keySet());
//        ranges.sort(Comparator.comparingInt((Pair<Integer,Integer> a)->(a.getRight()-a.getLeft())));
        sortedRanges.sort(Comparator.comparingInt((Pair<Integer, Integer> a) -> (a.getRight() - a.getLeft())).thenComparingInt(Pair::getLeft));
        System.out.println(sortedRanges);
        for (Pair<Integer, Integer> boundery : sortedRanges) {
//            if (boundery.getRight()-boundery.getLeft()>2) break;
//            if (boundery.getLeft()==1&&boundery.getRight()==19) {
//                System.out.println("x");
//            }
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
                } else {
                    allResults.addAll(acNode.operatorNodes);
                }
            }
            acNode.uncertainACs = new HashMap<>();
            int removed = 0;
            for (OperatorNode node : allResults) {
                boolean ok = true;
                for (SingleCse cse : node.dependencies) {
                    if (MAINTAINER.cseCounter.getValue(cse) == 0) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    acNode.addUncertainAC(node);
                } else {
                    removed++;
                }
//                acNode.addUncertainAC(node);
                if (acNode.minAC == null || acNode.minAC.accCost > node.accCost) {
                    acNode.minAC = node;
                }
            }
            System.out.println(boundery + " counter remove " + removed);

            MAINTAINER.updateCseState(acNode, range2acnode);


            if (!range2acnode.containsKey(boundery)) {
                range2acnode.put(boundery, new ACNode());
            } else {
                System.out.println("boundery=" + boundery + " = " + range2acnode.get(boundery).uncertainACs.size());
            }
        }

        for (Pair<Integer, Integer> range : sortedRanges) {
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

}
