package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.sysds.utils.Explain;

import java.util.*;

public class CostTree {

    public CostTree(VariableSet variablesUpdated) {
        this.variablesUpdated = variablesUpdated;
    }

    public VariableSet variablesUpdated = null;

    // HashMap<Pair<Integer, Integer>, ArrayList<OperatorNode>> range2OperatoeNode = new HashMap<>();


    public void testOperatorGraph(ArrayList<Pair<SingleCse, Hop>> pairs, Pair<SingleCse, Hop> emptyPair, ArrayList<Range> blockRanges, ArrayList<Leaf> leaves) {

        ArrayList<OperatorNode> list = new ArrayList<>();
        int maxIndex = 0;
        ArrayList<SingleCse> allCses = new ArrayList<>();
        ArrayList<SingleCse> uncertainCses = new ArrayList<>();
        ArrayList<SingleCse> certainusefulCses = new ArrayList<>();
        ArrayList<SingleCse> certainuselessCses = new ArrayList<>();
        ArrayList<SingleCse> constantCses = new ArrayList<>();
//        HashSet<Pair<Integer, Integer>> ranges  = getBestMatrixBranches(blockRanges,leaves);
        HashSet<Pair<Integer, Integer>> ranges = new HashSet<>();

        OperatorNode emptyNode = createOperatorGraph(emptyPair.getRight(), false);
        analyzeOperatorRange(emptyNode, emptyPair.getLeft(), new MutableInt(0));
        rGetRanges(emptyNode, ranges);

        for (Pair<SingleCse, Hop> p : pairs) {
            SingleCse cse = p.getLeft();
            Hop hop = p.getRight();
            System.out.println("========================");
            allCses.add(cse);
            OperatorNode node = createOperatorGraph(hop, false);
            MutableInt mutableInt = new MutableInt(0);
            analyzeOperatorRange(node, cse, mutableInt);
            boolean certainused = rCheckCertainUsed(cse, ranges);
            if (checkConstant(cse, leaves)) {
//                constantCses.add(cse);
                System.out.println("Constant Cse: " + cse);
            } else {
                if (certainused) {
//                    certainusefulCses.add(cse);
                    System.out.println("Certainly Useful: " + cse);
                    // continue;
                } else {
//                    uncertainCses.add(cse);
                    System.out.println("Uncertain: " + cse);
                }
            }
            //  System.out.println("x");
            //  System.out.println("mutableInt=" + mutableInt.getValue());
            maxIndex = Math.max(maxIndex, mutableInt.getValue() - 1);
            analyzeOperatorConstant(node);
            analyzeOperatorCost(node, new HashSet<>());
            addOperatorNodeToTable(node, new HashSet<>());
            list.add(node);
//            explainOperatorNode(node,0);
        }
        list.sort(Comparator.comparing(node -> node.accCost));

//        for (Map.Entry<Pair<Integer, Integer>, ACNode> e : range2acnode.entrySet()) {
//            System.out.println(e.getKey());
//            System.out.println(e.getValue().operatorNodes.size());
//        }

        filterOperatorNode0();

//        showAcnodeGraph();

        CseStateMaintainer MAINTAINER = new CseStateMaintainer();
        MAINTAINER.initRangeCounter(range2acnode);
        MAINTAINER.initCseState(allCses, certainusefulCses);

        selectBest(MAINTAINER);

//        for (Pair<Integer, Integer> range : range2OperatoeNode.keySet()) {
//            showBest(range);
//        }

        showBest(Pair.of(0, maxIndex), list.get(0));

        System.out.println("certainusefulCses: " + certainusefulCses.size());
        System.out.println(certainusefulCses.stream().map(SingleCse::toString).reduce((x, y) -> x + "\n" + y));
        System.out.println("============");
//        System.out.println("uncertainCses:" + uncertainCses.size());
//        System.out.println(uncertainCses.stream().map(SingleCse::toString).reduce((x, y) -> x + "\n" + y));
//        System.out.println("============");

        System.out.println("ConstantCses:" + constantCses.size());
        System.out.println(constantCses.stream().map(SingleCse::toString).reduce((x, y) -> x + "\n" + y));
        System.out.println("============");

        System.out.println("AllCses:" + allCses.size());
        System.out.println(allCses.stream().map(SingleCse::toString).reduce((x, y) -> x + "\n" + y));
        System.out.println("============");


//        ArrayList<SingleCse> C = new ArrayList<>();
//        ArrayList<SingleCse> D = new ArrayList<>();
//        for (SingleCse cse: uncertainCses) {
//            boolean a = true,b=true;
//            for (SingleCse c1: certainusefulCses) {
//                if ( cse.intersect(c1) && !(cse.contain(c1) || c1.contain(cse))) {
//                    a=false;break;
//                }
//            }
//            for (SingleCse c1: constantCses) {
//                if ( cse.intersect(c1) && !(cse.contain(c1) || c1.contain(cse))) {
//                    b=false;break;
//                }
//            }
//            if (a||b) C.add(cse);
//            else D.add(cse);
//        }
//
//        System.out.println("CCCCC:" + C.size());
//        System.out.println(C.stream().map(SingleCse::toString).reduce((x, y) -> x + "\n" + y));
//        System.out.println("============");
//
//        System.out.println("DDDDD:" + D.size());
//        System.out.println(D.stream().map(SingleCse::toString).reduce((x, y) -> x + "\n" + y));
//        System.out.println("============");

//        System.out.println("constant cse: "+constantCses.size());
//        System.out.println(constantCses.stream().map(SingleCse::toString).reduce((x,y)->x+"\n"+y));

        System.out.println(Explain.explain(emptyPair.getRight()));

        System.out.println("done");

//        for (int i = 0; i < 20; i++) {
//            System.out.println(list.get(i).accCost + " " + list.get(i).dependencies);
//        }
//        System.out.println("-------------------");
//        for (int i = 0; i < 20; i++) {
//            explainOperatorNode(list.get(i), 0);
//        }

    }


    HashSet<Pair<Integer, Integer>> getBestMatrixBranches(ArrayList<Range> blockRanges, ArrayList<Leaf> leaves) {
        HashSet<Pair<Integer, Integer>> ss = new HashSet<>();

        for (Range range : blockRanges) {
//            System.out.println("-----------------------");
//            System.out.println(range);
            int n = range.right - range.left + 1;
            long[] a = new long[n + 1];
            for (int i = 0; i < n; i++) {
                a[i] = leaves.get(range.left + i).hop.getDim1();
            }
            a[n] = leaves.get(range.right).hop.getDim2();
//            System.out.println(Arrays.stream(a).mapToObj(String::valueOf).reduce((x, y) -> x + "," + y));
            long[][] f = new long[n][n];
            HashSet<Integer>[][] g = new HashSet[n][n];
            for (int i = 0; i < n; i++) f[i][i] = 0;
            for (int l = 2; l <= n; l++) {
                for (int i = 0; i <= n - l; i++) {
                    int j = i + l - 1;
                    f[i][j] = Long.MAX_VALUE;
                    for (int k = i; k < j; k++) {
                        long tmp = f[i][k] + f[k + 1][j] + a[i] * a[j + 1] * a[k + 1];
                        if (f[i][j] > tmp) {
                            f[i][j] = tmp;
                            if (g[i][j] == null) g[i][j] = new HashSet<>();
                            else g[i][j].clear();
                            g[i][j].add(k);
                        } else if (f[i][j] == tmp) {
                            if (g[i][j] == null) g[i][j] = new HashSet<>();
                            g[i][j].add(k);
                        }
                    }
                }
            }
//            for (int i = 0; i < n; i++) {
//                for (int j = 0; j < n; j++) {
//                    System.out.printf("%d ", f[i][j]);
//                }
//                System.out.println("");
//            }
//            for (int i = 0; i < n; i++) {
//                for (int j = 0; j < n; j++) {
//                    System.out.printf("%s ", g[i][j]);
//                }
//                System.out.println("");
//            }
            ArrayList<Pair<Integer, Integer>> q = new ArrayList<>();
            q.add(Pair.of(0, n - 1));
            for (int x = 0; x < q.size(); x++) {
                int i = q.get(x).getLeft();
                int j = q.get(x).getRight();
                if (g[i][j] != null) {
                    for (int k : g[i][j]) {
                        q.add(Pair.of(i, k));
                        q.add(Pair.of(k + 1, j));
                    }
                }
            }
//            System.out.println(q);
            for (Pair<Integer, Integer> p : q) {
                if (p.getLeft() != p.getRight()) {
//                    best.add(Pair.of(p.getLeft() + range.left, p.getRight() + range.left));
                    ss.add(Pair.of(p.getLeft() + range.left, p.getRight() + range.left));
                }
            }
        }
        System.out.println(ss);

        return ss;
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

//    void showAcnodeGraph() {
//        ArrayList<Pair<Integer, Integer>> ranges = new ArrayList<>();
//        ranges.addAll(range2acnode.keySet());
//        ranges.sort(Comparator.comparingInt((Pair<Integer, Integer> a) -> (a.getRight() - a.getLeft())).thenComparingInt(Pair::getLeft));
//        System.out.println(ranges);
//        for (Pair<Integer, Integer> boundery : ranges) {
//            System.out.println("====================");
//            ACNode acNode = range2acnode.get(boundery);
//            System.out.println(boundery + " " + acNode.operatorNodes.size());
//            for (OperatorNode node : acNode.operatorNodes) {
//                System.out.println(node);
//            }
//        }
//        System.out.println("x");
//        //     System.exit(0);
//    }


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


    void showBest(Pair<Integer, Integer> range, OperatorNode bestsinglecsenode) {
        System.out.println("range: " + range);
        ArrayList<OperatorNode> list2 = new ArrayList<>();
        //            if (e.getValue().dependencies.size() > 2) {
        //                System.out.println("dependencies size > 2");
        //            }
        list2.addAll(range2acnode.get(range).uncertainACs.values());
        list2.sort(Comparator.comparingDouble(a -> a.accCost));
//        && list2.get(i).accCost <= bestsinglecsenode.accCost
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
                //  root.dependencies.addAll(root.inputs.get(i).dependencies);
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
        // double accCost = 0;
        double thisCost = 0;
        if (node.hops.get(0) instanceof BinaryOp) {
            System.out.println("x");
        }
        if (node.inputs.size() == 0) {
            node.accCost = 0;
        }
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorCost(node.inputs.get(i), visited);
            //  accCost += node.inputs.get(i).accCost;
        }
        thisCost = NodeCostEstimator.getNodeCost(node);

        if (!range2acnode.containsKey(node.range)) {
            ACNode acNode = new ACNode();
            acNode.range = node.range;
            OperatorNode node1 = node.copyWithoutDependencies();
            node1.thisCost = thisCost;
            acNode.getOperatorNodes().add(node1);
//                acNode.addOperatorNode(node1);
            range2acnode.put(node.range, acNode);
        }

        // accCost += thisCost;
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
            thisCost = thisCost / csesize;
            //    accCost = accCost / csesize;
        }
        if (node.isConstant) {
            thisCost /= 100;
            //  accCost /= 100;
        }
        //node.accCost = accCost;
        node.thisCost = thisCost;
        //  System.out.println(node);
//        range2acnode.get(node.range).addOperatorNode(node);

        visited.add(node);
    }


    void addOperatorNodeToTable(OperatorNode node, HashSet<OperatorNode> visited) {
        // node.accCost = node.thisCost;
        System.out.println("add node to table: " + node.range);

        if (!range2acnode.containsKey(node.range)) {
            ACNode acNode = new ACNode();
            acNode.range = node.range;
            acNode.addOperatorNode(node);
            //acNode.operatorNodes.add(node);
            range2acnode.put(node.range, acNode);
        } else {
            ACNode acNode = range2acnode.get(node.range);
            acNode.addOperatorNode(node);
        }
        for (int i = 0; i < node.inputs.size(); i++) {
            addOperatorNodeToTable(node.inputs.get(i), visited);
        }
//        if (node.inputs.size() == 0) {
//            Pair<Integer, Integer> range = node.range;
//            if (!range2acnode.containsKey(node.range)) {
//                ACNode acNode = new ACNode();
//                acNode.range = node.range;
//                //acNode.operatorNodes.add(node);
//                range2acnode.put(node.range, acNode);
//            }
//        } else {
//            for (int i = 0; i < node.inputs.size(); i++) {
//                addOperatorNodeToTable(node.inputs.get(i), visited);
//            }
//
//            Pair<Integer, Integer> range = node.range;
//            if (range2acnode.containsKey(range)) {
//                ACNode acNode = range2acnode.get(range);
//                acNode.operatorNodes.add(node);
//            } else {
//                ACNode acNode = new ACNode();
//                acNode.range = range;
//                acNode.operatorNodes.add(node);
//                range2acnode.put(range, acNode);
//            }
//        }
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
            ArrayList<OperatorNode> a1 = range2acnode.get(range).getOperatorNodes();
            if (a1.size() < 2) continue;
            OperatorNode operatorNode = a1.get(0);
            if (operatorNode.hops.size() < 1) continue;
            Hop hop = operatorNode.hops.get(0);
            if (hop instanceof BinaryOp) {
                operatorNode.dependencies = new HashSet<>();
                range2acnode.get(range).getOperatorNodes().clear();
                range2acnode.get(range).getOperatorNodes().add(operatorNode);
                continue;
            }
            ArrayList<OperatorNode> a2 = new ArrayList<>();
            for (OperatorNode node1 : a1) {
                boolean ok = true;
                for (OperatorNode node2 : a2) {
                    if (node1.dependencies.equals(node2.dependencies)) { // todo: check dRange
                        ok = false;
                        break;
                    }
                }
                if (ok) a2.add(node1);
            }
            HashMap<HashSet<SingleCse>,OperatorNode> a3 = new HashMap<>();
            for (OperatorNode node: a1) {
                if (!a3.containsKey(node.dependencies))
                    a3.put(node.dependencies,node);
            }
            System.out.println("range=" + range + " size(a1)=" + a1.size() + " size(a2)=" + a2.size()+ " size(a3)=" + a3.size());
         //   a2.sort(Comparator.comparing(c -> c.accCost));
            range2acnode.get(range).setOperatorNodes(new ArrayList<>(a3.values()));
        }
       //   System.exit(0);
    }


    HashMap<Pair<Integer, Integer>, ACNode> range2acnode = new HashMap<>();


    void classifyOperatorNode(CseStateMaintainer MAINTAINER, ArrayList<OperatorNode> allResults, ACNode acNode) {
        acNode.uncertainACs = new HashMap<>();
//        acNode.uncertainACs = new ArrayList<>();
        int removed = 0, removed2 = 0;
        for (OperatorNode node : allResults) {
            boolean hasUselessCse = false;
            boolean hasUncertainCse = false;
            for (SingleCse cse : node.dependencies) {
                CseStateMaintainer.CseState state = MAINTAINER.getCseState(cse);
                if (state == CseStateMaintainer.CseState.certainlyUseless) {
                    hasUselessCse = true;
                }
                if (state == CseStateMaintainer.CseState.uncertain) {
                    hasUncertainCse = true;
                }
            }
            if (hasUselessCse) {
                removed++;
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
                if (acNode.certainAC == null || acNode.certainAC.accCost > node.accCost) {
                    acNode.certainAC = node;
                }
            }
            if (acNode.minAC == null || acNode.minAC.accCost > node.accCost) {
                acNode.minAC = node;
            }
        }
        System.out.println(acNode.range + " remove " + removed + " " + removed2);
    }

    void selectBest(CseStateMaintainer MAINTAINER) {
//        dp = new HashMap<>();
        ArrayList<Pair<Integer, Integer>> sortedRanges = new ArrayList<>(range2acnode.keySet());
//        ranges.sort(Comparator.comparingInt((Pair<Integer,Integer> a)->(a.getRight()-a.getLeft())));
        sortedRanges.sort(Comparator.comparingInt((Pair<Integer, Integer> a) -> (a.getRight() - a.getLeft())).thenComparingInt(Pair::getLeft));
        System.out.println(sortedRanges);
        for (Pair<Integer, Integer> boundery : sortedRanges) {
//            if (boundery.getRight()-boundery.getLeft()>2) break;
            if (boundery.getLeft() == 1 && boundery.getRight() == 19) {
                System.out.println("x");
            }


            ACNode acNode = range2acnode.get(boundery);

            /*
            ArrayList<OperatorNode> allResults = new ArrayList<>();

            for (Pair<Pair<Integer,Integer>,Pair<Integer,Integer>> drange: acNode.drange2operatornodes.keySet()) {
                Pair<Integer, Integer> lRange = drange.getLeft();
                Pair<Integer, Integer> rRange = drange.getRight();
                if (lRange == null || rRange == null) continue;
                if (lRange.equals(boundery) || rRange.equals(boundery)) continue;
                ACNode lac = range2acnode.get(lRange);
                ACNode rac = range2acnode.get(rRange);
                if (lac == null || rac == null) continue;
                ArrayList<OperatorNode> lops = lac.getOperatorNodes();
                ArrayList<OperatorNode> rops = rac.getOperatorNodes();
                ArrayList<OperatorNode> mids = acNode.getOperatorNodes(drange);
                System.out.println("0.1 "+lops.size()+" "+rops.size()+" "+mids.size());

                for (OperatorNode operatorNode: mids) {
                    for (OperatorNode operatorNode1 : lops) {
                        if (operatorNode1 == null) continue;
                        for (OperatorNode operatorNode2 : rops) {
                            if (operatorNode2 == null) continue;
                            if (check(operatorNode1, operatorNode2, operatorNode.dependencies)) {
                              OperatorNode  tmp = createOperatorNode(operatorNode1, lRange, operatorNode2, rRange, operatorNode, boundery);
                                if (tmp != null) {  // && testttt(tmp.dependencies)
                                    allResults.add(tmp);
                                    //  System.out.println(tmp);
                                }
                            }
                        }
                    }
                }
            }
            */


            ArrayList<OperatorNode> operatorNodes = acNode.getOperatorNodes();
            ArrayList<OperatorNode> allResults = new ArrayList<>();

            boolean first = true;

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
                    if (tmp != null) allResults.add(tmp);  // && testttt(tmp.dependencies)
                    ArrayList<OperatorNode> lops = new ArrayList<>();
                    ArrayList<OperatorNode> rops = new ArrayList<>();
                    lops.addAll(lac.uncertainACs.values());
                    lops.addAll(lac.getOperatorNodes());
                    lops.add(lac.certainAC);
                    rops.addAll(rac.uncertainACs.values());
                    rops.addAll(rac.getOperatorNodes());
                    rops.add(rac.certainAC);

//                    if (first) {
//                        System.out.println("bounderyyyy: "+boundery);
//                        System.out.println(lops);
//                        System.out.println("----");
//                        System.out.println(rops);
//                        System.out.println("----");
//                        first = false;
//                    }
//                    if (boundery.getLeft()==14&&boundery.getRight()==19) {
//                        System.out.println(lops);
//                        System.out.println("----");
//                        System.out.println(rops);
//                        System.out.println("----");
//                    }

                    System.out.println("lRange:" + lRange + " rRange:" + rRange + " lops:" + lops.size() + " rops:" + rops.size());
                    for (OperatorNode operatorNode1 : lops) {
                        if (operatorNode1 == null) continue;
                        for (OperatorNode operatorNode2 : rops) {
                            if (operatorNode2 == null) continue;

//                            if (testttt(operatorNode1.dependencies)&&testttt(operatorNode2.dependencies)&&check(operatorNode1, operatorNode2, operatorNode.dependencies)) {
////                                System.out.println(operatorNode1.dependencies);
////                                System.out.println(operatorNode2.dependencies);
////                                System.out.println("x");
//                                    has15 = true;
//                            }

                            if (check(operatorNode1, operatorNode2, operatorNode.dependencies)) {
                                tmp = createOperatorNode(operatorNode1, lRange, operatorNode2, rRange, operatorNode, boundery);
                                if (tmp != null) {  // && testttt(tmp.dependencies)
                                    allResults.add(tmp);
                                    //  System.out.println(tmp);
                                }
                            }
                        }
                    }
                    System.out.println("0.1 " + lRange + " " + rRange + " " + lops.size() + " " + rops.size() + " " + acNode.getOperatorNodes().size());
                    System.out.println("0.2 " + allResults.size());
                }
//                if (operatorNode.range.getLeft()==6 && operatorNode.range.getRight()==10) {
//                    System.out.println("size="+allResults.size());
//                    System.out.println(allResults);
//                    System.out.println("size="+acNode.operatorNodes.size());
//                    System.out.println(acNode.operatorNodes);
//                    System.out.println("x");
//                }
            }

            System.out.println("1. " + allResults.size());
//            System.out.println("2. " + acNode.operatorNodes.size());
//            allResults.addAll(acNode.operatorNodes);

            classifyOperatorNode(MAINTAINER, allResults, acNode);

            MAINTAINER.updateCseState(acNode, range2acnode);

            if (!range2acnode.containsKey(boundery)) {
                range2acnode.put(boundery, new ACNode());
            } else {
                System.out.println("boundery=" + boundery + " = " + range2acnode.get(boundery).uncertainACs.size());
            }

            int has23 = 0;
            for (OperatorNode node : acNode.uncertainACs.values()) {
                if (testttt(node.dependencies)) {
                    has23++;
                }
            }

            System.out.println(boundery + " has23 " + has23);

            if (boundery.getLeft() == 1 && boundery.getRight() == 10) {
                ACNode acNode1 = range2acnode.get(boundery);
                System.out.println("x");
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

    boolean testttt(HashSet<SingleCse> cses) {
        for (SingleCse cse : cses) {
            for (Range range : cse.ranges) {
                if (range.left == 2 && range.right == 3) {
                    return true;
                }
            }
        }
        return false;
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
        if (  //lNode.accCost >= Double.MAX_VALUE / 2 ||
            //  rNode.accCost >= Double.MAX_VALUE / 2 ||
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
