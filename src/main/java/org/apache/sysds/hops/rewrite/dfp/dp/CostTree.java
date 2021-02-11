package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types;
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

    public CostTree(VariableSet variablesUpdated, long iterationNumber) {
        this.variablesUpdated = variablesUpdated;
        this.iterationNumber = iterationNumber;
        this.nodeCostEstimator = new NodeCostEstimator();
    }

    NodeCostEstimator nodeCostEstimator;
    long iterationNumber = 2;
    public VariableSet variablesUpdated = null;


    public ACNode testOperatorGraph(ArrayList<SinglePlan> pairs, Pair<SingleCse, Hop> emptyPair, ArrayList<Range> blockRanges, ArrayList<Leaf> leaves) {

        int maxIndex = 0;
        HashSet<Pair<Integer, Integer>> ranges = new HashSet<>();

        OperatorNode emptyNode = createOperatorGraph(emptyPair.getRight(), false);
        analyzeOperatorRange(emptyNode, emptyPair.getLeft(), new MutableInt(0));
        rGetRanges(emptyNode, ranges);

        for (SinglePlan p : pairs) {
            SingleCse cse = p.singleCse;
            Hop hop = p.hop;
            System.out.println("========================");
            OperatorNode node = createOperatorGraph(hop, false);
            MutableInt mutableInt = new MutableInt(0);
            analyzeOperatorRange(node, cse, mutableInt);
            boolean certainused = rCheckCertainUsed(cse, ranges);
            if (checkConstant(cse, leaves)) {
                p.tag = SinglePlan.SinglePlanTag.constant;
                System.out.println("Constant Cse: " + cse);
            } else {
                if (certainused) {
                    p.tag = SinglePlan.SinglePlanTag.Useful;
                    System.out.println("Certainly Useful: " + cse);
                    // continue;
                } else {
                    p.tag = SinglePlan.SinglePlanTag.uncertain;
                    System.out.println("Uncertain: " + cse);
                }
            }
            maxIndex = Math.max(maxIndex, mutableInt.getValue() - 1);
            analyzeOperatorConstant(node);
            System.out.println(cse);
            analyzeOperatorCost(node, new HashSet<>());
            p.node = node;
        }

        CseStateMaintainer MAINTAINER = new CseStateMaintainer();
        MAINTAINER.initRangeCounter(range2acnode);
        MAINTAINER.initCseState(pairs);
        selectBest(MAINTAINER);

        showBest(Pair.of(0, maxIndex));

        System.out.println(Explain.explain(emptyPair.getRight()));

        System.out.println("done");

        return range2acnode.get(Pair.of(0, maxIndex));
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


    static void explainOperatorNode(OperatorNode node, int d) {
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
        //double accCost = 0;
        double thisCost = 0;
//        if (node.hops.get(0) instanceof BinaryOp) {
//            System.out.println("x");
//        }
        if (node.inputs.size() == 0) {
            node.accCost = 0;
        }
        for (int i = 0; i < node.inputs.size(); i++) {
            analyzeOperatorCost(node.inputs.get(i), visited);
            //  accCost += node.inputs.get(i).accCost;
        }
        thisCost = this.nodeCostEstimator.getNodeCost(node);

        if (!range2acnode.containsKey(node.range)) {
            ACNode acNode = new ACNode();
            acNode.range = node.range;
            OperatorNode node1 = node.copyWithoutDependencies();
            node1.thisCost = thisCost;
            //   node1.accCost = accCost;
            //   acNode.operatorNodes.add(node1);
            acNode.addOperatorNode(node1);
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
            //   accCost = accCost / csesize;
        }
        if (node.isConstant) {
            //todo: iterationNumber
            thisCost /= iterationNumber;
//            thisCost /= 100;
            //  accCost /= 100;
        }
        //  node.accCost = accCost;
        node.thisCost = thisCost;
        //  System.out.println(node);
        range2acnode.get(node.range).addOperatorNode(node);
        visited.add(node);
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
                if (acNode.certainAC == null || acNode.certainAC.accCost > node.accCost) {
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
        System.out.println(acNode.range + " remove " + removed1 + " " + removed2);
    }

    void selectBest(CseStateMaintainer MAINTAINER) {
//        dp = new HashMap<>();
        ArrayList<Pair<Integer, Integer>> sortedRanges = new ArrayList<>(range2acnode.keySet());
//        ranges.sort(Comparator.comparingInt((Pair<Integer,Integer> a)->(a.getRight()-a.getLeft())));
        sortedRanges.sort(Comparator.comparingInt((Pair<Integer, Integer> a) -> (a.getRight() - a.getLeft())).thenComparingInt(Pair::getLeft));
        System.out.println(sortedRanges);
        for (Pair<Integer, Integer> boundery : sortedRanges) {
            System.out.println("boundery: " + boundery);
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
                Collection<OperatorNode> mids = acNode.drange2operatornodes.get(drange).values();
                System.out.println("0.1 " + lops.size() + " " + rops.size() + " " + mids.size());

                for (OperatorNode operatorNode : mids) {
                    for (OperatorNode operatorNode1 : lops) {
                        if (operatorNode1 == null) continue;
                        for (OperatorNode operatorNode2 : rops) {
                            if (operatorNode2 == null) continue;
                            if (check(operatorNode1, operatorNode2, operatorNode.dependencies)) {
                                OperatorNode tmp = createOperatorNode(operatorNode1, lRange, operatorNode2, rRange, operatorNode, boundery);
                                if (tmp != null) {  // && testttt(tmp.dependencies)
                                    allResults.add(tmp);
                                    //  System.out.println(tmp);
                                }
                            }
                        }
                    }
                }
            }
            System.out.println("1. " + allResults.size());

            classifyOperatorNode(MAINTAINER, allResults, acNode);

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

            if (!range2acnode.containsKey(boundery)) {
                range2acnode.put(boundery, new ACNode());
            } else {
                System.out.println("boundery=" + boundery + " = " + range2acnode.get(boundery).uncertainACs.size());
            }
            System.out.println(boundery + " min ac: ");
            System.out.println(acNode.minAC);
            MAINTAINER.printCseNumStats();


//            if (boundery.getLeft() == 1 && boundery.getRight() == 10) {
//                ACNode acNode1 = range2acnode.get(boundery);
//                System.out.println("x");
//            }


        }

        for (Pair<Integer, Integer> range : sortedRanges) {
            if (!range2acnode.containsKey(range)) {
                System.out.println(range + " " + 0);
            } else {
                System.out.println(range + " " + range2acnode.get(range).uncertainACs.size());
            }
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
