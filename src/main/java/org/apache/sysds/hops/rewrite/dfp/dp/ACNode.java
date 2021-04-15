package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.*;

public class ACNode {

    public Pair<Integer, Integer> range = null;
    // ArrayList<OperatorNode> operatorNodes = new ArrayList<>();

    HashMap<DRange, HashMap<HashSet<SingleCse>, OperatorNode>> drange2operatornodes = new HashMap<>();
    HashMap<DRange,OperatorNode> drange2emptynodes = new HashMap<>();

    void addEmptyOperatorNode(OperatorNode node) {
        if (!drange2emptynodes.containsKey(node.dRange)) {
            drange2emptynodes.put(node.dRange,node);
        } else {
            OperatorNode tmp = drange2emptynodes.get(node.dRange);
            if (tmp.thisCost> node.thisCost) {
                drange2emptynodes.put(node.dRange, node);
            }
        }
    }

    Collection<OperatorNode> getEmptyOperatorNodes() {
        return drange2emptynodes.values();
    }

    void addOperatorNode(OperatorNode node) {
        DRange p2 = node.dRange;
        HashMap<HashSet<SingleCse>, OperatorNode> cses2node;
        if (drange2operatornodes.containsKey(p2)) {
            cses2node = drange2operatornodes.get(p2);
        } else {
            cses2node = new HashMap<>();
        }
        if (cses2node.containsKey(node.dependencies)) {
            if (cses2node.get(node.dependencies).thisCost > node.thisCost) {
                cses2node.put(node.dependencies, node);
            }
        } else {
            cses2node.put(node.dependencies, node);
        }
        drange2operatornodes.put(p2, cses2node);
    }

    ArrayList<OperatorNode> getOperatorNodes(CseStateMaintainer maintainer) {
        uncertainACs.entrySet().removeIf(entry -> maintainer.hasUselessCse(entry.getKey()));
        ArrayList<OperatorNode> ops = new ArrayList<>(uncertainACs.values());
        if (certainAC != null) ops.add(certainAC);
        if (emptyOpnode!=null) ops.add(emptyOpnode);
        for (HashMap<HashSet<SingleCse>, OperatorNode> x : drange2operatornodes.values()) {
            for (OperatorNode node : x.values()) {
                if (node.accCost < Double.MAX_VALUE / 2) {
                    if (!maintainer.hasUselessCse(node.dependencies))
                        ops.add(node);
                }
            }
        }
        return ops;
    }

    public OperatorNode emptyOpnode = null;
    public OperatorNode minAC = null;
    public OperatorNode certainAC = null;
    public HashMap<HashSet<SingleCse>, OperatorNode> certainACs = new HashMap<>();
    // todo: certainACs

//    ArrayList<OperatorNode> uncertainACs = new ArrayList<>();

    public HashMap<HashSet<SingleCse>, OperatorNode> uncertainACs = new HashMap<>();

    void addUncertainAC(OperatorNode node) {
        if (uncertainACs.containsKey(node.dependencies)) {
            if (uncertainACs.get(node.dependencies).accCost > node.accCost) {
                uncertainACs.put(node.dependencies, node);
            }
        } else {
            uncertainACs.put(node.dependencies, node);
        }
//        boolean insert = true;
//        for (OperatorNode node1: uncertainACs) {
//            boolean x = true;
//            for (SingleCse s:node.dependencies) {
//                if (!node1.dependencies.contains(s)) {
//                    x=false;break;
//                }
//            }
//            for (SingleCse s:node1.dependencies) {
//                if (!node.dependencies.contains(s)) {
//                    x=false;break;
//                }
//            }
//            if (x) {
//                insert = false;break;
//            }
////            if (node.dependencies == node1.dependencies) {
////                insert = false;break;
////            }
//        }
//        if (insert) uncertainACs.add(node);
    }


}
