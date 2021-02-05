package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.*;

public class ACNode {

    Pair<Integer, Integer> range = null;
    // ArrayList<OperatorNode> operatorNodes = new ArrayList<>();

    //todo: HashMap< ,ArrayList<OperatorNode> > drange2operatornodes =new HashMap<>();
    HashMap<Pair<Pair<Integer, Integer>, Pair<Integer, Integer>>, HashMap<HashSet<SingleCse>, OperatorNode>> drange2operatornodes = new HashMap<>();

    void addOperatorNode(OperatorNode node) {
        Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> p2;
        if (node.inputs.size() == 2) {
            Pair<Integer, Integer> p0 = node.inputs.get(0).range;
            Pair<Integer, Integer> p1 = node.inputs.get(1).range;
            p2 = Pair.of(p0, p1);
        } else {
            p2 = Pair.of(node.range, node.range);
        }
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
        for (HashMap<HashSet<SingleCse>, OperatorNode> x : drange2operatornodes.values()) {
           for (OperatorNode node:x.values()) {
                if (node.accCost<Double.MAX_VALUE/2) {
                    if (!maintainer.hasUselessCse(node.dependencies))
                        ops.add(node);
                }
           }
        }
        return ops;
    }


    OperatorNode minAC = null;
    OperatorNode certainAC = null;
    HashMap<HashSet<SingleCse>, OperatorNode> certainACs = new HashMap<>();
    // todo: certainACs

//    ArrayList<OperatorNode> uncertainACs = new ArrayList<>();

    HashMap<HashSet<SingleCse>, OperatorNode> uncertainACs = new HashMap<>();

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
