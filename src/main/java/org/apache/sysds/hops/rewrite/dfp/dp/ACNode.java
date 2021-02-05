package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ACNode {

    Pair<Integer, Integer> range = null;

        private    ArrayList<OperatorNode> operatorNodes = new ArrayList<>();
//    private HashMap<HashSet<SingleCse>, OperatorNode> cse2operatorNodes = new HashMap<>();


    void addOperatorNode(OperatorNode node) {
        operatorNodes.add(node);
//        if (!cse2operatorNodes.containsKey(node.dependencies)) {
//            cse2operatorNodes.put(node.dependencies, node);
//        }
//        else {
//          OperatorNode node1 = cse2operatorNodes.get(node.dependencies);
//          if (node1.thisCost>node.thisCost) {
//              cse2operatorNodes.put(node.dependencies,node);
//          }
//        }
    }

    ArrayList<OperatorNode> getOperatorNodes() {
        return operatorNodes;
//        return new ArrayList<>(cse2operatorNodes.values());
//        HashMap<HashSet<SingleCse>,OperatorNode> a3 = new HashMap<>();
//        for (OperatorNode node: operatorNodes) {
//            if (!a3.containsKey(node.dependencies))
//                a3.put(node.dependencies,node);
//        }
//        return new ArrayList<>(a3.values());
    }

    void setOperatorNodes(ArrayList<OperatorNode> ops) {
        operatorNodes=ops;
//        cse2operatorNodes.clear();
//        for (OperatorNode node: ops) addOperatorNode(node);
    }

    /*
    HashMap<Pair<Pair<Integer, Integer>, Pair<Integer, Integer>>, HashMap<HashSet<SingleCse>, OperatorNode>> drange2operatornodes = new HashMap<>();
    HashMap<HashSet<SingleCse>, OperatorNode> cse2operatornode=new HashMap<>();


    void addOperatorNode(OperatorNode node) {
        if (cse2operatornode.containsKey(node.dependencies)) {
            if (cse2operatornode.get(node.dependencies).thisCost<=node.thisCost) return;
            for (HashMap<HashSet<SingleCse>, OperatorNode> map: drange2operatornodes.values()) {
                map.remove(node.dependencies);
            }
            cse2operatornode.put(node.dependencies,node);
        }

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

    ArrayList<OperatorNode> getOperatorNodes(Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> drange) {
        return new ArrayList<>(drange2operatornodes.get(drange).values());
    }

    ArrayList<OperatorNode> getOperatorNodes() {
        ArrayList<OperatorNode> ops = new ArrayList<>();
        ops.addAll(uncertainACs.values());
        if (certainAC != null) ops.add(certainAC);
        for (HashMap<HashSet<SingleCse>, OperatorNode> x : drange2operatornodes.values()) {
           for (OperatorNode node:x.values()) {
                if (node.accCost<Double.MAX_VALUE/2) {
                    ops.add(node);
                }
           }
        }
        return ops;
    }
   */

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
