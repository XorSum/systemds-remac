package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ACNode {

    Pair<Integer,Integer> range = null;
    ArrayList<OperatorNode> operatorNodes = new ArrayList<>();
    OperatorNode minAC = null;
    OperatorNode certainAC = null;
    //ArrayList<OperatorNode> uncertainACs = new ArrayList<>();

    HashMap<HashSet<SingleCse>,OperatorNode> uncertainACs = new HashMap<>();

    void addUncertainAC(OperatorNode node)  {
        if (uncertainACs.containsKey(node.dependencies)) {
            if (uncertainACs.get(node.dependencies).accCost>node.accCost) {
                uncertainACs.put(node.dependencies,node);
            }
        }else {
            uncertainACs.put(node.dependencies,node);
        }
    }
}
