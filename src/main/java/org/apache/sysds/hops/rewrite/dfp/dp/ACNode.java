package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ACNode {

    public Pair<Integer, Integer> range = null;

    ConcurrentHashMap<DRange, ConcurrentHashMap<HashSet<SingleCse>, OperatorNode>> drange2operatornodes = new ConcurrentHashMap<>();
    ConcurrentHashMap<DRange, OperatorNode> drange2emptynodes = new ConcurrentHashMap<>();

    public OperatorNode minAC = null;
    public OperatorNode certainAC = null;
    public ConcurrentHashMap<HashSet<SingleCse>, OperatorNode> uncertainACs = new ConcurrentHashMap<>();


    public ACNode(Pair<Integer, Integer> range) {
        this.range = range;
    }

    void addEmptyOperatorNode(OperatorNode node) {
        drange2emptynodes.computeIfPresent(node.dRange, ((dRange, node1) -> {
            if (node1.thisCost > node.thisCost) return node;
            else return node1;
        }));
        drange2emptynodes.putIfAbsent(node.dRange, node);
    }

    Collection<OperatorNode> getEmptyOperatorNodes() {
        return drange2emptynodes.values();
    }

    void addOperatorNode(OperatorNode node) {
        DRange p2 = node.dRange;
        drange2operatornodes.putIfAbsent(p2, new ConcurrentHashMap<>());
        ConcurrentHashMap<HashSet<SingleCse>, OperatorNode> cses2node = drange2operatornodes.get(p2);
        cses2node.computeIfPresent(node.dependencies, (cses, node1) -> {
            if (node.lessThan(node1)) {
                return node;
            } else {
                return node1;
            }
        });
        cses2node.putIfAbsent(node.dependencies, node);
    }

    ArrayList<OperatorNode> getOperatorNodes(CseStateMaintainer maintainer) {
        uncertainACs.entrySet().removeIf(entry -> maintainer.hasUselessCse(entry.getKey()));
        ArrayList<OperatorNode> ops = new ArrayList<>(uncertainACs.values());
        if (certainAC != null) ops.add(certainAC);
        for (Map<HashSet<SingleCse>, OperatorNode> x : drange2operatornodes.values()) {
            for (OperatorNode node : x.values()) {
                if (node.accCost < Double.MAX_VALUE / 2) {
                    if (!maintainer.hasUselessCse(node.dependencies))
                        ops.add(node);
                }
            }
        }
        return ops;
    }


    void addUncertainAC(OperatorNode node) {
        uncertainACs.computeIfPresent(node.dependencies, (cse, node1) -> {
            if (node.lessThan(node1)) return node;
            else return node1;
        });
        uncertainACs.putIfAbsent(node.dependencies, node);
    }


}
