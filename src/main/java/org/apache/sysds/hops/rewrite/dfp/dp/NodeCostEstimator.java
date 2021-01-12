package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.estim.SparsityEstimator;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

import java.util.HashMap;

public class NodeCostEstimator {

    static HashMap<Pair<Integer, Integer>, MMNode> range2mmnode = new HashMap<>();

    public static MMNode addOpnode2Mmnode(OperatorNode opnode) {
        if (opnode.mmNode!=null) return opnode.mmNode;
        for (Pair<Integer, Integer> r : opnode.ranges) {
            if (range2mmnode.containsKey(r)) {
                opnode.mmNode = range2mmnode.get(r);
                return opnode.mmNode;
            }
        }
        MMNode ans = null;
        if (HopRewriteUtils.isMatrixMultiply(opnode.hop)) {
            MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
            MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));
            ans = new MMNode(m0, m1, SparsityEstimator.OpCode.MM);
        } else if (HopRewriteUtils.isBinaryMatrixScalarOperation(opnode.hop)) {
            if (opnode.hop.getInput().get(0).isMatrix()) {
                ans = addOpnode2Mmnode(opnode.inputs.get(0));
            } else {
                ans = addOpnode2Mmnode(opnode.inputs.get(1));
            }
        } else if (HopRewriteUtils.isBinaryMatrixMatrixOperation(opnode.hop)) {
            MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
            MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));
            ans = new MMNode(m0, m1, SparsityEstimator.OpCode.PLUS);
        }
        if (ans != null) {
            for (Pair<Integer, Integer> r : opnode.ranges) {
                if (!range2mmnode.containsKey(r)) {
                    range2mmnode.put(r, ans);
                }
            }
            opnode.mmNode = ans;
        } else {
            System.out.println("mmnode == null");
            System.exit(0);
        }
        return ans;
    }

    public static double getNodeCost(OperatorNode opnode) {
        // MMNode mmnode = addOpnode2Mmnode(opnode);
        if (HopRewriteUtils.isMatrixMultiply(opnode.hop)) {
            MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
            MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));

        } else if (HopRewriteUtils.isBinaryMatrixScalarOperation(opnode.hop)) {
            if (opnode.hop.getInput().get(0).isMatrix()) {

            } else {

            }
        } else if (HopRewriteUtils.isBinaryMatrixMatrixOperation(opnode.hop)) {
            MMNode m0 = addOpnode2Mmnode(opnode.inputs.get(0));
            MMNode m1 = addOpnode2Mmnode(opnode.inputs.get(1));

        }
        return 0;
    }


}
