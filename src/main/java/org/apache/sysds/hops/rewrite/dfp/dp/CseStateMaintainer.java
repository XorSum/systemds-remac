package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.expressions.Sin;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class CseStateMaintainer {


    Counter<Pair<Integer, Integer>> rangeCounter = new Counter<>();
    Counter<SingleCse> cseCounter = new Counter<>();

    enum CseState {
        certainlyUseful, certainlyUseless, uncertain, constant
    }

    HashMap<SingleCse, CseState> map = new HashMap<>();

    void initCseState(ArrayList<SingleCse> allCses , ArrayList<SingleCse> certainlyUsefulCses) {
        map.clear();
        for (SingleCse cse: allCses) {
            map.put(cse,CseState.uncertain);
        }
        for (SingleCse cse: certainlyUsefulCses) {
            map.put(cse, CseState.certainlyUseful);
        }
    }

    void initRangeCounter(HashMap<Pair<Integer, Integer>, ACNode> range2acnode) {
        for (ACNode acNode : range2acnode.values()) {
            for (OperatorNode node : acNode.operatorNodes) {
                for (OperatorNode in : node.inputs) {
                    rangeCounter.increment(in.range);
                }
            }
        }
    }

    void setCseState(SingleCse cse, CseState state) {
        map.put(cse,state);
    }

    CseState getCseState(SingleCse cse) {
        return null;
    }

    void updateCseState(ACNode acNode, HashMap<Pair<Integer, Integer>, ACNode> range2acnode) {
        if (acNode.minAC != null) {
            for (SingleCse cse : acNode.minAC.dependencies) {
                cseCounter.increment(cse);
            }
        }
        for (OperatorNode node : acNode.operatorNodes) {
            for (OperatorNode in : node.inputs) {
                rangeCounter.decrement(in.range);
                if (rangeCounter.getValue(in.range) == 0) {
                    ACNode acNode1 = range2acnode.get(in.range);
                    if (acNode1.minAC != null) {
                        for (SingleCse cse : acNode1.minAC.dependencies) {
                            cseCounter.decrement(cse);
                        }
                    }
                }
            }
        }
    }

}
