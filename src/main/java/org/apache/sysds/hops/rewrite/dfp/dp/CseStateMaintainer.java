package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CseStateMaintainer {

    protected static final Log LOG = LogFactory.getLog(CseStateMaintainer.class.getName());

    Counter<Pair<Integer, Integer>> rangeCounter = new Counter<>();
    Counter<SingleCse> cseCounter = new Counter<>();

    enum CseState {
        certainlyUseful, certainlyUseless, uncertain, constant
    }

    HashMap<SingleCse, CseState> map = new HashMap<>();

    void initCseState(ArrayList<SinglePlan> allCses) {
        map.clear();
        for (SinglePlan p : allCses) {
            if (p.tag == SinglePlan.SinglePlanTag.Useful) {
                map.put(p.singleCse, CseState.certainlyUseful);
            } else if (p.tag == SinglePlan.SinglePlanTag.Useless) {
                map.put(p.singleCse, CseState.certainlyUseless);
            } else if (p.tag == SinglePlan.SinglePlanTag.constant) {
                map.put(p.singleCse, CseState.constant);
            } else {
                map.put(p.singleCse, CseState.uncertain);
            }
        }
    }

    void initRangeCounter(HashMap<Pair<Integer, Integer>, ACNode> range2acnode) {
        for (ACNode acNode : range2acnode.values()) {
            for (DRange drange : acNode.drange2operatornodes.keySet()) {
                rangeCounter.increment(drange.getLeftRange());
                rangeCounter.increment(drange.getRightRange());
            }
        }
//        for (ACNode acNode : range2acnode.values()) {
//            for (OperatorNode node : acNode.operatorNodes) {
//                for (OperatorNode in : node.inputs) {
//                    rangeCounter.increment(in.range);
//                }
//            }
//        }
    }

    void setCseState(SingleCse cse, CseState state) {
//        System.out.println("update cse state " + state + " " + cse);
        map.put(cse, state);
    }

    CseState getCseState(SingleCse cse) {
//        if (map.get(cse)==CseState.uncertain) {
//            if (cseCounter.getValue(cse) == 0) {
//                setCseState(cse, CseState.certainlyUseless);
//            }
//        }
        return map.get(cse);
//        return CseState.uncertain;
    }

    void updateCseState(ACNode acNode, HashMap<Pair<Integer, Integer>, ACNode> range2acnode) {

        if (acNode.minAC != null) {
            for (SingleCse cse : acNode.minAC.dependencies) {
                cseCounter.increment(cse);
            }
        }

        for (DRange drange : acNode.drange2operatornodes.keySet()) {
            Pair<Integer,Integer> leftRange = drange.getLeftRange();
            rangeCounter.decrement(leftRange);
            if (rangeCounter.getValue(leftRange) == 0) {
                if (rangeCounter.getValue(leftRange) == 0) {
                    ACNode acNode1 = range2acnode.get(leftRange);
                    if (acNode1.minAC != null) {
                        for (SingleCse cse : acNode1.minAC.dependencies) {
                            cseCounter.decrement(cse);
                            if (cseCounter.getValue(cse) == 0 && getCseState(cse) == CseState.uncertain) {
                                setCseState(cse, CseState.certainlyUseless);
                            }
                        }
                    }
                }
            }
            Pair<Integer,Integer> rightRange = drange.getRightRange();
            rangeCounter.decrement(rightRange);
            if (rangeCounter.getValue(rightRange) == 0) {
                if (rangeCounter.getValue(rightRange) == 0) {
                    ACNode acNode1 = range2acnode.get(rightRange);
                    if (acNode1.minAC != null) {
                        for (SingleCse cse : acNode1.minAC.dependencies) {
                            cseCounter.decrement(cse);
                            if (cseCounter.getValue(cse) == 0 && getCseState(cse) == CseState.uncertain) {
                                setCseState(cse, CseState.certainlyUseless);
                            }
                        }
                    }
                }
            }
        }


        //        if (acNode.minAC != null) {
//            for (SingleCse cse : acNode.minAC.dependencies) {
//                cseCounter.increment(cse);
//            }
//        }
//        for (OperatorNode node : acNode.operatorNodes) {
//            for (OperatorNode in : node.inputs) {
//                rangeCounter.decrement(in.range);
//                if (rangeCounter.getValue(in.range) == 0) {
//                    ACNode acNode1 = range2acnode.get(in.range);
//                    if (acNode1.minAC != null) {
//                        for (SingleCse cse : acNode1.minAC.dependencies) {
//                            cseCounter.decrement(cse);
//                            if (cseCounter.getValue(cse)==0) {
//                                setCseState(cse,CseState.certainlyUseless);
//                            }
//                        }
//                    }
//                }
//            }
//        }
    }

    boolean hasUncertain(HashSet<SingleCse> singleCses) {
        for (SingleCse cse : singleCses) {
            CseState state = getCseState(cse);
            if (state == CseState.uncertain || state==CseState.constant) {
                return true;
            }
        }
        return false;
    }

    boolean hasUselessCse(HashSet<SingleCse> singleCses) {
        for (SingleCse cse : singleCses) {
            if (getCseState(cse) == CseState.certainlyUseless) {
                return true;
            }
        }
        return false;
    }

    void printCseNumStats() {
        int uncertainNum = 0, usefulNum = 0, uselessNum = 0, constantNum = 0;

        for (Map.Entry<SingleCse, CseState> entry : map.entrySet()) {
            switch (entry.getValue()) {
                case uncertain:
                    uncertainNum++;
                    break;
                case certainlyUseful:
                    usefulNum++;
                    break;
                case certainlyUseless:
                    uselessNum++;
                    break;
                case constant:
                    constantNum++;
                    break;
            }
        }
        LOG.info("uncertain: " + uncertainNum + " useful: " + usefulNum + " useless: " + uselessNum);
    }

}
