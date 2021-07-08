package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

public class SinglePlan {
    public SingleCse singleCse;
    public Hop hop;
    public OperatorNode node;
    public SinglePlanTag tag;

    public enum SinglePlanTag {
        Useful, Useless, uncertain, constant
    }
}
