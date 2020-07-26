package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.common.Types.OpOp2;
import org.apache.sysds.hops.Hop;

public class FenpeiRuleLeft extends MyRule {
    private OpOp2 op1;
    private OpOp2 op2;

    public FenpeiRuleLeft(OpOp2 op1, OpOp2 op2) {
        this.op1 = op1;
        this.op2 = op2;
    }

    @Override
    public Hop apply(Hop parent, Hop hi, int pos) {



        return super.apply(parent, hi, pos);
    }
}
