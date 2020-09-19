package org.apache.sysds.hops.rewrite.dfp.rule.jiaohuan;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

/*
 * a+b -> b+a
 */
public class JiaoHuanRule extends MyRule {
     private Types.OpOp2 operator;

     public JiaoHuanRule(Types.OpOp2 operator ) {
         this.operator = operator;
     }

    @Override
    public Hop apply(Hop parent, Hop hop, int pos) {
        // a+b -> b+a
        if (HopRewriteUtils.isBinary(hop, this.operator)) {
            Hop a = hop.getInput().get(0);
            Hop b = hop.getInput().get(1);
            Hop ba = HopRewriteUtils.createBinary(b, a, this.operator);

            // replace
            if (parent != null) {
                HopRewriteUtils.replaceChildReference(parent, hop, ba);
                HopRewriteUtils.cleanupUnreferenced(hop);
            }
            hop = ba;
        }
        return hop;
    }
}
