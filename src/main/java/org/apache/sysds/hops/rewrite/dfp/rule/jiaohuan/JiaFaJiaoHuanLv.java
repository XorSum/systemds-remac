package org.apache.sysds.hops.rewrite.dfp.rule.jiaohuan;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.utils.Explain;

public class JiaFaJiaoHuanLv extends MyRule {
    @Override
    public Hop apply(Hop parent, Hop hop, int pos) {
        // a+b -> b+a
        if (HopRewriteUtils.isBinary(hop, Types.OpOp2.PLUS)) {
            Hop a = hop.getInput().get(0);
            Hop b = hop.getInput().get(1);
            Hop ba = HopRewriteUtils.createBinary(b, a, Types.OpOp2.PLUS);

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
