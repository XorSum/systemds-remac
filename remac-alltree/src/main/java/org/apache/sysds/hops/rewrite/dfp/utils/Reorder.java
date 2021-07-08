package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.RemoveUnnecessaryTransposeRule;
import org.apache.sysds.hops.rewrite.dfp.rule.fenpei.FenpeiRuleLeft;
import org.apache.sysds.hops.rewrite.dfp.rule.fenpei.FenpeiRuleRight;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.scalar.ScalarLeftMoveRule;
import org.apache.sysds.hops.rewrite.dfp.rule.scalar.ScalarRightMoveRule;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeMatrixMatrixMultSplitRule;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeMinusSplitRule;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposePlusSplitRule;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeScalarMatrixMultSplitRule;

import java.util.ArrayList;

import static org.apache.sysds.hops.rewrite.dfp.utils.ApplyRulesOnDag.applyDAGRule;
import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashHopDag;

public class Reorder {
    public static Hop reorder(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new TransposeMatrixMatrixMultSplitRule());
        rules.add(new TransposePlusSplitRule());
        rules.add(new TransposeMinusSplitRule());
        rules.add(new RemoveUnnecessaryTransposeRule());
        rules.add(new MatrixMultJieheRule());
        rules.add(new FenpeiRuleLeft(Types.OpOp2.MINUS));
        rules.add(new FenpeiRuleLeft(Types.OpOp2.PLUS));
        rules.add(new FenpeiRuleRight(Types.OpOp2.MINUS));
        rules.add(new FenpeiRuleRight(Types.OpOp2.PLUS));
        rules.add(new ScalarLeftMoveRule());
        rules.add(new ScalarRightMoveRule());
        rules.add(new TransposeScalarMatrixMultSplitRule());
        Pair<Long, Long> hash1 = null;
        Pair<Long, Long> hash2 = hashHopDag(hop);
        do {
            hop = applyDAGRule(hop, rules, 10000, false);
            hash1 = hash2;
            hash2 = hashHopDag(hop);
        } while (hash2.compareTo(hash1) != 0);
        return hop;
    }
}
