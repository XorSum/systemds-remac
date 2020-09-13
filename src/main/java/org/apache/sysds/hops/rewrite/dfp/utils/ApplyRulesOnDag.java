package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

import java.util.List;
import java.util.Random;

public class ApplyRulesOnDag {

    private static int count;

    public static Hop applyDAGRule(Hop hop, List<MyRule> rules, int max_count, boolean isrand) {
        count = max_count;
        hop.resetVisitStatus();
        hop = apply_dag_rule_iter(null, hop, 0, rules, false, isrand);
        hop.resetVisitStatus();
        hop = apply_dag_rule_iter(null, hop, 0, rules, true, isrand);
        return hop;
    }

    private static Hop apply_dag_rule_iter(Hop parent, Hop hop, int pos, List<MyRule> rules, boolean descendFirst, boolean isrand) {
        if (count <= 0) return hop;
        if (hop.isVisited())
            return hop;
        if (descendFirst) {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop hi = hop.getInput().get(i);
                hi = apply_dag_rule_iter(hop, hi, i, rules, descendFirst, isrand);
            }
        }
        for (MyRule rule : rules) {
            if (isrand) {
                Random ran1 = new Random();
                if (ran1.nextBoolean())
                    hop = rule.apply(parent, hop, pos);
                count = count - 1;
            } else {
                hop = rule.apply(parent, hop, pos);
                count = count - 1;
            }
        }
        if (!descendFirst) {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop hi = hop.getInput().get(i);
                hi = apply_dag_rule_iter(hop, hi, i, rules, descendFirst, isrand);
            }
        }
        hop.setVisited();
        return hop;
    }

}
