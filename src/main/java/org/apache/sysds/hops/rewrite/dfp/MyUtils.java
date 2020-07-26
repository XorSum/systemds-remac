package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.HopsException;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;

import java.util.HashMap;
import java.util.List;

public class MyUtils {

    private static int count;

    public static Hop applyRule(Hop hop, List<MyRule> rules, int max_count) {
        count = max_count;
        hop.resetVisitStatus();
        hop = apply_rule_iter(null, hop, 0, rules, false);
        hop.resetVisitStatus();
        hop = apply_rule_iter(null, hop, 0, rules, true);
        return hop;
    }

    private static Hop apply_rule_iter(Hop parent, Hop hop, int pos, List<MyRule> rules, boolean descendFirst) {
        if (count <= 0) return hop;
        if (hop.isVisited())
            return hop;
        if (descendFirst) {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop hi = hop.getInput().get(i);
                hi = apply_rule_iter(hop, hi, i, rules, descendFirst);
            }
        }
        for (MyRule rule : rules) {
            hop = rule.apply(parent, hop, pos);
            count = count - 1;
        }
        if (!descendFirst) {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop hi = hop.getInput().get(i);
                hi = apply_rule_iter(hop, hi, i, rules, descendFirst);
            }
        }
        hop.setVisited();
        return hop;
    }


    /**
     * Deep copy of hops dags for parallel recompilation.
     *
     * @param hops high-level operator
     * @return high-level operator
     */
    public static Hop deepCopyHopsDag(Hop hops) {
        Hop ret = null;

        try {
            HashMap<Long, Hop> memo = new HashMap<>(); //orig ID, new clone
            ret = rDeepCopyHopsDag(hops, memo);
        } catch (Exception ex) {
            throw new HopsException(ex);
        }

        return ret;
    }

    private static Hop rDeepCopyHopsDag(Hop hop, HashMap<Long, Hop> memo)
            throws CloneNotSupportedException {
        Hop ret = memo.get(hop.getHopID());

        //create clone if required
        if (ret == null) {
            ret = (Hop) hop.clone();

            //create new childs and modify references
            for (Hop in : hop.getInput()) {
                Hop tmp = rDeepCopyHopsDag(in, memo);
                ret.getInput().add(tmp);
                tmp.getParent().add(ret);
            }
            memo.put(hop.getHopID(), ret);
        }
        return ret;
    }
}
