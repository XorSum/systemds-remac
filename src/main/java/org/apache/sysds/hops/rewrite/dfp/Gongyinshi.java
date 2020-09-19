package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.fenpei.FenpeiRuleLeft;
import org.apache.sysds.hops.rewrite.dfp.rule.fenpei.FenpeiRuleLeft2;
import org.apache.sysds.hops.rewrite.dfp.rule.jiaohuan.JiaoHuanRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule2;
import org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashHopDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.MyExplain.myResetVisitStatus;


public class Gongyinshi {

    private static ArrayList<Hop> dags;
    private static ArrayList<Integer> path;
    private static Hop firstDag;
    private static Set<Pair<Long, Long>> hashKeysSet;

    public static ArrayList<Hop> generateGongyinshiTrees(Hop root) {
        dags = new ArrayList<>();
        hashKeysSet = new HashSet<>();
        dags.add(root); // push
        hashKeysSet.add(hashHopDag(root));
        path = new ArrayList<>();
        myResetVisitStatus(root);
        for (int i = 0; i < dags.size()  ; i++) {
            firstDag = dags.get(i);
            generate_iter(firstDag, 0);
        }
        System.out.println("All trees: " + dags.size());
        return dags;
    }

    /*
    ac + bc + ad + bd -> (a+b)(c+d)
    axb+ayb -> a(x+y)b
    xay + xax + yay + yax -> (x+y)a(x+y)
     */


    private static void generate_iter(Hop current, int depth) {
        for (int i = 0; i < current.getInput().size(); i++) {
            if (path.size() <= depth) path.add(i);
            else path.set(depth, i);
            generate_iter(current.getInput().get(i), depth + 1);
        }
        if (HopRewriteUtils.isMatrixMultiply(current) && dags.size()<1000 ) {
            copyChangePush(depth);
        }
    }

    private static void copyChangePush(int depth) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new FenpeiRuleLeft(Types.OpOp2.PLUS));
        rules.add(new FenpeiRuleLeft2(Types.OpOp2.PLUS));
        rules.add(new FenpeiRuleLeft(Types.OpOp2.PLUS));
        rules.add(new FenpeiRuleLeft2(Types.OpOp2.PLUS));
        rules.add(new FenpeiRuleLeft(Types.OpOp2.MINUS));
        rules.add(new FenpeiRuleLeft2(Types.OpOp2.MINUS));
        rules.add(new FenpeiRuleLeft(Types.OpOp2.MINUS));
        rules.add(new FenpeiRuleLeft2(Types.OpOp2.MINUS));
        rules.add(new JiaoHuanRule(Types.OpOp2.PLUS));
        rules.add(new JiaoHuanRule(Types.OpOp2.MINUS));

        rules.add(new MatrixMultJieheRule2());
        rules.add(new MatrixMultJieheRule());

        for (MyRule rule : rules) {
            Hop shadowDag = DeepCopyHopsDag.deepCopyHopsDag(firstDag);
            Hop parentNode = null;
            Hop currentNode = shadowDag;
            for (int d = 0; d < depth; d++) {
                int i = path.get(d);
                parentNode = currentNode;
                currentNode = currentNode.getInput().get(i);
            }
            currentNode = rule.apply(parentNode, currentNode, 0);
            if (parentNode == null) shadowDag = currentNode;
            Pair<Long, Long> hash = hashHopDag(shadowDag);
            if (!hashKeysSet.contains(hash)) {
                hashKeysSet.add(hash);
                dags.add(shadowDag);
            }
        }
    }



}
