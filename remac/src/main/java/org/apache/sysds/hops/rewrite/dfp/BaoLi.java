package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule2;
import org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashHopDag;

public class BaoLi {

    private static ArrayList<Hop> dags;
    private static ArrayList<Integer> path;
    private static Hop firstDag;
    private static Set<Pair<Long, Long>> hashKeysSet;
    private static ArrayList<MyRule> rules;

    static {
        rules = new ArrayList<>();
        rules.add(new MatrixMultJieheRule());
        rules.add(new MatrixMultJieheRule2());
    }

    public static ArrayList<Hop> generateAllTrees(Hop root) {
//        System.out.println("<--");
        dags = new ArrayList<>();
        hashKeysSet = new HashSet<>();
        dags.add(root); // push
        hashKeysSet.add(hashHopDag(root));
        path = new ArrayList<>();
//        System.out.println("root=");
        root.resetVisitStatusForced(new HashSet<>());
//        System.out.println(Explain.explain(root));
        for (int i = 0; i < dags.size(); i++) {
            firstDag = dags.get(i);
            // System.out.println(Explain.explain(firstDag));
            generate_iter(firstDag, 0);
            //  break;
        }
        System.out.println("All trees: " + dags.size());
//        System.out.println("-->");
        return dags;
//        for (int i = 0; i < dags.size(); i++) {
//            System.out.println("HASH=" + hashHopDag(dags.get(i)));
//            dags.get(i).resetVisitStatus();
//            System.out.println(Explain.explain(dags.get(i)));
//        }
    }

    private static void generate_iter(Hop current, int depth) {
        // System.out.println("F2: dep: "+depth+" , size "+path.size());
        for (int i = 0; i < current.getInput().size(); i++) {
            if (path.size() <= depth) path.add(i);
            else path.set(depth, i);
            generate_iter(current.getInput().get(i), depth + 1);
        }
        //   System.out.println("call F3: dep: "+depth+" , id: "+current.getHopID());
        for (MyRule rule : rules) {
            if (dags.size() < 1000 && rule.applicable(null, current, 0)) {
                copyChangePush(depth, rule);
            }
        }
    }

    private static void copyChangePush(int depth, MyRule rule) {
//        System.out.println("F3: dep: "+depth+" , id: "+path.size());
        Hop shadowDag = DeepCopyHopsDag.deepCopyHopsDag(firstDag);
        Hop parentNode = null;
        Hop currentNode = shadowDag;
        for (int d = 0; d < depth; d++) {
            int i = path.get(d);
//                System.out.print(" " + i);
            parentNode = currentNode;
            currentNode = currentNode.getInput().get(i);
        }
//            System.out.println("");
        currentNode = rule.apply(parentNode, currentNode, 0);
        if (parentNode == null) shadowDag = currentNode;
        Pair<Long, Long> hash = hashHopDag(shadowDag);
//            shadowDag.resetVisitStatus();
//            System.out.println("the new tree is:");
//            System.out.println(Explain.explain(shadowDag));
//            System.out.println("new tree: hash=" + hash);
//            shadowDag.resetVisitStatus();
//            System.out.println(Explain.explain(shadowDag));
        if (!hashKeysSet.contains(hash)) {
//                System.out.println("push");
            hashKeysSet.add(hash);
            dags.add(shadowDag);
        }
//            else {
//                System.out.println("don't push");
//            }

    }

}
