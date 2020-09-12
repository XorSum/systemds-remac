package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule2;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeMinusSplitRule;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeMatrixMatrixMultMergeRule;

import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.MyUtils.deepCopyHopsDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.MyUtils.hashHopDag;

public class BaoLi {

    private static ArrayList<Hop> dags;
    private static ArrayList<Integer> path;
    private static Hop firstDag;
    private static Set<Pair<Long,Long>> set;

    public static ArrayList<Hop> generateAllTrees(Hop root) {
        dags = new ArrayList<>();
        set = new HashSet<>();
        dags.add(root); // push
        set.add(hashHopDag(root));
        path = new ArrayList<>();
//        System.out.println("Root=");
//        root.resetVisitStatus();
//        System.out.println(Explain.explain(root));
        for (int i = 0; i < dags.size()  ; i++) {
            firstDag = dags.get(i);
            // System.out.println(Explain.explain(firstDag));
            generate_iter(firstDag, 0);
          //  break;
        }
        System.out.println("All trees: " + dags.size());
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
        if (HopRewriteUtils.isMatrixMultiply(current) && dags.size()<200000 ) {
            copyChangePush(depth);
        }
    }

    private static void copyChangePush(int depth) {
//        System.out.println("F3: dep: "+depth+" , id: "+path.size());
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new MatrixMultJieheRule());
        rules.add(new MatrixMultJieheRule2());
        rules.add(new TransposeMinusSplitRule());
        rules.add(new TransposeMatrixMatrixMultMergeRule());
        for (MyRule rule : rules) {
            Hop shadow = deepCopyHopsDag(firstDag);
            Hop parent = null;
            Hop p = shadow;
            for (int d = 0; d < depth; d++) {
                int i = path.get(d);
//                System.out.print(" " + i);
                parent = p;
                p = p.getInput().get(i);
            }
//            System.out.println("");
            p = rule.apply(parent, p, 0);
            if (parent == null) shadow = p;
//            shadow.resetVisitStatus();
//            System.out.println("the new tree is:");
//            System.out.println(Explain.explain(shadow));
            Pair<Long,Long> hash = hashHopDag(shadow);
//            System.out.println("new tree: hash=" + hash);
//            shadow.resetVisitStatus();
//            System.out.println(Explain.explain(shadow));
            if (!set.contains(hash)) {
//                System.out.println("push");
                set.add(hash);
                dags.add(shadow);
            }
//            else {
//                System.out.println("don't push");
//            }
        }
    }

}
