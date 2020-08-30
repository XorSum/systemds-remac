package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule2;
import org.apache.sysds.utils.Explain;
import org.spark_project.jetty.util.ArrayQueue;

import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.MyUtils.deepCopyHopsDag;

public class BaoLi {

    private static ArrayList<Hop> dags;
    private static ArrayList<Integer> path;
    private static Hop firstDag;
    private static Set<Pair<Long,Long>> set;

    public static void generateAllTrees(Hop root) {
        dags = new ArrayList<>();
        set = new HashSet<>();
        dags.add(root); // push
        set.add(hashHopDag(root));
        path = new ArrayList<>();
        for (int i = 0; i < dags.size(); i++) {
            firstDag = dags.get(i);
            firstDag.resetVisitStatus();
            // System.out.println(Explain.explain(firstDag));
            generate_iter(firstDag, 0);
           // break;
        }
        System.out.println("All trees: " + dags.size());
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
        if (HopRewriteUtils.isMatrixMultiply(current)) {
            copyChangePush(depth);
        }
    }

    private static void copyChangePush(int depth) {
//        System.out.println("F3: dep: "+depth+" , id: "+path.size());
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new MatrixMultJieheRule());
        rules.add(new MatrixMultJieheRule2());
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
            shadow.resetVisitStatus();
//            System.out.println("the new tree is:");
//            System.out.println(Explain.explain(shadow));
            Pair<Long,Long> hash = hashHopDag(shadow);
            System.out.println("new tree: hash=" + hash);
            shadow.resetVisitStatus();
            System.out.println(Explain.explain(shadow));
            if (!set.contains(hash)) {
                System.out.println("push");
                set.add(hash);
                dags.add(shadow);
            } else {
                System.out.println("don't push");
            }
        }
    }

    private static boolean isPrime(long x) {
        for (long i = 2; i * i <= x; i++) {
            if (x % i == 0) return false;
        }
        return true;
    }

    private static long getPrime(int i) {
        int cnt = 0;
        long x = 1;
        while (cnt <= i) {
            x++;
            if (isPrime(x)) cnt++;
        }
        return x;
    }

    public static Pair<Long,Long> hashHopDag(Hop root) {
        long l = root.getOpString().hashCode();
        long r = root.getOpString().hashCode();
        //  System.out.println("opString=" + root.getOpString() + ", hash=" + ans);
        for (int i = 0; i < root.getInput().size(); i++) {
            Pair<Long,Long> tmp = hashHopDag(root.getInput().get(i));
         //   System.out.println("ans+="+tmp +"*"+ getPrime(i));
           // ans = ans + hashHopDag(root.getInput().get(i)) * getPrime(i);
            l = l + tmp.getLeft()*getPrime(i);
            r = r + tmp.getRight()*getPrime(i+1);
        }
        return Pair.of(l,r);
    }

    public static void main(String[] args) {
        for (int i=0;i<=10;i++) {
            System.out.println(i+" "+getPrime(i));
        }
    }
}
