package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.common.Types;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.HopsException;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.parser.StatementBlock;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class MyUtils {

    public static String explain(Hop hop) {
        String ans = explain_iter(hop);
        return ans;
    }

    private static String explain_iter(Hop hop) {
        StringBuilder sb = new StringBuilder();
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
            sb.append( explain_iter(hop.getInput().get(0)));
            sb.append("%*%");
            sb.append( explain_iter(hop.getInput().get(1)));
        }else  if (HopRewriteUtils.isTransposeOperation(hop)) {
            sb.append("t(");
            sb.append( explain_iter(hop.getInput().get(0)));
            sb.append(")");
        }else if (hop.getOpString().equals("dg(rand)")) {
            sb.append("dg(rand)");
        } else if (hop instanceof DataOp && ((DataOp)hop).getOp()== Types.OpOpData.TRANSIENTREAD ) {
            sb.append( ((DataOp)hop).getName() );
        }else if (hop instanceof DataOp && ((DataOp)hop).getOp()== Types.OpOpData.TRANSIENTWRITE ) {
            sb.append( ( (DataOp)hop).getName() );
            sb.append(":=");
        } else if (hop.getInput().size()==1){
            sb.append(hop.getOpString());
            sb.append("(");
            sb.append( explain_iter(hop.getInput().get(0)));
            sb.append(")");
        } else if (hop.getInput().size()==2) {
            sb.append( explain_iter(hop.getInput().get(0)));
            sb.append(hop.getOpString());
            sb.append( explain_iter(hop.getInput().get(1)));
        }
        else{
            sb.append("[");
            sb.append(hop.getOpString());
            sb.append("]");
        }
        return sb.toString();
    }


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

    public static Pair<Long, Long> hashHopDag(Hop root) {
        long l = root.getOpString().hashCode();
        long r = root.getOpString().hashCode();
        //  System.out.println("opString=" + root.getOpString() + ", hash=" + ans);
        for (int i = 0; i < root.getInput().size(); i++) {
            Pair<Long, Long> tmp = hashHopDag(root.getInput().get(i));
            //   System.out.println("ans+="+tmp +"*"+ getPrime(i));
            // ans = ans + hashHopDag(root.getInput().get(i)) * getPrime(i);
            l = l + tmp.getLeft() * getPrime(i);
            r = r + tmp.getRight() * getPrime(i + 1);
        }
        return Pair.of(l, r);
    }

    public static void main(String[] args) {
        for (int i = 0; i <= 10; i++) {
            System.out.println(i + " " + getPrime(i));
        }
    }


}
