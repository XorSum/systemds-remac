package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;

import static org.apache.sysds.hops.rewrite.dfp.AnalyzeSymmetryMatrix.querySymmetry;
import static org.apache.sysds.hops.rewrite.dfp.utils.Prime.getPrime;

public class Hash {

    public static Pair<Long, Long> hashHopDag(Hop root) {
        Pair<Long, Long> hash = rHashHopDag(root);
//        System.out.println("exp=" + explain(root) + ", hash=" + hash);
        return hash;
    }

    public static Pair<Long, Long> rHashHopDag(Hop root) {
        long l, r;
        if (Judge.isLeafMatrix(root)) {
            l = root.getName().hashCode();
            r = root.getName().hashCode();
        }
        else if (HopRewriteUtils.isTransposeOperation(root)
                && Judge.isLeafMatrix(root.getInput().get(0))
                && querySymmetry(root.getInput().get(0).getName())) {
            // 对称矩阵
         //   System.out.println("对称");
            return rHashHopDag(root.getInput().get(0));
        }
        else {
            l = root.getOpString().hashCode();
            r = root.getOpString().hashCode();
            //  System.out.println("opString=" + root.getOpString() + ", hash=" + ans);
            for (int i = 0; i < root.getInput().size(); i++) {
                Pair<Long, Long> tmp = rHashHopDag(root.getInput().get(i));
                //   System.out.println("ans+="+tmp +"*"+ getPrime(i));
                // ans = ans + hashHopDag(root.getInput().get(i)) * getPrime(i);
                l = l + tmp.getLeft() * getPrime(i);
                r = r + tmp.getRight() * getPrime(i + 1);
            }
        }
      //  System.out.println("hash "+root.getHopID()+" "+l+" "+r);
        return Pair.of(l, r);
    }

    public static Pair<Long, Long> hashTransposeHopDag(Hop root) {
        Pair<Long, Long> hash = rHashTransposeHopDag(root);
     //   System.out.println("exp=T{" + explain(root) + "}, hash=" + hash);
        return hash;
    }

    public static Pair<Long, Long> rHashTransposeHopDag(Hop root) {
        long l, r;
        if (Judge.isLeafMatrix(root)) {
            return rHashHopDag(HopRewriteUtils.createTranspose(root));
        } else if (HopRewriteUtils.isTransposeOperation(root)) {
            return rHashHopDag(root.getInput().get(0));
        } else {
            l = root.getOpString().hashCode();
            r = root.getOpString().hashCode();
            int n = root.getInput().size();
            for (int i = 0; i < n; i++) {
                Pair<Long, Long> p = rHashTransposeHopDag(root.getInput().get(n - i - 1));
                l = l + getPrime(i) * p.getLeft();
                r = r + getPrime(i) * p.getRight();
            }
        }
        return Pair.of(l, r);
    }

}
