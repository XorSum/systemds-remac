package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.Hop;

import static org.apache.sysds.hops.rewrite.dfp.utils.Prime.getPrime;

public class Hash {

    public static Pair<Long, Long> hashHopDag(Hop root) {
        long l ,r;
        if (Judge.isLeafMatrix(root)) {
            l = root.getOpString().hashCode();
            r = root.getOpString().hashCode();
        } else {
            l =  root.getOpString().hashCode();
            r =  root.getOpString().hashCode();
            //  System.out.println("opString=" + root.getOpString() + ", hash=" + ans);
            for (int i = 0; i < root.getInput().size(); i++) {
                Pair<Long, Long> tmp = hashHopDag(root.getInput().get(i));
                //   System.out.println("ans+="+tmp +"*"+ getPrime(i));
                // ans = ans + hashHopDag(root.getInput().get(i)) * getPrime(i);
                l = l + tmp.getLeft() * getPrime(i);
                r = r + tmp.getRight() * getPrime(i + 1);
            }
        }
        return Pair.of(l, r);
    }

}
