package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.DataGenOp;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.AnalyzeSymmetryMatrix;
import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class Judge {

    public static boolean isSame(Hop a, Hop b) {
        if (a == null || b == null) return false;
        Hop aa = deepCopyHopsDag(a);
        aa = reorder(aa);
        Hop bb = deepCopyHopsDag(b);
        bb = reorder(bb);
        boolean ret = isSame_iter(aa, bb);
     //   System.out.println("Compare " +MyExplain.myExplain(a)+" "+MyExplain.myExplain(b) +  "Ret=" + ret);
        return ret;
    }

    private static boolean isSame_iter(Hop a, Hop b) {
        if (a.equals(b)) return true;
        if (HopRewriteUtils.isTransposeOperation(a)
                && isLeafMatrix(a.getInput().get(0))
                && AnalyzeSymmetryMatrix.querySymmetry(a.getInput().get(0).getName())) {
            return isSame_iter(a.getInput().get(0),b);
        }
        if (HopRewriteUtils.isTransposeOperation(b)
                && isLeafMatrix(b.getInput().get(0))
                && AnalyzeSymmetryMatrix.querySymmetry(b.getInput().get(0).getName())) {
            return isSame_iter(a,b.getInput().get(0));
        }
        if (a.getInput().size() != b.getInput().size()) return false;
        if (!a.getInput().isEmpty()) {
            for (int i = 0; i < a.getInput().size(); i++) {
                if (!isSame_iter(a.getInput().get(i), b.getInput().get(i))) {
                    return false;
                }
            }
        }
//        System.out.println(a.getOpString());
//        System.out.println(b.getOpString());
        return a.getOpString().equals(b.getOpString());
    }

    public static boolean isSampleHop(Hop a) {
        if ("dg(rand)".equals(a.getOpString())) {
            return true;
        } else if (isRead(a)) {
            return true;
        } else if (a.getInput().size() == 0) {
            return true;
        } else if (a.getInput().size() == 1) {
            return isSampleHop(a.getInput().get(0));
        } else {
            return false;
        }
    }

    public static boolean isRead(Hop hop) {
        return hop instanceof DataOp &&
                ((DataOp) hop).isRead();
    }

    public static boolean isWrite(Hop hop) {
        return hop instanceof DataOp &&
                ((DataOp) hop).isWrite();
    }

    public static boolean isDiagMatrix(Hop hop) {
        // 如果只有对角线上的元素非零，返回true
        if (HopRewriteUtils.isReorg(hop, Types.ReOrgOp.DIAG)) {
            Hop son = hop.getInput().get(0);
            return son.getDim2() == 1;
        }
        return false;
    }


    public static boolean isLeafMatrix(Hop hop) {
        return "dg(rand)".equals(hop.getOpString())
                || hop instanceof DataGenOp
                || isRead(hop)
                || isDiagMatrix(hop);
    }

    public static boolean isAllOfMult(Hop hop) {
        if (isSampleHop(hop)) return true;
        boolean ans = HopRewriteUtils.isMatrixMultiply(hop);
        for (int i = 0; ans && i < hop.getInput().size(); i++) {
            ans = isAllOfMult(hop.getInput().get(i));
        }
//        if (ans)
//            System.out.println(" all of mult");
        return ans;
    }

}
