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

    public static void myResetVisitStatus(Hop hop) {
        hop.setVisited(false);
        for (int i=0;i<hop.getInput().size();i++) {
            myResetVisitStatus(hop.getInput().get(i));
        }
    }

    public static String explain(Hop hop) {
        String ans = explain_iter(hop);
        return ans;
    }

    private static String explain_iter(Hop hop) {
        StringBuilder sb = new StringBuilder();
        if (HopRewriteUtils.isMatrixMultiply(hop)) {
            sb.append(explain_iter(hop.getInput().get(0)));
            sb.append("%*%");
            sb.append(explain_iter(hop.getInput().get(1)));
        } else if (HopRewriteUtils.isTransposeOperation(hop)) {
            sb.append("t(");
            sb.append(explain_iter(hop.getInput().get(0)));
            sb.append(")");
        } else if (hop.getOpString().equals("dg(rand)")) {
            sb.append("dg(rand)");
        } else if (hop instanceof DataOp && ((DataOp) hop).getOp() == Types.OpOpData.TRANSIENTREAD) {
            sb.append(((DataOp) hop).getName());
        } else if (hop instanceof DataOp && ((DataOp) hop).getOp() == Types.OpOpData.TRANSIENTWRITE) {
            sb.append(((DataOp) hop).getName());
            sb.append(":=");
        } else if (hop.getInput().size() == 1) {
            sb.append(hop.getOpString());
            sb.append("(");
            sb.append(explain_iter(hop.getInput().get(0)));
            sb.append(")");
        } else if (hop.getInput().size() == 2) {
            sb.append(explain_iter(hop.getInput().get(0)));
            sb.append(hop.getOpString());
            sb.append(explain_iter(hop.getInput().get(1)));
        } else {
            sb.append("[");
            sb.append(hop.getOpString());
            sb.append("]");
        }
        return sb.toString();
    }







}
