package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.StatementBlockRewriteRule;
import org.apache.sysds.hops.rewrite.dfp.rule.MyRule;
import org.apache.sysds.hops.rewrite.dfp.rule.RemoveUnnecessaryTransposeRule;
import org.apache.sysds.hops.rewrite.dfp.rule.fenpei.FenpeiRuleLeft;
import org.apache.sysds.hops.rewrite.dfp.rule.fenpei.FenpeiRuleRight;
import org.apache.sysds.hops.rewrite.dfp.rule.jiehe.MatrixMultJieheRule;
import org.apache.sysds.hops.rewrite.dfp.rule.transpose.TransposeMatrixMatrixMultSplitRule;
import org.apache.sysds.hops.rewrite.dfp.utils.MyUtils;
import org.apache.sysds.parser.*;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.sysds.hops.rewrite.dfp.utils.ApplyRulesOnDag.applyDAGRule;

public class RewriteLoopConstrant extends StatementBlockRewriteRule {
    @Override
    public boolean createsSplitDag() {
        return false;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {
        if (sb==null) return new ArrayList<>();
        System.out.println("ccc");
        if (sb instanceof WhileStatementBlock) {
            System.out.println("While Statement");
            WhileStatementBlock wstmtblk = (WhileStatementBlock) sb;
            VariableSet variablesUpdated = wstmtblk.variablesUpdated();
            WhileStatement wstmt = (WhileStatement) sb.getStatement(0);
            // step 1. reorder
            ArrayList<StatementBlock> body = wstmt.getBody();
            for (int j = 0; j < wstmt.getBody().size(); j++) {
                StatementBlock s = body.get(j);
                if (s==null||s.getHops()==null) continue;
                for (int k = 0; k < s.getHops().size(); k++) {
                    Hop hop = s.getHops().get(k);
                    hop.resetVisitStatus();
//                    System.out.println(Explain.explain(hop));
//                    System.out.println("========");
                    hop = splitExp(hop);
//                    hop.resetVisitStatus();
//                    System.out.println(Explain.explain(hop));
                    s.getHops().set(k, hop);
                }
            }

            // step 2. find constant
            ArrayList<Hop> constantHops = new ArrayList<>();
            for (int j = 0; j < wstmt.getBody().size(); j++) {
                StatementBlock s = body.get(j);
                if (s==null||s.getHops()==null) continue;
                for (int k = 0; k < s.getHops().size(); k++) {
                    Hop hop = s.getHops().get(k);
                    hop.resetVisitStatus();
                    //      System.out.println("qqqqqqqqqqqqqqqqqqqqqqqqq");
                    Map<Long, Boolean> mp = new HashMap<>();
                    rFindConstant(hop, variablesUpdated, constantHops,mp);
                }
            }
            System.out.println("found constant hops:");

            ArrayList<Hop> twriteHops = new ArrayList<>();
            for (int j = 0; j < constantHops.size(); j++) {
                Hop hop = constantHops.get(j);
                String name = "constant" + hop.getHopID();
                Hop twriteHop = HopRewriteUtils.createTransientWrite(name, hop);
                twriteHops.add(twriteHop);
                System.out.println(j + " " + hop.getName());
                hop.resetVisitStatus();
                System.out.println(Explain.explain(hop));
                System.out.println("x");
                constantHops.set(j, hop);
            }

            for (int j = 0; j < wstmt.getBody().size(); j++) {
                StatementBlock s = body.get(j);
                if (s==null||s.getHops()==null) continue;
                for (int k = 0; k < s.getHops().size(); k++) {
                    Hop hop = s.getHops().get(k);
                    hop.resetVisitStatus();
                    //      System.out.println("qqqqqqqqqqqqqqqqqqqqqqqqq");
                    rReplaceConstant(hop, variablesUpdated, constantHops);
                }
            }


            // step 3. take constant
            StatementBlock preStmtBlk = new StatementBlock();
            preStmtBlk.setHops(twriteHops);
            preStmtBlk.setLiveIn(new VariableSet());
            preStmtBlk.setLiveOut(new VariableSet());

           // preStmtBlk.setLiveIn(wstmtblk.getLiveIn());
           // preStmtBlk.setLiveOut(wstmtblk.getLiveOut());


            // step 4. construct new statement blocks

            List<StatementBlock> res = new ArrayList<>();
            res.add(preStmtBlk);
            res.add(sb);
            return res;


//            Statement stmt1 = new AssignmentStatement( , );
//
//            preStmtBlk.addStatement();


        } else {
            List<StatementBlock> res = new ArrayList<>();
            res.add(sb);
            return res;
        }
    }

    @Override
    public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
      //  System.out.println("ddd");
        return sbs;
    }


    private static Hop splitExp(Hop hop) {
        ArrayList<MyRule> rules = new ArrayList<>();
        rules.add(new TransposeMatrixMatrixMultSplitRule());
        rules.add(new RemoveUnnecessaryTransposeRule());
        rules.add(new MatrixMultJieheRule());
        rules.add(new FenpeiRuleLeft(Types.OpOp2.MINUS));
        rules.add(new FenpeiRuleLeft(Types.OpOp2.PLUS));
        rules.add(new FenpeiRuleRight(Types.OpOp2.MINUS));
        rules.add(new FenpeiRuleRight(Types.OpOp2.PLUS));
        hop = applyDAGRule(hop, rules, 100, false);
        return hop;
    }

    private static Hop checkConstant(Hop hop) {
        if (hop.isVisited()) return hop;
        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop child = hop.getInput().get(i);
            checkConstant(child);
        }

        System.out.println("aaa");

        hop.setVisited();
        return hop;
    }

    private boolean rFindConstant(Hop hop, VariableSet variablesUpdated,
                                  ArrayList<Hop> constantHops, Map<Long,Boolean> mp) {
        // if (hop.isVisited()) return false;
        if (mp.containsKey(hop.getHopID())) return mp.get(hop.getHopID());
        System.out.println("access "+hop.getHopID());
        boolean isConstant = true;
        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop child = hop.getInput().get(i);
            if (rFindConstant(child, variablesUpdated, constantHops,mp) == false) {
                isConstant = false;
            }
        }
        if (variablesUpdated.containsVariable(hop.getName())) isConstant = false;
        // System.out.println(hop.getName()+" "+isConstant);
        if (isConstant == false) {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop child = hop.getInput().get(i);
                if (rFindConstant(child, variablesUpdated, constantHops,mp) == true) {
                    if (child.isVisited() == false &&child.getInput().size()>0 ) {
                        constantHops.add(child);
                        child.setVisited();
                    }
                }
            }
        }
        mp.put(hop.getHopID(),isConstant);
        return isConstant;
    }

    private void rReplaceConstant(Hop hop, VariableSet variablesUpdated, ArrayList<Hop> constantHops) {
        if (hop.isVisited()) return;

     //   System.out.println("call replace");
        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop child = hop.getInput().get(i);
            int j = -1;
            for (int k = 0; k < constantHops.size(); k++) {
                if (child.getHopID() == constantHops.get(k).getHopID()) {
                    j = k;
                    break;
                }
            }
            if (j < 0) rReplaceConstant(child, variablesUpdated, constantHops);
            else {
                System.out.println("REPLACE");

                String name = "constant" + child.getHopID();
                Hop tread = HopRewriteUtils.createTransientRead(name, child);
                HopRewriteUtils.replaceChildReference(hop, child, tread);

            }
        }
        hop.setVisited();
    }


}
