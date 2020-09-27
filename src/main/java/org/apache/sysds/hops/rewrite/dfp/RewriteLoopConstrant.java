package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.parser.StatementBlock;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.parser.WhileStatement;
import org.apache.sysds.parser.WhileStatementBlock;
import org.apache.sysds.utils.Explain;

import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.ApplyRulesOnDag.applyDAGRule;
import static org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil.liftLoopConstant;
import static org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil.rFindConstant;
import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Hash.hashHopDag;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.isLeafMatrix;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.isSampleHop;


public class RewriteLoopConstrant extends StatementBlockRewriteRule {
    @Override
    public boolean createsSplitDag() {
        return false;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {
        List<StatementBlock> res = new ArrayList<>();
        System.out.println("rewriteStatementBlock");
        if (sb == null) return res;
        if (sb instanceof WhileStatementBlock) {
            System.out.println("While Statement");
            WhileStatementBlock wsb = (WhileStatementBlock) sb;
            VariableSet variablesUpdated = wsb.variablesUpdated();
            ConstantUtil.init(variablesUpdated);

            WhileStatement ws = (WhileStatement) wsb.getStatement(0);
            ArrayList<StatementBlock> body = ws.getBody();
            for (int j = 0; j < body.size(); j++) {
                StatementBlock s = body.get(j);
                if (s == null || s.getHops() == null) continue;
                for (int k = 0; k < s.getHops().size(); k++) {

                    Hop hop = s.getHops().get(k);
                    Hop copy = deepCopyHopsDag(hop);
                    System.out.println(" Exp =" + MyExplain.myExplain(copy));
                 //   RewriteDFP.rewriteDFP(copy);
                    RewriteDFP.main(copy);
//                    ArrayList<Hop> trees = BaoLi.generateAllTrees(copy);


//                    ArrayList<MySolution> solutions = new ArrayList<>();
//                    for (Hop h : trees) {
//                        MySolution solution = liftLoopConstant(h);
//                     //   System.out.println(solution);
//                        solutions.add(solution);
//                    }
                }
            }
        }
//        else {
//            if (sb.getHops() != null) {
//                for (int k = 0; k < sb.getHops().size(); k++) {
//                    Hop hop = sb.getHops().get(k);
//                    System.out.println(" Exp =" + MyExplain.myExplain(hop));
//                }
//            }
//        }
        res.add(sb);
        return res;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
        return sbs;
    }



}
