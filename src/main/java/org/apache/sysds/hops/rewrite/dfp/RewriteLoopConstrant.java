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
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.parser.StatementBlock;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.parser.WhileStatement;
import org.apache.sysds.parser.WhileStatementBlock;
import org.apache.sysds.utils.Explain;

import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.ApplyRulesOnDag.applyDAGRule;
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
            WhileStatement ws = (WhileStatement) wsb.getStatement(0);
            ArrayList<StatementBlock> body = ws.getBody();
            for (int j = 0; j < body.size(); j++) {
                StatementBlock s = body.get(j);
                if (s == null || s.getHops() == null) continue;
                for (int k = 0; k < s.getHops().size(); k++) {

                    Hop hop = s.getHops().get(k);
                    Hop copy = deepCopyHopsDag(hop);
                    System.out.println(" Exp =" + MyExplain.myExplain(copy));

                    ArrayList<Hop> trees = BaoLi.generateAllTrees(copy);


                    ArrayList<MySolution> solutions = new ArrayList<>();
                    for (Hop h : trees) {
                        MySolution solution = liftLoopConstant(h, variablesUpdated);
                     //   System.out.println(solution);
                        solutions.add(solution);
                    }
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


    private static MySolution liftLoopConstant(Hop hop, VariableSet variablesUpdated) {

        Map<Long, Pair<Hop, Hop>> topConstantHops = new HashMap<>(); //   <id,<tread,twrite>        Map<Long, Boolean> constantTable = new HashMap<>();
        Map<Long, Boolean> constantTable = new HashMap<>();
        // step 1. 判断子节点是否是常量
        rFindConstant(hop, variablesUpdated, constantTable);
        // step 2. 把top常量替换为tread，把twrite放入哈希表
        collectConstantHops(hop, topConstantHops, constantTable);
        hop.resetVisitStatusForced(new HashSet<>());
        // step 3. 创建solution
        MySolution mySolution = new MySolution();
        mySolution.body = hop;
        mySolution.preLoopConstants = new ArrayList<>();
        for (Map.Entry<Long, Pair<Hop, Hop>> c : topConstantHops.entrySet()) {
            Hop h = c.getValue().getRight();
            h.resetVisitStatusForced(new HashSet<>());
            mySolution.preLoopConstants.add(h);
        }

        return mySolution;
    }

    private static boolean rFindConstant(Hop hop,
                                         VariableSet variablesUpdated,
                                         Map<Long, Boolean> constantTable) {
        if (constantTable.containsKey(hop.getHopID())) { // 记忆化搜索
            return constantTable.get(hop.getHopID());
        }
        boolean isConstant = true;
        if (variablesUpdated.containsVariable(hop.getName())) { // 判断自己是否改变
            isConstant = false;
        }
        if (!isLeafMatrix(hop)) {  // 判断儿子是否改变
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop child = hop.getInput().get(i);
                if (!rFindConstant(child, variablesUpdated, constantTable)) {
                    isConstant = false;
                }
            }
        }
        //  System.out.println("cons(" + hop.getHopID() + ") " + hop.getName()+" " + isConstant);
        constantTable.put(hop.getHopID(), isConstant);
        return isConstant;
    }

    private static void collectConstantHops(Hop hop,
                                            Map<Long, Pair<Hop, Hop>> topConstantHops,
                                            Map<Long, Boolean> constantTable) {
        if (hop.isVisited()) return;
        if (constantTable.get(hop.getHopID())) return;
        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop child = hop.getInput().get(i);
            Long id = child.getHopID();
            if ( constantTable.get(child.getHopID())) {// 非常量父节点指向常量子节点,说明子节点是个top常量
                if (!isSampleHop(child) && !hop.isScalar()) {
                    if (!topConstantHops.containsKey(id)) {
                        String name = "constant" + id;
                        Hop twrite = HopRewriteUtils.createTransientWrite(name, child);
                        Hop tread = HopRewriteUtils.createTransientRead(name, child);
                        topConstantHops.put(id, Pair.of(tread, twrite));
                        HopRewriteUtils.replaceChildReference(hop, child, tread);
                        HopRewriteUtils.cleanupUnreferenced(child);
                    } else {
                        Hop tread = topConstantHops.get(id).getLeft();
                        HopRewriteUtils.replaceChildReference(hop, child, tread);
                        HopRewriteUtils.cleanupUnreferenced(child);
                    }
                }
            } else {// 儿子也不是常量，则继续向下递归
                collectConstantHops(child, topConstantHops, constantTable);
            }
        }
        hop.setVisited();
    }

}
