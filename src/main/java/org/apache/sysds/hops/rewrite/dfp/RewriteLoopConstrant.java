package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.StatementBlockRewriteRule;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil;
import org.apache.sysds.hops.rewrite.dfp.utils.FakeCostEstimator;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.parser.*;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;

import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;


public class RewriteLoopConstrant extends StatementBlockRewriteRule {

    protected static final Log LOG = LogFactory.getLog(RewriteLoopConstrant.class.getName());

    private ExecutionContext ec;

    public RewriteLoopConstrant(ExecutionContext ec) {
        this.ec = ec;
    }

    public RewriteLoopConstrant() {
        this.ec = null;
    }

    @Override
    public boolean createsSplitDag() {
        return false;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {
        List<StatementBlock> res = new ArrayList<>();
//        System.out.println("rewriteStatementBlock  " + sb.toString());
        if (sb == null) return res;
        if (sb instanceof WhileStatementBlock) {
            LOG.trace("While Statement");

            WhileStatementBlock wsb = (WhileStatementBlock) sb;
            WhileStatement ws = (WhileStatement) wsb.getStatement(0);
            VariableSet variablesUpdated = wsb.variablesUpdated();

            ConstantUtil.init(variablesUpdated);
            RewriteCoordinate rewriteCoordinate = new RewriteCoordinate(ec);
            rewriteCoordinate.variablesUpdated = variablesUpdated;
            rewriteCoordinate.onlySearchConstantSubExp = true;

            ArrayList<Hop> twriteHops = new ArrayList<>();

            for (int j = 0; j < ws.getBody().size(); j++) {
                StatementBlock s = ws.getBody().get(j);
                if (s == null || s.getHops() == null) continue;
                for (int k = 0; k < s.getHops().size(); k++) {
                    Hop hop = s.getHops().get(k);
                    Hop copy = deepCopyHopsDag(hop);
                    LOG.debug(" Exp =" + MyExplain.myExplain(copy));
                    copy = rewriteCoordinate.rewiteHopDag(copy);
                    MySolution mySolution = ConstantUtil.liftLoopConstant(copy);

                    s.getHops().set(k,mySolution.body);
                 //   System.out.println(preStatmentBlock.getHops());
                    ArrayList<Hop> preHop = mySolution.preLoopConstants;
                    twriteHops.addAll(preHop);
                 //   System.out.println(preHop);


                  //  ArrayList<Hop> preSBhops = preStatmentBlock.getHops();
//                    System.out.println(preSBhops);
//                    System.out.println("x");

//                    double cost =  FakeCostEstimator.estimate(hop,null);
//                    System.out.println("cost="+cost);
//                   RewriteDFP.rewriteDFP(copy);
//                    RewriteCoordinate.statementBlock = sb;
//                    RewriteCoordinate.rewiteHopDag(copy);
                    //  FakeCostEstimator.printTime();
//                    ArrayList<Hop> trees = BaoLi.generateAllTrees(copy);
//                    ArrayList<MySolution> solutions = new ArrayList<>();
//                    for (Hop h : trees) {
//                        MySolution solution = liftLoopConstant(h);
//                     //   System.out.println(solution);
//                        solutions.add(solution);
//                    }
                }
            }

            StatementBlock preStatmentBlock = new StatementBlock();

            preStatmentBlock.setLiveIn(wsb.liveIn());
            preStatmentBlock.setLiveOut(wsb.liveOut());
            preStatmentBlock.setHops(twriteHops);
            res.add(preStatmentBlock);
            res.add(wsb);
        } else {
            res.add(sb);
//            if (sb.getHops() != null) {
//                for (int k = 0; k < sb.getHops().size(); k++) {
//                    Hop hop = sb.getHops().get(k);
//                    Hop copy = deepCopyHopsDag(hop);
//                    System.out.println(" Exp =" + MyExplain.myExplain(copy));
//                    RewriteCoordinate.main(copy,ec);
//                }
//            }
        }

        return res;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
        return sbs;
    }


}
