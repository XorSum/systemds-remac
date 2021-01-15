package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.StatementBlockRewriteRule;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.hops.rewrite.dfp.costmodel.FakeCostEstimator2;
import org.apache.sysds.hops.rewrite.dfp.dp.CostTree;
import org.apache.sysds.parser.*;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;


import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;


public class RewriteLoopConstant extends StatementBlockRewriteRule {

    protected final static Log LOG = LogFactory.getLog(RewriteLoopConstant.class.getName());

    private ExecutionContext ec;
    public VariableSet variablesUpdated = null;

    public RewriteLoopConstant(ExecutionContext ec,VariableSet variablesUpdated) {
        this.ec = ec;
        this.variablesUpdated = variablesUpdated;
    }


    @Override
    public boolean createsSplitDag() {
        return false;
    }

    private HashMap<Hop,MySolution> map1,map2;

    @Override
    public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {


        List<StatementBlock> res = new ArrayList<>();
        if (sb.getHops()==null) {
            res.add(sb);
            return res;
        }
        map1 = new HashMap<>();
        map2 = new HashMap<>();
//        LOG.trace("While Statement");
//        LOG.info("begin constant search");
//        double cost1 = func1((WhileStatementBlock) sb, res, true, false);
//        LOG.info("constant cost = "+cost1);
//        LOG.info("end constant search");
//        Thread.interrupted();
//        System.exit(0);
        LOG.info("=================================");
        LOG.info("begin normal search");
        double cost2 = func1( sb, res, false, false);
        LOG.info("end normal search");
//        LOG.info("constant cost = "+cost1);
        LOG.info("normal cost = "+cost2);
//        if (cost1 < cost2) {
//            LOG.info("use constat result");
//            func1((WhileStatementBlock) sb, res, true, true);
//        } else {
            LOG.info("use normal result");
            func1(sb, res, false, true);
//        }
        LOG.info("=================================");
      //  System.exit(0);
        FakeCostEstimator2.cleanUnusedScratchMMNode();
        return res;



    }




    double func1(StatementBlock sb, List<StatementBlock> res, boolean constant, boolean modify) {
//        WhileStatementBlock wsb = (WhileStatementBlock) sb;
//        WhileStatement ws =(WhileStatement) wsb.getStatement(0);
//
//        sb.variablesUpdated();


        double cost = 0;

        RewriteCoordinate rewriteCoordinateConstant = new RewriteCoordinate(ec, sb,variablesUpdated);
        RewriteCoordinate rewriteCoordinate = new RewriteCoordinate(ec,sb);

        ArrayList<Hop> twriteHops = new ArrayList<>();

            for (int k = 0; k < sb.getHops().size(); k++) {
             //   Hop hop = s.getHops().get(k);
                Hop  hop = sb.getHops().get(k);
                Hop copy = deepCopyHopsDag(hop);
             //   LOG.debug(" Exp =" + MyExplain.myExplain(copy));
                MySolution mySolution;
                if (!modify) {
                    if (constant) {
                        mySolution = rewriteCoordinateConstant.rewiteHopDag(copy);
                        map1.put(hop, mySolution);
                    } else {
                        mySolution = rewriteCoordinate.rewiteHopDag(copy);
                        map2.put(hop, mySolution);
                    }
                    cost += mySolution.cost;
                  //  LOG.debug(mySolution);
                } else {
                    if (constant) mySolution = map1.get(hop);
                    else mySolution = map2.get(hop);
                    sb.getHops().set(k,mySolution.body);
                    twriteHops.addAll(mySolution.preLoopConstants);
                }
            }

        if (modify) {
            res.clear();
            if (twriteHops.size() > 0) {
                StatementBlock preStatmentBlock = new StatementBlock();
                preStatmentBlock.setLiveIn(sb.liveIn());
                preStatmentBlock.setLiveOut(sb.liveIn());  // 这里没写错,就该这样写
                preStatmentBlock.setHops(twriteHops);
                res.add(preStatmentBlock);
            }
            res.add(sb);
        }
        return cost;
    }


    @Override
    public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
        return sbs;
    }


}
