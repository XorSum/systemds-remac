package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.StatementBlockRewriteRule;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil;
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtilByTag;
import org.apache.sysds.parser.*;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;


import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;


public class RewriteLoopConstant extends StatementBlockRewriteRule {

    protected final static Log LOG = LogFactory.getLog(RewriteLoopConstant.class.getName());

    private ExecutionContext ec;
    public VariableSet variablesUpdated = null;
    long iterationNumber;

    public RewriteLoopConstant(ExecutionContext ec, VariableSet variablesUpdated, long iterationNumber) {
        this.ec = ec;
        this.variablesUpdated = variablesUpdated;
        this.iterationNumber = iterationNumber;
    }

    @Override
    public boolean createsSplitDag() {
        return false;
    }

//    public static ArrayList<StatementBlock> preLoop = new ArrayList<>();
    public static StatementBlock preLoopStatementBlock;
    ArrayList<Hop> twriteHops = new ArrayList<>();

    static {
        preLoopStatementBlock = new StatementBlock();
        preLoopStatementBlock.setLiveIn(new VariableSet());
        preLoopStatementBlock.setLiveOut(new VariableSet());
    }

    @Override
    public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {

        List<StatementBlock> res = new ArrayList<>();
        if (sb.getHops() == null) {
            res.add(sb);
            return res;
        }

        LOG.info("=================================");
        LOG.info("begin normal search");
        double cost2 = func1(sb, res);
        LOG.info("end normal search");
        LOG.info("normal cost = " + cost2);
      //  FakeCostEstimator2.cleanUnusedScratchMMNode();
        LOG.info(res);
        return res;

    }


    double func1(StatementBlock sb, List<StatementBlock> res) {
        double cost = 0;
//        RewriteCoordinate rewriteCoordinateConstant = new RewriteCoordinate(ec, sb,variablesUpdated);
        RewriteCoordinate rewriteCoordinate = new RewriteCoordinate(ec, sb);
//        rewriteCoordinate.constantUtil = new ConstantUtil(variablesUpdated);
        rewriteCoordinate.constantUtilByTag = new ConstantUtilByTag();
        rewriteCoordinate.coordinate.variablesUpdated = variablesUpdated;

        rewriteCoordinate.iterationNumber = iterationNumber;
        for (int k = 0; k < sb.getHops().size(); k++) {
            Hop hop = sb.getHops().get(k);
            Hop copy = deepCopyHopsDag(hop);
            MySolution mySolution = rewriteCoordinate.rewiteHopDag(copy);
            cost += mySolution.cost;
            sb.getHops().set(k, mySolution.body);
            twriteHops.addAll(mySolution.preLoopConstants);
        }
        if (twriteHops.size() > 0) {
//                preLoopStatementBlock = new StatementBlock();
//                preLoopStatementBlock.setLiveIn(sb.liveIn());
//                preLoopStatementBlock.setLiveOut(sb.liveIn());  // 这里没写错,就该这样写
            preLoopStatementBlock.liveIn().addVariables(sb.liveIn());
            preLoopStatementBlock.liveOut().addVariables(sb.liveIn());
            preLoopStatementBlock.setHops(twriteHops);
        }
        res.clear();
        res.add(sb);
        return cost;
    }


    @Override
    public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
        return sbs;
    }


}
