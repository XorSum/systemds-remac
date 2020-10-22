package org.apache.sysds.hops.rewrite.dfp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.StatementBlockRewriteRule;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.hops.rewrite.dfp.utils.ConstantUtil;
import org.apache.sysds.hops.rewrite.dfp.utils.MyExplain;
import org.apache.sysds.parser.*;
import org.apache.sysds.runtime.controlprogram.BasicProgramBlock;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;


import java.util.*;

import static org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag.deepCopyHopsDag;


public class RewriteLoopConstrant extends StatementBlockRewriteRule {

    protected final static Log LOG = LogFactory.getLog(RewriteLoopConstrant.class.getName());

    private ExecutionContext ec;

    public RewriteLoopConstrant(ExecutionContext ec) {
        this.ec = ec;
    }

//    public RewriteLoopConstrant() {
//        this.ec = null;
//    }

    @Override
    public boolean createsSplitDag() {
        return false;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {
        List<StatementBlock> res = new ArrayList<>();
//        System.out.println("rewriteStatementBlock  " + sb.toString());
        if (sb == null) {
            res.add(sb);
            return res;
        }
        if (sb instanceof WhileStatementBlock) {
            LOG.trace("While Statement");

            WhileStatementBlock wsb = (WhileStatementBlock) sb;
            WhileStatement ws = (WhileStatement) wsb.getStatement(0);
            VariableSet variablesUpdated = wsb.variablesUpdated();

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
                    ConstantUtil constantUtil = new ConstantUtil(variablesUpdated);

                    MySolution mySolution = constantUtil.liftLoopConstant(copy);
                    LOG.debug(mySolution);

                    if (mySolution.preLoopConstants.size()>0) {
                        s.getHops().set(k, mySolution.body);
                        twriteHops.addAll(mySolution.preLoopConstants);
                    }
                }
            }

            if (twriteHops.size()>0) {
                StatementBlock preStatmentBlock = new StatementBlock();
                preStatmentBlock.setLiveIn(wsb.liveIn());
                preStatmentBlock.setLiveOut(wsb.liveIn());  // 这里没写错,就该这样写
                preStatmentBlock.setHops(twriteHops);
                res.add(preStatmentBlock);
            }
            res.add(sb);

        } else {
            res.add(sb);
        }
        return res;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
        return sbs;
    }


}
