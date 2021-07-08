package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteRule;
import org.apache.sysds.hops.rewrite.ProgramRewriteStatus;
import org.apache.sysds.hops.rewrite.StatementBlockRewriteRule;
import org.apache.sysds.parser.StatementBlock;

import java.util.ArrayList;
import java.util.List;

import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class RewriteTempStatementBlock extends StatementBlockRewriteRule {

    @Override
    public boolean createsSplitDag() {
        return false;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlock(StatementBlock sb, ProgramRewriteStatus state) {
        System.out.println("REORDER");
        if (sb.getHops()!=null) {
            for (int i=0;i<sb.getHops().size();i++) {
                Hop hi = sb.getHops().get(i);
                hi = reorder(hi);
                sb.getHops().set(i,hi);
            }
        }
        ArrayList<StatementBlock> list = new ArrayList<>( );
        list.add(sb);
        return list;
    }

    @Override
    public List<StatementBlock> rewriteStatementBlocks(List<StatementBlock> sbs, ProgramRewriteStatus state) {
        System.out.println("REORDER");
        for (int j=0;j<sbs.size();j++) {
            StatementBlock sb = sbs.get(j);
            if (sb.getHops()!=null) {
                for (int i=0;i<sb.getHops().size();i++) {
                    Hop hi = sb.getHops().get(i);
                    hi = reorder(hi);
                    sb.getHops().set(i,hi);
                }
            }
        }
        return sbs;
    }
}
