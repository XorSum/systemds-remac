package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.parser.DMLProgram;
import org.apache.sysds.parser.DMLTranslator;
import org.apache.sysds.parser.StatementBlock;
import org.apache.sysds.runtime.controlprogram.Program;

import java.util.ArrayList;
import java.util.HashSet;

public class CreateRuntimeProgram {

    public static Program constructProgramBlocks(Hop hop,StatementBlock statementBlock) {
        DMLProgram prog = new DMLProgram();
        StatementBlock sb = new StatementBlock();
        ArrayList<Hop> hops = new ArrayList<>();
        hops.add(hop);
        sb.setHops(hops);
        sb.setLiveIn(statementBlock.liveIn());
        sb.setLiveOut(statementBlock.liveOut());
        prog.addStatementBlock(sb);
        DMLTranslator dmlt = new DMLTranslator(prog);
        try {
            dmlt.constructLops(prog);
        } catch (Exception e) {
            System.out.println("Construct Program Blocks Error");
            hop.resetVisitStatusForced(new HashSet<>());
            System.out.println(MyExplain.myExplain(hop));
            System.out.println("y");
            return null;
        }
        Program rtprog = dmlt.getRuntimeProgram(prog, ConfigurationManager.getDMLConfig());
        return rtprog;
    }


}
