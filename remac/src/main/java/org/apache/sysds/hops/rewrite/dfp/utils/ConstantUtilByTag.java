package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.MySolution;
import org.apache.sysds.parser.VariableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ConstantUtilByTag {

    protected static final Log LOG = LogFactory.getLog(ConstantUtilByTag.class.getName());

    public MySolution liftLoopConstant(Hop hop) {
        Map<Long, Pair<Hop, Hop>> topConstantHops = new HashMap<>(); //   <id,<tread,twrite>        Map<Long, Boolean> constantTable = new HashMap<>();
        // step 2. 把top常量替换为tread，把twrite放入哈希表
        hop.resetVisitStatusForced(new HashSet<>());
        collectConstantHops(hop, topConstantHops);
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

    private void collectConstantHops(Hop hop,
                                     Map<Long, Pair<Hop, Hop>> topConstantHops) {
        if (hop.isVisited()) return;
        if (hop.isConstant) return;
        for (int i = 0; i < hop.getInput().size(); i++) {
            Hop child = hop.getInput().get(i);
            if (child.isConstant) {
//                LOG.debug("found top constant "+child.getHopID());
                Long id = child.getHopID();
                if (!topConstantHops.containsKey(id)) {
                    String name = "_conVar" + id;
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
            } else {
                collectConstantHops(child, topConstantHops);
            }
        }
        hop.setVisited();
    }


}
