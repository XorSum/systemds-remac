package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.MySolution;
import org.apache.sysds.parser.VariableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.isLeafMatrix;
import static org.apache.sysds.hops.rewrite.dfp.utils.Judge.isSampleHop;

public class ConstantUtil {

   public static VariableSet variablesUpdated;

   static Map<Long, Boolean> constantTable;

   public static void  init(VariableSet variablesUpdat) {
       variablesUpdated  = variablesUpdat;
        constantTable = new HashMap<>();
   }

    public static MySolution liftLoopConstant(Hop hop) {
        Map<Long, Pair<Hop, Hop>> topConstantHops = new HashMap<>(); //   <id,<tread,twrite>        Map<Long, Boolean> constantTable = new HashMap<>();
        Map<Long, Boolean> constantTable = new HashMap<>();
        // step 1. 判断子节点是否是常量
        rFindConstant(hop);
        // step 2. 把top常量替换为tread，把twrite放入哈希表
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

    public static boolean rFindConstant(Hop hop) {
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
                if (!rFindConstant(child)) {
                    isConstant = false;
                }
            }
        }
        //  System.out.println("cons(" + hop.getHopID() + ") " + hop.getName()+" " + isConstant);
        constantTable.put(hop.getHopID(), isConstant);
        return isConstant;
    }

    private static void collectConstantHops(Hop hop,
                                            Map<Long, Pair<Hop, Hop>> topConstantHops) {
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
                collectConstantHops(child, topConstantHops);
            }
        }
        hop.setVisited();
    }


}
