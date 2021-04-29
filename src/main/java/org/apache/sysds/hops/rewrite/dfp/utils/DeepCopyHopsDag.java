package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.HopsException;

import java.util.HashMap;

public class DeepCopyHopsDag {
    /**
     * Deep copy of hops dags for parallel recompilation.
     *
     * @param hops high-level operator
     * @return high-level operator
     */
    public static Hop deepCopyHopsDag(Hop hops) {
        Hop ret = null;

        try {
            HashMap<Long, Hop> memo = new HashMap<>(); //orig ID, new clone
            ret = rDeepCopyHopsDag(hops, memo);
        } catch (Exception ex) {
            throw new HopsException(ex);
        }
        return ret;
    }

    public static Hop rDeepCopyHopsDag(Hop hop, HashMap<Long, Hop> memo)
            throws CloneNotSupportedException {
        Hop ret = memo.get(hop.getHopID());

        //create clone if required
        if (ret == null) {
            ret = (Hop) hop.clone();

            //create new childs and modify references
            for (Hop in : hop.getInput()) {
                Hop tmp = rDeepCopyHopsDag(in, memo);
                ret.getInput().add(tmp);
                tmp.getParent().add(ret);
            }
            memo.put(hop.getHopID(), ret);
        }
        return ret;
    }

}
