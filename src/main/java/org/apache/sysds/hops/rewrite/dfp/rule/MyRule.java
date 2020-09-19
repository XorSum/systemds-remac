package org.apache.sysds.hops.rewrite.dfp.rule;

import org.apache.sysds.hops.Hop;

public class MyRule {

    public Hop apply(Hop parent, Hop hop, int pos) {
        return hop;
    }

}
