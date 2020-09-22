package org.apache.sysds.hops.rewrite.dfp.rule;

import org.apache.sysds.hops.Hop;

public interface MyRule {

    public Hop apply(Hop parent, Hop hop, int pos);

    public Boolean applicable(Hop parent, Hop hop, int pos);

}
