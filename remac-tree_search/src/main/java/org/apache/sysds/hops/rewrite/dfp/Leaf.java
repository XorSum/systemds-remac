package org.apache.sysds.hops.rewrite.dfp;

import org.apache.spark.sql.sources.In;
import org.apache.sysds.hops.Hop;

import java.util.ArrayList;

public class Leaf {
    public Hop hop;
    public ArrayList<Integer> path;
    public int depth;

    public Leaf(Hop hop, ArrayList<Integer> path, int depth) {
        this.hop = hop;
        this.path = (ArrayList<Integer>)path.clone();
        this.depth = depth;
    }
}