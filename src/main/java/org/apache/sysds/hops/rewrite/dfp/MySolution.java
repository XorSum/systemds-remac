package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;

public class MySolution {

    public Hop body;
    public ArrayList<Hop> preLoopConstants;
    public double cost;

    public MySolution() {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MySolution{" +
                "body={\n" + Explain.explain(body) +
                "}, preLoopConstants={\n");
        for (Hop h : preLoopConstants) {
            sb.append(Explain.explain(h));
            sb.append("----\n");
        }
        sb.append("}, cost=" + cost + '}');
        return sb.toString();
    }
}
