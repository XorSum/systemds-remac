package org.apache.sysds.hops.rewrite.dfp;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.dfp.coordinate.MultiCse;
import org.apache.sysds.utils.Explain;

import java.util.ArrayList;

public class MySolution {
    public MultiCse multiCse=null;
    public Hop body=null;
    public ArrayList<Hop> preLoopConstants = new ArrayList<>();
    public double cost = Double.MAX_VALUE;

    public MySolution() {
    }

    public MySolution(Hop body) {
        this.body = body;
    }

    public MySolution(MultiCse multiCse, Hop body) {
        this.multiCse = multiCse;
        this.body = body;
    }



    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MySolution{");
        if (multiCse!=null) {
            sb.append(multiCse.toString());
        }
        if (body!=null) {
            sb.append("body:{\n" + Explain.explain(body) +"}," );
        }
        if (preLoopConstants!=null) {
            sb.append("preLoopConstants={\n");
            for (Hop h : preLoopConstants) {
                sb.append(Explain.explain(h));
                sb.append("----\n");
            }
            sb.append("},");
        }
        sb.append("cost=" + cost + '}');
        return sb.toString();
    }
}
