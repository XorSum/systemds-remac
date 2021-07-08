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

    public  double preCost = 0;
    public  double bodyCost = 0;
    public double preloopShuffleCost = 0;
    public double preloopBroadcastCost = 0;
    public double preloopComputeCost = 0;
    public  double preloopCollectCost = 0;
    public double bodyShuffleCost = 0;
    public double bodyBroadcastCost = 0;
    public double bodyComputeCost = 0;
    public  double bodyCollectCost = 0;

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
//        sb.append("cost=" + cost + "\n");
        sb.append("allCost=" + cost+ "\n");
        sb.append("preCost=" + preCost+ "\n");
        sb.append("bodyCost=" + bodyCost+ "\n");
        sb.append("preloopShuffleCost=" + preloopShuffleCost+ "\n");
        sb.append("preloopBroadcastCost=" + preloopBroadcastCost+ "\n");
        sb.append("preloopComputeCost=" + preloopComputeCost+ "\n");
        sb.append("preloopCollectCost=" + preloopCollectCost+ "\n");
        sb.append("bodyShuffleCost=" + bodyShuffleCost+ "\n");
        sb.append("bodyBroadcastCost=" + bodyBroadcastCost+ "\n");
        sb.append("bodyComputeCost=" + bodyComputeCost+ "\n");
        sb.append("bodyCollectCost=" + bodyCollectCost+ "}");
        return sb.toString();
    }
}
