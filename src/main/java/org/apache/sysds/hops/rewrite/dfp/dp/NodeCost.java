package org.apache.sysds.hops.rewrite.dfp.dp;


public class NodeCost {

    public double shuffleCost = 0;
    public double broadcastCost = 0;
    public double computeCost = 0;
    public double collectCost = 0;

    @Override
    public String toString() {
        return "{" +
                "sum=" + getSummary() +
                ", sf=" + shuffleCost +
                ", bc=" + broadcastCost +
                ", cp=" + computeCost +
                ", cl=" + collectCost +
                '}';
    }




    public NodeCost(double shuffleCost, double broadcastCost, double computeCost, double collectCost) {
        this.shuffleCost = shuffleCost;
        this.broadcastCost = broadcastCost;
        this.computeCost = computeCost;
        this.collectCost = collectCost;
    }

    public double getSummary() {
        return shuffleCost + broadcastCost + computeCost + collectCost;
    }

    public static NodeCost  add(NodeCost a, NodeCost b) {
        return new NodeCost(a.shuffleCost + b.shuffleCost,
                a.broadcastCost + b.broadcastCost,
                a.computeCost + b.computeCost,
                a.collectCost + b.collectCost);
    }

    public static NodeCost  add(NodeCost a, NodeCost b, NodeCost c) {
        return new NodeCost(a.shuffleCost + b.shuffleCost+c.shuffleCost,
                a.broadcastCost + b.broadcastCost+c.broadcastCost,
                a.computeCost + b.computeCost+c.computeCost,
                a.collectCost + b.collectCost+c.collectCost);
    }

    public static NodeCost ZERO() {
        return new NodeCost(0, 0, 0, 0);
    }

    public static NodeCost INF() {
        return new NodeCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
    }

}
