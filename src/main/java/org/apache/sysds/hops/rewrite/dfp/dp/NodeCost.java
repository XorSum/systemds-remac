package org.apache.sysds.hops.rewrite.dfp.dp;


public class NodeCost {

    public double shuffleCost = 0;
    public double broadcastCost = 0;
    public double computeCost = 0;
    public double collectCost = 0;
    public double joinCost = 0;

    @Override
    public String toString() {
        return "{" +
                "sum=" + getSummary() +
                ", sf=" + shuffleCost +
                ", bc=" + broadcastCost +
                ", cp=" + computeCost +
                ", cl=" + collectCost +
                ", jo=" + joinCost +
                '}';
    }




    public NodeCost(double shuffleCost, double broadcastCost, double computeCost, double collectCost) {
        this.shuffleCost = shuffleCost;
        this.broadcastCost = broadcastCost;
        this.computeCost = computeCost;
        this.collectCost = collectCost;
        this.joinCost = 0;
    }

    public NodeCost(double shuffleCost, double broadcastCost, double computeCost, double collectCost, double joinCost) {
        this.shuffleCost = shuffleCost;
        this.broadcastCost = broadcastCost;
        this.computeCost = computeCost;
        this.collectCost = collectCost;
        this.joinCost = joinCost;
    }

    public void plus(NodeCost that) {
        this.shuffleCost += that.shuffleCost;
        this.broadcastCost += that.broadcastCost;
        this.computeCost += that.computeCost;
        this.collectCost += that.collectCost;
        this.joinCost += that.joinCost;
    }

    public void minus(NodeCost that) {
        this.shuffleCost -= that.shuffleCost;
        this.broadcastCost -= that.broadcastCost;
        this.computeCost -= that.computeCost;
        this.collectCost -= that.collectCost;
        this.joinCost -= that.joinCost;
    }

    public void multiply(double x) {
        this.shuffleCost *=x;
        this.broadcastCost *=x;
        this.computeCost *=x;
        this.collectCost *=x;
        this.joinCost *=x;
    }

    public void plusMultiply(NodeCost that,double x) {
        this.shuffleCost += that.shuffleCost*x;
        this.broadcastCost += that.broadcastCost*x;
        this.computeCost += that.computeCost*x;
        this.collectCost += that.collectCost*x;
        this.joinCost += that.joinCost*x;
    }

    public double getSummary() {
        return shuffleCost + broadcastCost + computeCost + collectCost+joinCost;
    }

    public static NodeCost  add(NodeCost a, NodeCost b) {
        return new NodeCost(a.shuffleCost + b.shuffleCost,
                a.broadcastCost + b.broadcastCost,
                a.computeCost + b.computeCost,
                a.collectCost + b.collectCost,
                a.joinCost+b.joinCost);
    }

    public static NodeCost  add(NodeCost a, NodeCost b, NodeCost c) {
        return new NodeCost(a.shuffleCost + b.shuffleCost+c.shuffleCost,
                a.broadcastCost + b.broadcastCost+c.broadcastCost,
                a.computeCost + b.computeCost+c.computeCost,
                a.collectCost + b.collectCost+c.collectCost,
                a.joinCost+b.joinCost+c.joinCost);
    }

    public static NodeCost ZERO() {
        return new NodeCost(0, 0, 0, 0,0);
    }

    public static NodeCost INF() {
        return new NodeCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,Double.MAX_VALUE);
    }

}
