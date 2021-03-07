package org.apache.sysds.hops.rewrite.dfp.costmodel;

public class CostSummary {

    public   double shuffleCostSummary = 0;
    public   double broadcastCostSummary = 0;
    public   double computeCostSummary = 0;
    public   double collectCostSummary = 0;

    public CostSummary() {
    }

    public void addShuffle(double c) {
        shuffleCostSummary += c;
    }
    public void addBroadcast(double c) {
        broadcastCostSummary += c;
    }
    public void addCompute(double c) {
        computeCostSummary += c;
    }
    public void addCollect(double c) {
        collectCostSummary += c;
    }

    public double getShuffleCostSummary() {
        return shuffleCostSummary;
    }

    public double getBroadcastCostSummary() {
        return broadcastCostSummary;
    }

    public double getComputeCostSummary() {
        return computeCostSummary;
    }

    public double getCollectCostSummary() {
        return collectCostSummary;
    }
}
