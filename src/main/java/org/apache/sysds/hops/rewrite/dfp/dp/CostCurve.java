package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;
import org.apache.sysds.runtime.controlprogram.Program;

import java.util.ArrayList;
import java.util.HashMap;

public class CostCurve {

    public static class Node {
        double cost;
        Hop op;
        Node parent;
        ArrayList<Integer> covers = new ArrayList<>();
    }

    public static class Candidate {
        Hop root;
        //  Program program;
        //  ArrayList<Node> nodes = new ArrayList<>();
        HashMap<ArrayList<Integer>, Node> covers2Node = new HashMap<>();
        ArrayList<Double> leaves = new ArrayList<>();
    }

    ArrayList<Candidate> candidates = new ArrayList<>();

    CostCurve() {

    }

    Candidate buildCandidateFromHop(Hop root, SingleCse singleCse) {
        Candidate candidate = new Candidate();


        return candidate;
    }

    void deleteNode() {

    }

    double compare(Node a,Node b) {
        return 0;
    }


}
