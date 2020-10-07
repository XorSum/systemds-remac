package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.hops.Hop;

public class FakeCostEstimator {

    public static void estimate(Hop hop) {
//        try {
//            Thread.sleep(1);
//        }catch (Exception e) {
//
//        }
        rEstimate(hop);
    }

    private static void rEstimate(Hop hop) {
        for (Hop h: hop.getInput()) {
            rEstimate(h);
        }
    }


}
