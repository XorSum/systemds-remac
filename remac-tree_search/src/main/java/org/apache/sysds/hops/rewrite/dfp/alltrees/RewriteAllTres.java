package org.apache.sysds.hops.rewrite.dfp.alltrees;

import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.rewrite.dfp.MySolution;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.hops.rewrite.dfp.utils.DeepCopyHopsDag;

import java.util.ArrayList;

import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class RewriteAllTres {

   public static long searchTime = 0;

    public static MySolution rewiteHopDag(Hop root, RewriteCoordinate rewriteCoordinateEstimator) {

        MySolution origianlSolution = new MySolution(root);
      //  rewriteCoordinateEstimator.estimate(origianlSolution,false);
        Hop copy = DeepCopyHopsDag.deepCopyHopsDag(root);
        copy = reorder(copy);
        long start = System.nanoTime();
        MySolution solution = ConcurrentBaoLi.generateAllTrees(copy,rewriteCoordinateEstimator);
        long end = System.nanoTime();
        searchTime += end-start;
        
//        System.out.println(solution);
        return  origianlSolution;
    }


}
