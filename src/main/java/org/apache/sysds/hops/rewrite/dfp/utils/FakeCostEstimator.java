package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.HopsException;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.estim.MMNode;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.runtime.controlprogram.LocalVariableMap;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;

import javax.xml.bind.SchemaOutputResolver;

import static org.apache.sysds.hops.estim.SparsityEstimator.OpCode;

public class FakeCostEstimator {

    public static long time = 0;

    public static void printTime() {
     //   System.out.println("fake estimator time = " + (time/1000000.0) + "ms" );
    }

    public static double estimate(Hop hop, ExecutionContext ec) {
//        try {
//            Thread.sleep(1);
//        }catch (Exception e) {
//
//        }
        long start = System.nanoTime();
        double cost = 0;
        if (ec == null) {
            hop.refreshSizeInformation();
             cost = preEstimate(hop);
//            System.out.println("cost="+cost);
        } else {
            if (!Judge.isLeafMatrix(hop)) {
                EstimatorMatrixHistogram estim = new EstimatorMatrixHistogram(true);
                try {
                    MMNode mmNode = rCreateMMNode(hop, estim, ec);
                    //  System.out.println("mmNode="+mmNode);
                    estim.estim(mmNode);
                } catch (Exception e) {
                    //  System.out.println("error");
                }
            }
        }
        long end = System.nanoTime();
        time += end - start;
        return cost;
    }

    private static double preEstimate(Hop hop) {
        double sum = 0;
//        System.out.println("dims: "+ hop.getDim1()+" "+hop.getDim2()+" dc="+hop.getDataCharacteristics());
        if (!hop.isMatrix()) return 0;
        if (Judge.isLeafMatrix(hop)) {
            if (hop.isMatrix() && hop.getDim1()>0&&hop.getDim2()>0)
                sum += hop.getDim1() * hop.getDim2();
        } else {
            for (int i = 0; i < hop.getInput().size(); i++) {
                Hop child = hop.getInput().get(i);
                sum += preEstimate(child);
            }
            if (HopRewriteUtils.isMatrixMultiply(hop)) {
                Hop left = hop.getInput().get(0);
                Hop right = hop.getInput().get(1);
                if (left.getDim1() > 0 && left.getDim2() > 0 && right.getDim2() > 0)
                    sum += left.getDim1() * left.getDim2() * right.getDim2();
            } else if (HopRewriteUtils.isBinary(hop)) {
                Hop left = hop.getInput().get(0);
                Hop right = hop.getInput().get(1);
                if ( left.isScalar() && right.isMatrix() ) {
                    left = right;
                }
                if ( left.isMatrix() && left.getDim1() > 0 && left.getDim2() > 0)
                    sum += left.getDim1() * left.getDim2();
            } else {
                Hop left = hop.getInput().get(0);
                if (left.isMatrix() && left.getDim1() > 0 && left.getDim2() > 0)
                    sum += left.getDim1() * left.getDim2();
            }
        }
        return sum;
    }


    private static MMNode rCreateMMNode(Hop hop, EstimatorMatrixHistogram estim, ExecutionContext ec) {
//        MM,
//        MULT, PLUS, EQZERO, NEQZERO,
//        CBIND, RBIND,
//        TRANS, DIAG, RESHAPE;

        MMNode mmNode = null;
        if (Judge.isLeafMatrix(hop)) {
            MatrixBlock matrixBlock = getMatrix(hop.getName(), ec.getVariables());
            mmNode = new MMNode(matrixBlock);
        } else if (HopRewriteUtils.isMatrixMultiply(hop)) {
            MMNode left = rCreateMMNode(hop.getInput().get(0), estim, ec);
            MMNode right = rCreateMMNode(hop.getInput().get(1), estim, ec);
            mmNode = new MMNode(left, right, OpCode.MM);
        } else if (HopRewriteUtils.isScalarMatrixBinaryMult(hop)) {
            MMNode left = rCreateMMNode(hop.getInput().get(0), estim, ec);
            MMNode right = rCreateMMNode(hop.getInput().get(1), estim, ec);
            mmNode = new MMNode(left, right, OpCode.MULT);
        } else if (HopRewriteUtils.isBinary(hop, Types.OpOp2.PLUS)) {
            MMNode left = rCreateMMNode(hop.getInput().get(0), estim, ec);
            MMNode right = rCreateMMNode(hop.getInput().get(1), estim, ec);
            mmNode = new MMNode(left, right, OpCode.PLUS);
        } else if (HopRewriteUtils.isTransposeOperation(hop)) {
            MMNode left = rCreateMMNode(hop.getInput().get(0), estim, ec);
            mmNode = new MMNode(left, OpCode.TRANS);
        }
        if (mmNode == null) {
            System.out.println("a");
        }
        return mmNode;
    }


    private static MatrixBlock getMatrix(String name, LocalVariableMap vars) {
        Data dat = vars.get(name);
        if (!(dat instanceof MatrixObject))
            throw new HopsException("Input '" + name + "' not a matrix: " + dat.getDataType());
        return ((MatrixObject) dat).acquireReadAndRelease();
    }


}
