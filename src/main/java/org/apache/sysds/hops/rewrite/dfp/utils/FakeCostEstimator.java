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

import static org.apache.sysds.hops.estim.SparsityEstimator.OpCode;

public class FakeCostEstimator {

    public static void estimate(Hop hop, ExecutionContext ec) {
//        try {
//            Thread.sleep(1);
//        }catch (Exception e) {
//
//        }
        if (ec == null) return;
        if (Judge.isLeafMatrix(hop)) return;
        EstimatorMatrixHistogram estim= new EstimatorMatrixHistogram(true);
        MMNode mmNode =null;
        try {
            mmNode = rCreateMMNode(hop,ec);
          //  System.out.println("mmNode="+mmNode);
            estim.estim(mmNode);
        } catch (Exception e) {
          //  System.out.println("error");
        }
    }
    private static MMNode rCreateMMNode(Hop hop, ExecutionContext ec) {
//        MM,
//        MULT, PLUS, EQZERO, NEQZERO,
//        CBIND, RBIND,
//        TRANS, DIAG, RESHAPE;

        MMNode mmNode = null;
        if (Judge.isLeafMatrix(hop)) {
            MatrixBlock matrixBlock = getMatrix( hop.getName(),ec.getVariables());
            mmNode = new MMNode(matrixBlock);
        } else if (HopRewriteUtils.isMatrixMultiply(hop)) {
            MMNode left = rCreateMMNode(hop.getInput().get(0),ec);
            MMNode right = rCreateMMNode(hop.getInput().get(1),ec);
            mmNode = new MMNode(left,right, OpCode.MM);
        } else if (HopRewriteUtils.isScalarMatrixBinaryMult(hop)) {
            MMNode left = rCreateMMNode(hop.getInput().get(0),ec);
            MMNode right = rCreateMMNode(hop.getInput().get(1),ec);
            mmNode = new MMNode(left,right, OpCode.MULT);
        } else if (HopRewriteUtils.isBinary(hop, Types.OpOp2.PLUS)) {
            MMNode left = rCreateMMNode(hop.getInput().get(0),ec);
            MMNode right = rCreateMMNode(hop.getInput().get(1),ec);
            mmNode = new MMNode(left,right, OpCode.PLUS);
        } else if (HopRewriteUtils.isTransposeOperation(hop)) {
            MMNode left = rCreateMMNode(hop.getInput().get(0),ec);
            mmNode = new MMNode(left, OpCode.TRANS);
        }
        if (mmNode==null) {
            System.out.println("a");
        }
        return mmNode;
    }



    private static MatrixBlock getMatrix(String name, LocalVariableMap vars) {
        Data dat = vars.get(name);
        if( !(dat instanceof MatrixObject) )
            throw new HopsException("Input '"+name+"' not a matrix: "+dat.getDataType());
        return ((MatrixObject)dat).acquireReadAndRelease();
    }


}
