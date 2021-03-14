package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;

import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class MemoryEstimateUtil {

    private static DataOp tread(String name,  long dim1,long dim2,long  nnz ) {
        DataOp a = new DataOp(name, Types.DataType.MATRIX, Types.ValueType.FP64,
                Types.OpOpData.TRANSIENTREAD, "", dim1, dim2, nnz, 1000);
        return  a;
    }

    private static Hop t(Hop a) {
        return HopRewriteUtils.createTranspose(a);
    }

    private static Hop multi(Hop a,Hop b) {
        return HopRewriteUtils.createMatrixMultiply(a,b);
    }

    public static Hop createH(long dim1,long dim2,long  nnz  ) {
//        (13567) TRead h [300,300,1000,-1] [0,0,1 -> 1MB], CP, notPersist
        Hop h13567 = tread("h",dim2,dim2,dim2*dim2);
//        (13573) TRead a [10000,300,1000,3000000] [0,0,23 -> 23MB], CP, notPersist
        Hop h13573 = tread("a",dim1,dim2,nnz);
//        (13572) r(r') (13573) [300,10000,1000,3000000] [23,0,23 -> 46MB], SPARK, notPersist
        Hop h13572 = HopRewriteUtils.createTranspose(h13573);
//        (13576) TRead g [300,1,1000,-1] [0,0,0 -> 0MB], CP, notPersist
        Hop h13576 = tread("g",dim2,1,dim2);
//        (13575) ba(+*) (13567,13576) [300,1,1000,-1] [1,0,0 -> 1MB], SPARK, notPersist
        Hop h13575 = HopRewriteUtils.createMatrixMultiply(h13567,h13576);
//        (13574) ba(+*) (13573,13575) [10000,1,1000,-1] [23,0,0 -> 23MB], SPARK, notPersist
        Hop h13574 = HopRewriteUtils.createMatrixMultiply(h13573,h13575);
//        (13571) ba(+*) (13572,13574) [300,1,1000,-1] [23,0,0 -> 23MB], SPARK, notPersist
        Hop h13571 = HopRewriteUtils.createMatrixMultiply(h13572,h13574);
//        (13570) ba(+*) (13567,13571) [300,1,1000,-1] [1,0,0 -> 1MB], SPARK, notPersist
        Hop h13570 = HopRewriteUtils.createMatrixMultiply(h13567,h13571);
//        (13580) r(r') (13575) [1,300,1000,-1] [0,0,0 -> 0MB], SPARK, notPersist
        Hop h13580 = HopRewriteUtils.createTranspose(h13575);
//        (13579) ba(+*) (13580,13572) [1,10000,1000,-1] [23,0,0 -> 23MB], SPARK, notPersist
        Hop h13579 = HopRewriteUtils.createMatrixMultiply(h13580,h13572);
//        (13578) ba(+*) (13579,13573) [1,300,1000,-1] [23,0,0 -> 23MB], SPARK, notPersist
        Hop h13578 = HopRewriteUtils.createMatrixMultiply(h13579,h13573);
//        (13577) ba(+*) (13578,13567) [1,300,1000,-1] [1,0,0 -> 1MB], SPARK, notPersist
        Hop h13577 = HopRewriteUtils.createMatrixMultiply(h13578,h13567);
//        (13569) ba(+*) (13570,13577) [300,300,1000,-1] [0,0,1 -> 1MB], SPARK, notPersist
        Hop h13569 = HopRewriteUtils.createMatrixMultiply(h13570,h13577);
//        (13582) ba(+*) (13578,13570) [1,1,1000,-1] [0,0,0 -> 0MB], SPARK, notPersist
        Hop h13582 = HopRewriteUtils.createMatrixMultiply(h13578,h13570);
//        (13581) u(castdts) (13582) [0,0,0,-1] [0,0,0 -> 0MB], SPARK, notPersist
        Hop h13581 = HopRewriteUtils.createUnary(h13582, Types.OpOp1.CAST_AS_DOUBLE);
//        (13568) b(/) (13569,13581) [300,300,1000,-1] [1,0,1 -> 1MB], SPARK, notPersist
        Hop h13568 = HopRewriteUtils.createBinary(h13569,h13581, Types.OpOp2.DIV);
//        (13566) b(-) (13567,13568) [300,300,1000,-1] [1,0,1 -> 2MB], SPARK, notPersist
        Hop h13566 = HopRewriteUtils.createBinary(h13567,h13568, Types.OpOp2.MINUS);
//        (13584) ba(+*) (13575,13580) [300,300,1000,-1] [0,0,1 -> 1MB], SPARK, notPersist
        Hop h13584 = HopRewriteUtils.createMatrixMultiply(h13575,h13580);
//        (13587) ba(+*) (13578,13575) [1,1,1000,-1] [0,0,0 -> 0MB], SPARK, notPersist
        Hop h13587 = HopRewriteUtils.createMatrixMultiply(h13578,h13575);
//        (13586) u(castdts) (13587) [0,0,0,-1] [0,0,0 -> 0MB], SPARK, notPersist
        Hop h13586 = HopRewriteUtils.createUnary(h13587, Types.OpOp1.CAST_AS_DOUBLE);
//        (13585) b(*) (13586) [0,0,0,-1] [0,0,0 -> 0MB], SPARK, notPersist
        Hop h13585 = HopRewriteUtils.createBinary(h13586,new LiteralOp(2), Types.OpOp2.MULT);
//        (13583) b(/) (13584,13585) [300,300,1000,-1] [1,0,1 -> 1MB], SPARK, notPersist
        Hop h13583 = HopRewriteUtils.createBinary(h13584,h13585, Types.OpOp2.DIV);
//        (13565) b(+) (13566,13583) [300,300,1000,-1] [1,0,1 -> 2MB], SPARK, notPersist
        Hop h13565 = HopRewriteUtils.createBinary(h13566,h13583, Types.OpOp2.PLUS);
//        (13564) TWrite h (13565) [300,300,1000,-1] [1,0,0 -> 1MB], SPARK, notPersist
        Hop h13564 = HopRewriteUtils.createTransientWrite("h",h13565);
        return h13564;
    }

    public static void main(String[] args) {
        Hop root = createH(1000000,10000,100000000);
        VariableSet variableSet = new VariableSet();
        variableSet.addVariable("h",null);
        variableSet.addVariable("g",null);
        variableSet.addVariable("x",null);
        variableSet.addVariable("d",null);
        variableSet.addVariable("i",null);
        RewriteCoordinate rewriteCoordinate = new RewriteCoordinate(null,null);
        rewriteCoordinate.variablesUpdated = variableSet;
        rewriteCoordinate.constantUtil = new ConstantUtil(variableSet);
        rewriteCoordinate.rewiteHopDag(root);

    }

}
