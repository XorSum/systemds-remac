package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.AggBinaryOp;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.matrix.operators.AggregateOperator;

import static org.apache.sysds.hops.rewrite.dfp.utils.Reorder.reorder;

public class MemoryEstimateUtil {

    public static void main(String[] args) {

        long prod = 12480000000l;
        long id = 1;
        double sparsity = 0.00074;
        for (long dim2 = 1000;dim2 <=20000;dim2 +=3000) {
            long dim1 = 1000;
            while (MatrixBlock.estimateSizeInMemory(dim1,dim2,sparsity)<=3e10) {
                dim1 += 1000;
            }
//            long dim1 = prod/dim2;
//            System.out.printf("%d %d %d %d\n",dim1,dim2,dim1*dim2,MatrixBlock.estimateSizeInMemory(dim1,dim2,sparsity));
            System.out.printf("run %d %d %f %d\n",dim1,dim2,sparsity,id);
            id ++;
        }

//        long dim1 = 2275857466l;
//        long dim2 = 58355534;
//        long nnz = 96855;

//        long dim1 = 12000000 ;
//        long dim2 =  1000;
//        long nnz =(long)(0.2*dim1 * dim2);
////        long nnz = 383010135;
////        System.out.println(nnz);
//        System.out.println(nnz);
//
//
//        DataOp dataOp = new DataOp( "", Types.DataType.MATRIX, Types.ValueType.FP64,
//                Types.OpOpData.TRANSIENTREAD,"",dim1,dim2,nnz,0  );
//
//        System.out.println(dataOp.getMemEstimate());

//        DataOp b = new DataOp( "l", Types.DataType.MATRIX, Types.ValueType.FP64, Types.OpOpData.TRANSIENTREAD,"a",dim1,1,dim1,0  );

//        AggBinaryOp ab = HopRewriteUtils.createMatrixMultiply(a,b);

   //     System.out.println("exec type of a :" + a.optFindExecType());

//        System.out.println("exec type of b :" +b.optFindExecType());

//        System.out.println(ab.getMemEstimate());

//        System.out.println("exec type of a*b :" + ab.optFindExecType());

//        DataOp  h = new DataOp("h", Types.DataType.MATRIX, Types.ValueType.FP64, Types.OpOpData.TRANSIENTREAD,"f",100,100,100,100 );
//
//        //   DataOp minus1 = new DataOp("-1",Types.DataType.SCALAR, Types.ValueType.FP64, Types.OpOpData.TRANSIENTREAD,new HashMap<>());
//
//        LiteralOp minus1 = new LiteralOp(-1);
//
//        DataOp  g = new DataOp("g", Types.DataType.MATRIX, Types.ValueType.FP64, Types.OpOpData.TRANSIENTREAD,"f",100,1,100,100 );
//
//        Hop a = HopRewriteUtils.createBinary(h,minus1, Types.OpOp2.MULT);
//        Hop b = HopRewriteUtils.createMatrixMultiply(a,g);
//
//        System.out.println(MyExplain.myExplain(b));
//
//        Hop c = reorder(b);
//
//        System.out.println(MyExplain.myExplain(c));


    }

}
