package org.apache.sysds.hops.rewrite.dfp.utils;

import org.apache.sysds.common.Types;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.hops.rewrite.dfp.coordinate.RewriteCoordinate;
import org.apache.sysds.parser.VariableSet;
import org.apache.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysds.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.runtime.meta.MatrixCharacteristics;

public class MemoryEstimateUtil {

    private static DataOp tread(String name, long dim1, long dim2, long nnz) {
        DataOp a = new DataOp(name, Types.DataType.MATRIX, Types.ValueType.FP64,
                Types.OpOpData.TRANSIENTREAD, "", dim1, dim2, nnz, 1000);
        return a;
    }

    private static Hop t(Hop a) {
        return HopRewriteUtils.createTranspose(a);
    }

    private static Hop multi(Hop a, Hop b) {
        return HopRewriteUtils.createMatrixMultiply(a, b);
    }

    public static Hop createH(long dim1, long dim2, long nnz) {
        Hop h67 = tread("h", dim2, dim2, dim2 * dim2);
        Hop h73 = tread("a", dim1, dim2, nnz);
        Hop h72 = HopRewriteUtils.createTranspose(h73);
        Hop h76 = tread("g", dim2, 1, dim2);
        Hop h75 = HopRewriteUtils.createMatrixMultiply(h67, h76);
        Hop h74 = HopRewriteUtils.createMatrixMultiply(h73, h75);
        Hop h71 = HopRewriteUtils.createMatrixMultiply(h72, h74);
        Hop h70 = HopRewriteUtils.createMatrixMultiply(h67, h71);
        Hop h80 = HopRewriteUtils.createTranspose(h75);
        Hop h79 = HopRewriteUtils.createMatrixMultiply(h80, h72);
        Hop h78 = HopRewriteUtils.createMatrixMultiply(h79, h73);
        Hop h77 = HopRewriteUtils.createMatrixMultiply(h78, h67);
        Hop h69 = HopRewriteUtils.createMatrixMultiply(h70, h77);
        Hop h82 = HopRewriteUtils.createMatrixMultiply(h78, h70);
        Hop h81 = HopRewriteUtils.createUnary(h82, Types.OpOp1.CAST_AS_DOUBLE);
        Hop h68 = HopRewriteUtils.createBinary(h69, h81, Types.OpOp2.DIV);
        Hop h66 = HopRewriteUtils.createBinary(h67, h68, Types.OpOp2.MINUS);
        Hop h84 = HopRewriteUtils.createMatrixMultiply(h75, h80);
        Hop h87 = HopRewriteUtils.createMatrixMultiply(h78, h75);
        Hop h86 = HopRewriteUtils.createUnary(h87, Types.OpOp1.CAST_AS_DOUBLE);
        Hop h85 = HopRewriteUtils.createBinary(h86, new LiteralOp(2), Types.OpOp2.MULT);
        Hop h83 = HopRewriteUtils.createBinary(h84, h85, Types.OpOp2.DIV);
        Hop h65 = HopRewriteUtils.createBinary(h66, h83, Types.OpOp2.PLUS);
        Hop h64 = HopRewriteUtils.createTransientWrite("h", h65);
        return h64;
    }

    static void printPatritions(String name, long r, long c, long nnz) {
        MatrixCharacteristics dc1 = new MatrixCharacteristics(r, c, 1000, nnz);
        int p1 = SparkUtils.getNumPreferredPartitions(dc1);
        System.out.println(name + ", dc: " + dc1 + ", partitions: " + p1);
    }

    private static int getPreferredParJoin(DataCharacteristics mc1, DataCharacteristics mc2) {
//        int defPar = SparkExecutionContext.getDefaultParallelism(true);
        int defPar = 144;
        int maxSizeIn = SparkUtils.getNumPreferredPartitions(mc1) +
                SparkUtils.getNumPreferredPartitions(mc2);
        return (maxSizeIn > defPar / 2) ? Math.max(maxSizeIn, defPar) : maxSizeIn;
    }

    private static int getMaxParJoin(DataCharacteristics mc1, DataCharacteristics mc2) {
        return mc1.colsKnown() ? (int) mc1.getNumColBlocks() :
                mc2.rowsKnown() ? (int) mc2.getNumRowBlocks() :
                        Integer.MAX_VALUE;
    }

    private static void cpmmPatritions(String name, long r, long c, long nnz) {
        MatrixCharacteristics mc1 = new MatrixCharacteristics(c, r, 1000, nnz);
        MatrixCharacteristics mc2 = new MatrixCharacteristics(r, c, 1000, nnz);
        int numPreferred = getPreferredParJoin(mc1, mc2);
        int numMaxJoin = getMaxParJoin(mc1, mc2);
        int numPartJoin = Math.min(numMaxJoin, numPreferred);
        System.out.println(name + ", " + numPreferred + ", " + numMaxJoin + ", " + numPartJoin);
    }

    private static void cpmmPatritions() {
     //   cpmmPatritions("criteo_0_1", 116800000, 47, 3237650536l);
        cpmmPatritions("criteo_20000", 58400000, 14955, 2277596265l);
        cpmmPatritions("criteo_40000", 58400000, 8692, 2277598765l);
        cpmmPatritions("reddit_20000", 104473929, 20000, 2014518046);
        cpmmPatritions("reddit_5000", 104473929, 5000, 2012581099);
      //  cpmmPatritions("reddit_9_10", 120000000, 34, 1676633971);
    }


    public static void main(String[] args) {
//        Hop root = createH(1000000,10000,100000000);
//        VariableSet variableSet = new VariableSet();
//        variableSet.addVariable("h",null);
//        variableSet.addVariable("g",null);
//        variableSet.addVariable("x",null);
//        variableSet.addVariable("d",null);
//        variableSet.addVariable("i",null);
//        RewriteCoordinate rewriteCoordinate = new RewriteCoordinate(null,null);
//        rewriteCoordinate.variablesUpdated = variableSet;
//        rewriteCoordinate.constantUtil = new ConstantUtil(variableSet);
//        rewriteCoordinate.rewiteHopDag(root);

//        Hop h = tread("h",20000,20000,20000*20000);
//        double size =  MatrixBlock.estimateSizeInMemory(20000,20000,1.0);
//        System.out.println("size="+size);
//        double size1 =  MatrixBlock.estimateSizeInMemory(14955,14955,0.0026180407098227864);
        double size1 = MatrixBlock.estimateSizeInMemory(14955, 14955, 0.00210162714451911);

        System.out.println("criteo_20000=" + size1);
//        double size2 =  MatrixBlock.estimateSizeInMemory(8692,8692,0.006952184527876477);
        double size2 = MatrixBlock.estimateSizeInMemory(8692, 8692, 0.00556222096943855);

        System.out.println("criteo_40000=" + size2);


        printPatritions("criteo_0_1", 116800000, 47, 3237650536l);
        printPatritions("criteo_20000", 58400000, 14955, 2277596265l);
        printPatritions("criteo_40000", 58400000, 8692, 2277598765l);
        printPatritions("reddit_20000", 104473929, 20000, 2014518046);
        printPatritions("reddit_5000", 104473929, 5000, 2012581099);
        printPatritions("reddit_9_10", 120000000, 34, 1676633971);

        printPatritions("criteo_0_1", 47, 116800000, 3237650536l);
        printPatritions("criteo_20000", 14955, 58400000, 2277596265l);
        printPatritions("criteo_40000", 8692, 58400000, 2277598765l);
        printPatritions("reddit_20000", 20000, 104473929, 2014518046);
        printPatritions("reddit_5000", 5000, 104473929, 2012581099);
        printPatritions("reddit_9_10", 34, 120000000, 1676633971);

        cpmmPatritions();

    }

}
