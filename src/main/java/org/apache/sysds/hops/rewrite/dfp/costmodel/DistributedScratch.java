package org.apache.sysds.hops.rewrite.dfp.costmodel;

import org.apache.sysds.common.Types;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.hops.DataOp;
import org.apache.sysds.hops.Hop;
import org.apache.sysds.hops.LiteralOp;
import org.apache.sysds.hops.estim.EstimatorMatrixHistogram;
import org.apache.sysds.hops.rewrite.HopRewriteUtils;
import org.apache.sysds.parser.*;
import org.apache.sysds.runtime.controlprogram.Program;
import org.apache.sysds.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysds.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysds.runtime.instructions.cp.Data;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.meta.DataCharacteristics;
import org.apache.sysds.utils.Hash;

import java.util.ArrayList;
import java.util.HashMap;

public class DistributedScratch {

    public static ExecutionContext ec;

    private static HashMap<String, EstimatorMatrixHistogram.MatrixHistogram> map = new HashMap<>();

    public static void setMatrixHistogram(String name, EstimatorMatrixHistogram.MatrixHistogram histogram) {
        map.put(name, histogram);
    }


   public static EstimatorMatrixHistogram.MatrixHistogram createFullHistogram(int row, int col) {
        if (row<=0||col<=0) {
            System.out.println(row+" "+col);
            System.exit(-1);
        }
        int[] r = new int[row];
        for (int i = 0; i < row; i++) r[i] = col;
        int[] c = new int[col];
        for (int i = 0; i < col; i++) c[i] = row;
        return new EstimatorMatrixHistogram.MatrixHistogram(r, null, c, null, col, row);
    }

    public static EstimatorMatrixHistogram.MatrixHistogram getMatrixHistogram(String name) {
        if (map.containsKey(name)) return map.get(name);
        if (ec == null) {
            System.out.println("ec == null");
            return null;
        }
        if (!ec.containsVariable(name)) {
            System.out.println("!ec.containsVariable(name)");
            return null;
        }
        EstimatorMatrixHistogram.MatrixHistogram histogram = null;
        recompileFlag = false;
        try {
            if (ec != null) {
                Program rtprog = getScrtchRtProg(name);
                rtprog.execute(ec);
                System.out.println("execute succ");
                histogram = getScrtchMatrixes(name);
                map.put(name, histogram);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("fail");
        }
        recompileFlag = true;

        return histogram;
    }

    public static boolean recompileFlag = true;


    private static Program getScrtchRtProg(String name) {
        MatrixObject data =(MatrixObject) ec.getVariable(name);
        System.out.println(data);
        DataCharacteristics dc = data.getMetaData().getDataCharacteristics();
        Hop a = HopRewriteUtils.createTransientRead(name, new DataOp("l", Types.DataType.MATRIX, Types.ValueType.FP64,
                Types.OpOpData.TRANSIENTREAD, "", dc.getRows(), dc.getCols(), dc.getNonZeros(), dc.getBlocksize()));
        Hop zero = new LiteralOp(0);
        Hop one = new LiteralOp(1);
        Hop a_ne_0 = HopRewriteUtils.createBinary(a, zero, Types.OpOp2.NOTEQUAL);
        Hop hr = HopRewriteUtils.createAggUnaryOp(a_ne_0, Types.AggOp.SUM, Types.Direction.Row);
        Hop hc = HopRewriteUtils.createAggUnaryOp(a_ne_0, Types.AggOp.SUM, Types.Direction.Col);
        Hop hr_eq_1 = HopRewriteUtils.createBinary(hr, one, Types.OpOp2.EQUAL);
        Hop hc_eq_1 = HopRewriteUtils.createBinary(hc, one, Types.OpOp2.EQUAL);
        Hop t_hr_eq_1 = HopRewriteUtils.createTranspose(hr_eq_1);
        Hop t_hc_eq_1 = HopRewriteUtils.createTranspose(hc_eq_1);
        Hop hec = HopRewriteUtils.createMatrixMultiply(t_hr_eq_1, a_ne_0);
        Hop her = HopRewriteUtils.createMatrixMultiply(a_ne_0, t_hc_eq_1);

        DMLProgram prog = new DMLProgram();
        StatementBlock sb = new StatementBlock();
        ArrayList<Hop> hops = new ArrayList<>();
        VariableSet livein = new VariableSet();
        VariableSet liveout = new VariableSet();

        livein.addVariable(name, new DataIdentifier(name, Types.DataType.MATRIX, Types.ValueType.FP64));
        liveout.addVariable(name, new DataIdentifier(name, Types.DataType.MATRIX, Types.ValueType.FP64));

        String name_hr = "_sVar" + name + "hr";
        Hop tw_hr = HopRewriteUtils.createTransientWrite(name_hr, hr);
        hops.add(tw_hr);
        liveout.addVariable(name_hr, new DataIdentifier(name_hr, Types.DataType.MATRIX, Types.ValueType.INT64));

        String name_hc = "_sVar" + name + "hc";
        Hop tw_hc = HopRewriteUtils.createTransientWrite(name_hc, hc);
        hops.add(tw_hc);
        liveout.addVariable(name_hc, new DataIdentifier(name_hc, Types.DataType.MATRIX, Types.ValueType.INT64));

        String name_her = "_sVar" + name + "her";
        liveout.addVariable(name_her, new DataIdentifier(name_her, Types.DataType.MATRIX, Types.ValueType.INT64));
        Hop tw_her = HopRewriteUtils.createTransientWrite(name_her, her);
        hops.add(tw_her);

        String name_hec = "_sVar" + name + "hec";
        liveout.addVariable(name_hec, new DataIdentifier(name_hec, Types.DataType.MATRIX, Types.ValueType.INT64));
        Hop tw_hec = HopRewriteUtils.createTransientWrite(name_hec, hec);
        hops.add(tw_hec);

        sb.setHops(hops);
        sb.setLiveIn(livein);
        sb.setLiveOut(liveout);
        prog.addStatementBlock(sb);

        try {
            DMLTranslator dmlt = new DMLTranslator(prog);
            dmlt.constructLops(prog);
            return dmlt.getRuntimeProgram(prog, ConfigurationManager.getDMLConfig());
        } catch (Exception e) {
            e.printStackTrace();
//            throw e;
            //   System.out.println("x");
        }
        return null;
    }


    private static int[] getRowArray(String name) {
        MatrixBlock mb = getMatrixBlock(name);
        DataCharacteristics dc = mb.getDataCharacteristics();
        int[] r = new int[(int) dc.getRows()];
        for (int i = 0; i < dc.getRows(); i++) r[i] = (int) mb.getValue(i, 0);
        return r;
    }

    private static int[] getColArray(String name) {
        MatrixBlock mb = getMatrixBlock(name);
        DataCharacteristics dc = mb.getDataCharacteristics();
        int[] c = new int[(int) dc.getCols()];
        for (int i = 0; i < dc.getCols(); i++) c[i] = (int) mb.getValue(0, i);
        return c;
    }

    private static MatrixBlock getMatrixBlock(String name) {
        MatrixObject matrixObj = ec.getMatrixObject(name);
//        System.out.println("obj" + matrixObj);
        MatrixBlock matrixBlock = matrixObj.acquireReadAndRelease();
//        System.out.println("blk=" + matrixBlock);
        return matrixBlock;
    }

    private static EstimatorMatrixHistogram.MatrixHistogram getScrtchMatrixes(String name) {
        ArrayList<MatrixBlock> list = new ArrayList<>();

        int[] r = getRowArray("_sVar" + name + "hr");
        int[] r1e = getRowArray("_sVar" + name + "her");
        int[] c = getColArray("_sVar" + name + "hc");
        int[] c1e = getColArray("_sVar" + name + "hec");
        int rmax = 0;
        for (int i : r) {
            rmax = Math.max(i, rmax);
        }
        int cmax = 0;
        for (int i : c) {
            cmax = Math.max(i, cmax);
        }

        EstimatorMatrixHistogram.MatrixHistogram histogram = new EstimatorMatrixHistogram.MatrixHistogram(r, r1e, c, c1e, rmax, cmax);

        return histogram;
    }

}
