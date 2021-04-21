package org.apache.sysds.hops.rewrite.dfp.coordinate;

public class ManualSolution {

    Coordinate coordinate;

    public ManualSolution(Coordinate coordinate) {
        this.coordinate = coordinate;
    }

    SingleCse createSingleCseBfgsAta() {
        SingleCse sAta = new SingleCse();
        sAta.isConstant = true;
        sAta.name = coordinate.getRangeName(39, 40);
        sAta.ranges.add(Range.of(39, 40, true));
        sAta.ranges.add(Range.of(52, 53, true));
        sAta.ranges.add(Range.of(7, 8, true));
        sAta.ranges.add(Range.of(17, 18, true));
        sAta.ranges.add(Range.of(23, 24, true));
        sAta.ranges.add(Range.of(26, 27, false));
        sAta.ranges.add(Range.of(44, 45, false));
        sAta.ranges.add(Range.of(34, 35, true));
        return sAta;
    }

    SingleCse createSingleCseBfgsDtd2() {
        SingleCse sDtd = new SingleCse();
        sDtd.name = coordinate.getRangeName(1, 4);
        sDtd.ranges.add(Range.of(1, 4, false));
        sDtd.ranges.add(Range.of(11, 14, false));
        return sDtd;
    }

    SingleCse createSingleCseBfgsDtd4() {
        SingleCse sDtd = new SingleCse();
        sDtd.name = coordinate.getRangeName(1, 4);
        sDtd.ranges.add(Range.of(1, 4, false));
        sDtd.ranges.add(Range.of(11, 14, false));
        sDtd.ranges.add(Range.of(30, 33, false));
        sDtd.ranges.add(Range.of(46, 49, false));
        return sDtd;
    }


    SingleCse createSingleCseBfgsAtad() {
        SingleCse sAtahg = new SingleCse();
        sAtahg.name = coordinate.getRangeName(7, 10);
        sAtahg.ranges.add(Range.of(7, 10, false));
        sAtahg.ranges.add(Range.of(17, 20, false));
        sAtahg.ranges.add(Range.of(21, 24, true));
        sAtahg.ranges.add(Range.of(26, 29, false));
        sAtahg.ranges.add(Range.of(39, 42, false));
        sAtahg.ranges.add(Range.of(52, 55, false));
        return sAtahg;
    }

    SingleCse createSingleCseBfgsDtatad() {
        SingleCse sDtatad = new SingleCse();
        sDtatad.name = coordinate.getRangeName(5, 10);
        sDtatad.ranges.add(Range.of(5, 10, false));
        sDtatad.ranges.add(Range.of(15, 20, false));
        sDtatad.ranges.add(Range.of(37, 42, false));
        sDtatad.ranges.add(Range.of(50, 55, false));
        return sDtatad;
    }

    SingleCse createSingleCseBfgsAtah() {
        SingleCse sAtah = new SingleCse();
        sAtah.name = coordinate.getRangeName(34, 36);
        sAtah.ranges.add(Range.of(34, 36, false));
        sAtah.ranges.add(Range.of(43, 45, true));
        return sAtah;
    }

    SingleCse createSingleCseBfgsHy() {
        SingleCse sHy = new SingleCse();
        sHy.name = coordinate.getRangeName(5, 9);
        sHy.ranges.add(Range.of(6, 10, false));
        sHy.ranges.add(Range.of(15, 19, true));
        sHy.ranges.add(Range.of(21, 25, true));
        sHy.ranges.add(Range.of(32, 36, true));
        sHy.ranges.add(Range.of(37, 41, true));
        sHy.ranges.add(Range.of(43, 47, false));
        sHy.ranges.add(Range.of(50, 54, true));
        return sHy;
    }

    SingleCse createSingleCseBfgsY() {
        SingleCse sY = new SingleCse();
        sY.name = coordinate.getRangeName(5, 8);
        sY.ranges.add(Range.of(7, 10, false));
        sY.ranges.add(Range.of(15, 18, true));
        sY.ranges.add(Range.of(21, 24, true));
        sY.ranges.add(Range.of(26, 29, false));
        sY.ranges.add(Range.of(32, 35, true));
        sY.ranges.add(Range.of(37, 40, true));
        sY.ranges.add(Range.of(44, 47, false));
        sY.ranges.add(Range.of(50, 53, true));
        return sY;
    }

    SingleCse createSingleCseBfgsD() {
        SingleCse sd = new SingleCse();
        sd.name = coordinate.getRangeName(1, 2);
        sd.ranges.add(Range.of(1, 2, false));
        sd.ranges.add(Range.of(3, 4, true));
        sd.ranges.add(Range.of(9, 10, false));
        sd.ranges.add(Range.of(11, 12, false));
        sd.ranges.add(Range.of(13, 14, true));
        sd.ranges.add(Range.of(15, 16, true));
        sd.ranges.add(Range.of(21, 22, true));
        sd.ranges.add(Range.of(28, 29, false));
        sd.ranges.add(Range.of(30, 31, false));
        sd.ranges.add(Range.of(32, 33, true));
        sd.ranges.add(Range.of(37, 38, true));
        sd.ranges.add(Range.of(46, 47, false));
        sd.ranges.add(Range.of(48, 49, true));
        sd.ranges.add(Range.of(50, 51, true));
        return sd;
    }

    SingleCse createSingleCseBfgsDYtH() {
        SingleCse sd = new SingleCse();
        sd.name = coordinate.getRangeName(30, 36);
        sd.ranges.add(Range.of(30, 36, false));
        sd.ranges.add(Range.of(43, 49, true));
        return sd;
    }

    SingleCse createSingleCseBfgsDFull() {
        SingleCse sd = new SingleCse();
        sd.name = coordinate.getRangeName(1, 2);
        sd.ranges.add(Range.of(1, 2, false));
        sd.ranges.add(Range.of(3, 4, true));
        sd.ranges.add(Range.of(5, 6, true));
        sd.ranges.add(Range.of(9, 10, false));
        sd.ranges.add(Range.of(11, 12, false));
        sd.ranges.add(Range.of(13, 14, true));
        sd.ranges.add(Range.of(15, 16, true));
        sd.ranges.add(Range.of(19, 20, false));
        sd.ranges.add(Range.of(21, 22, true));
        sd.ranges.add(Range.of(28, 29, false));
        sd.ranges.add(Range.of(30, 31, false));
        sd.ranges.add(Range.of(32, 33, true));
        sd.ranges.add(Range.of(37, 38, true));
        sd.ranges.add(Range.of(41, 42, false));
        sd.ranges.add(Range.of(46, 47, false));
        sd.ranges.add(Range.of(48, 49, true));
        sd.ranges.add(Range.of(50, 51, true));
        sd.ranges.add(Range.of(54, 55, false));
        return sd;
    }

    SingleCse createSingleCseDfpY() {
        SingleCse sY = new SingleCse(); // atahg
        sY.name = coordinate.getRangeName(2, 5);
        sY.ranges.add(Range.of(2, 5, false));
        sY.ranges.add(Range.of(6, 9, true));
        sY.ranges.add(Range.of(11, 14, true));
        sY.ranges.add(Range.of(16, 19, false));
        sY.ranges.add(Range.of(24, 27, true));
        return sY;
    }

    SingleCse createSingleCseDfpHy() {
        SingleCse sHy = new SingleCse(); // hatahg
        sHy.name = coordinate.getRangeName(1, 5);
        sHy.ranges.add(Range.of(1, 5, false));
        sHy.ranges.add(Range.of(6, 10, true));
//        sHy.ranges.add(Range.of(15, 19, false));
        sHy.ranges.add(Range.of(11, 15, true));
        sHy.ranges.add(Range.of(24, 28, true));
        return sHy;
    }

    SingleCse createSingleCseDfpAta() {
        SingleCse sAta = new SingleCse(); // ata
        sAta.isConstant = true;
        sAta.name = coordinate.getRangeName(2, 3);
        sAta.ranges.add(Range.of(2, 3, false));
        sAta.ranges.add(Range.of(8, 9, true));
        sAta.ranges.add(Range.of(13, 14, true));
        sAta.ranges.add(Range.of(16, 17, false));
        sAta.ranges.add(Range.of(26, 27, true));
        return sAta;
    }

    SingleCse createSingleCseDfpD() {
        SingleCse sD = new SingleCse(); // d
        sD.name = coordinate.getRangeName(4, 5);
        sD.ranges.add(Range.of(4, 5, false));
        sD.ranges.add(Range.of(6, 7, true));
        sD.ranges.add(Range.of(11, 12, true));
        sD.ranges.add(Range.of(18, 19, false));
        sD.ranges.add(Range.of(20, 21, false));
        sD.ranges.add(Range.of(22, 23, true));
        sD.ranges.add(Range.of(24, 25, true));
        return sD;
    }

    SingleCse createSingleCseDfpDtd() {
        SingleCse sDtd = new SingleCse(); // dtd
        sDtd.name = coordinate.getRangeName(4, 7);
        sDtd.ranges.add(Range.of(4, 7, false));
        sDtd.ranges.add(Range.of(20, 23, false));
        return sDtd;
    }

    SingleCse createSingleCseDfpHata() {
        SingleCse sHata = new SingleCse();
        sHata.name = coordinate.getRangeName(1, 3);
        sHata.ranges.add(Range.of(1, 3, false));
        sHata.ranges.add(Range.of(8, 10, true));
        return sHata;
    }

    SingleCse createSingleCseDfpAtad() {
        SingleCse sAtad = new SingleCse();
        sAtad.name = coordinate.getRangeName(11, 14);
        sAtad.ranges.add(Range.of(11, 14, false));
        sAtad.ranges.add(Range.of(16, 19, true));
        sAtad.ranges.add(Range.of(26, 29, true));
        return sAtad;
    }

    MultiCse createMultiCseDfpAtaDtd() {
        MultiCse multiCse = new MultiCse();
        multiCse.cses.add(createSingleCseDfpAta());
        SingleCse sD = createSingleCseDfpD();
        sD.ranges.add(Range.of(28, 29, false));
        multiCse.cses.add(sD);
        multiCse.cses.add(createSingleCseDfpDtd());
        multiCse.cses.add(createSingleCseDfpHata());
        multiCse.cses.add(createSingleCseDfpAtad());
        return multiCse;
    }

    MultiCse createMultiCseDfpAta() {
        MultiCse multiCse = new MultiCse();
        multiCse.cses.add(createSingleCseDfpAta());
        multiCse.cses.add(createSingleCseDfpHy());
        multiCse.cses.add(createSingleCseDfpY());
        multiCse.cses.add(createSingleCseDfpD());
        return multiCse;
    }

    MultiCse createMultiCseDfp() {
        MultiCse multiCse = new MultiCse();
        multiCse.cses.add(createSingleCseDfpHy());
        multiCse.cses.add(createSingleCseDfpY());
        multiCse.cses.add(createSingleCseDfpD());
        return multiCse;
    }

    MultiCse createMultiCseDfpSporseAta() {
        SingleCse sHy = new SingleCse();
        sHy.name = coordinate.getRangeName(1, 4);
        sHy.ranges.add(Range.of(1, 4, false));
        sHy.ranges.add(Range.of(6, 9, true));
        SingleCse sAta = new SingleCse();
        sAta.name = coordinate.getRangeName(3, 4);
        sAta.ranges.add(Range.of(3, 4, false));
        sAta.ranges.add(Range.of(6, 7, true));
        MultiCse multiCse = new MultiCse();
        multiCse.cses.add(sHy);
        multiCse.cses.add(sAta);
        return multiCse;
    }

    MultiCse createMultiCseBfgsAtaDtd() {
        MultiCse multiCse = new MultiCse();
        multiCse.cses.add(createSingleCseBfgsDFull());
        multiCse.cses.add(createSingleCseBfgsAta());
        multiCse.cses.add(createSingleCseBfgsDtd4());
        multiCse.cses.add(createSingleCseBfgsAtad());
        multiCse.cses.add(createSingleCseBfgsAtah());
        multiCse.cses.add(createSingleCseBfgsDtatad());
        multiCse.cses.add(createSingleCseBfgsDYtH());
        return multiCse;
    }

    MultiCse createMultiCseBfgsAta() {
        MultiCse multiCse = new MultiCse();
        multiCse.cses.add(createSingleCseBfgsHy());
        multiCse.cses.add(createSingleCseBfgsY());
        multiCse.cses.add(createSingleCseBfgsD());
        multiCse.cses.add(createSingleCseBfgsDtd2());
        multiCse.cses.add(createSingleCseBfgsAta());
        multiCse.cses.add(createSingleCseBfgsDtatad());
        multiCse.cses.add(createSingleCseBfgsDYtH());
        return multiCse;
    }


    MultiCse createMultiCseBfgs() {
        MultiCse multiCse = new MultiCse();
        multiCse.cses.add(createSingleCseBfgsHy());
        multiCse.cses.add(createSingleCseBfgsY());
        multiCse.cses.add(createSingleCseBfgsD());
        multiCse.cses.add(createSingleCseBfgsDtd2());
        multiCse.cses.add(createSingleCseBfgsDtatad());
        multiCse.cses.add(createSingleCseBfgsDYtH());
        return multiCse;
    }

    MultiCse createMultiCseGdAta() {
        MultiCse multiCse = new MultiCse();
        SingleCse sAtATheta = new SingleCse();
        SingleCse sAtA = new SingleCse();
        SingleCse sAtB = new SingleCse();
        sAtATheta.name = coordinate.getRangeName(1, 3);
        sAtATheta.ranges.add(Range.of(1, 3, false));
        sAtATheta.ranges.add(Range.of(7, 9, false));
        sAtATheta.ranges.add(Range.of(14, 16, false));
        sAtATheta.ranges.add(Range.of(20, 22, false));
        sAtA.name = coordinate.getRangeName(1, 2);
        sAtA.ranges.add(Range.of(1, 2, false));
        sAtA.ranges.add(Range.of(7, 8, false));
        sAtA.ranges.add(Range.of(14, 15, false));
        sAtA.ranges.add(Range.of(20, 21, false));
        sAtB.name = coordinate.getRangeName(4, 5);
        sAtB.ranges.add(Range.of(4, 5, false));
        sAtB.ranges.add(Range.of(10, 11, false));
        sAtB.ranges.add(Range.of(17, 18, false));
        sAtB.ranges.add(Range.of(23, 24, false));
        multiCse.cses.add(sAtATheta);
        multiCse.cses.add(sAtA);
        multiCse.cses.add(sAtB);
        return multiCse;
    }

    MultiCse createMultiCseGdAtaTheta() {
        MultiCse multiCse = new MultiCse();
        SingleCse sAtA = new SingleCse();
        SingleCse sAtB = new SingleCse();
        sAtA.isConstant = true;
        sAtB.isConstant = true;
        sAtA.name = coordinate.getRangeName(1, 2);
        sAtA.ranges.add(Range.of(1, 2, false));
        sAtB.name = coordinate.getRangeName(4, 5);
        sAtB.ranges.add(Range.of(4, 5, false));
        multiCse.cses.add(sAtA);
        multiCse.cses.add(sAtB);
        return multiCse;
    }

    MultiCse createMultiCseGdAtbTheta() {
        MultiCse multiCse = new MultiCse();
        SingleCse sAtB = new SingleCse();
        sAtB.isConstant = true;
        sAtB.name = coordinate.getRangeName(4, 5);
        sAtB.ranges.add(Range.of(4, 5, false));
        multiCse.cses.add(sAtB);
        return multiCse;
    }

    MultiCse createMultiCseGAtA() {
        MultiCse multiCse = new MultiCse();
        SingleCse sAtA = new SingleCse();
        sAtA.isConstant = true;
        sAtA.name = coordinate.getRangeName(0, 1);
        sAtA.ranges.add(Range.of(0, 1, false));
        multiCse.cses.add(sAtA);
        return multiCse;
    }

    MultiCse createMultiCseGdAtb() {
        MultiCse multiCse = new MultiCse();
        SingleCse sAtB = new SingleCse();
        sAtB.name = coordinate.getRangeName(4, 5);
        sAtB.ranges.add(Range.of(4, 5, false));
        sAtB.ranges.add(Range.of(10, 11, false));
        sAtB.ranges.add(Range.of(17, 18, false));
        sAtB.ranges.add(Range.of(23, 24, false));
        multiCse.cses.add(sAtB);
        return multiCse;
    }


}
