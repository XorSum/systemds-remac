package org.apache.sysds.hops.rewrite.dfp.coordinate;


public class Main {
    public static void main(String[] args) {
        SingleCse out  = new SingleCse();
        SingleCse in  = new SingleCse();

        out.ranges.add(Range.of(1,4,false));
        out.ranges.add(Range.of(5,8,true));
        out.ranges.add(Range.of(9,12,true));

        in.ranges.add(Range.of(2,4,false));
        in.ranges.add(Range.of(5,7,true));
        in.ranges.add(Range.of(9,11,true));
        in.ranges.add(Range.of(13,15,false));
        in.ranges.add(Range.of(18,20,true));

        System.out.println(out.contain(in));

    }
}
