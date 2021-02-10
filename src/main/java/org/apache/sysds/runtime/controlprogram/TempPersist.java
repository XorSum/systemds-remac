package org.apache.sysds.runtime.controlprogram;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;
import java.util.LinkedList;

public class TempPersist {
    public static class Frame {

        private final ArrayList<String> rddLabels = new ArrayList<>();
        private final ArrayList<JavaPairRDD<?, ?>> javaPairRDDS = new ArrayList<>();

        public void addCseLabel(String rddLabel) {
            System.out.println("Add variable label " + rddLabel);
            rddLabels.add(rddLabel);
        }

        public boolean shouldPersist(String rddLabel) {
            boolean ans = rddLabels.contains(rddLabel);
           // System.out.println(ans + ", check " + rddLabel + " in " + rddLabels);
            return ans;
        }

        public void addRdd(JavaPairRDD<?, ?> rdd) {
            javaPairRDDS.add(rdd);
        }

        public void cleanPersistedCses() {
            for (JavaPairRDD rdd : javaPairRDDS) rdd.unpersist();
            System.out.println("Unpersist " + javaPairRDDS.size() + " rdds");
            System.out.println("Clear labels " + rddLabels);
            javaPairRDDS.clear();
            rddLabels.clear();
        }
    }

    private static final LinkedList<Frame> frames = new LinkedList<>();

    public static Frame createNewFrame() {
        if (frames.size()==0||frames.getLast().rddLabels.size()>0) {
            Frame frame = new Frame();
            frames.add(frame);
            return frame;
        } else {
            return frames.getLast();
        }
    }

    public static void addCseLabel(String rddLabel) {
        if (frames.size() == 0) createNewFrame();
        Frame frame = frames.getLast();
        frame.addCseLabel(rddLabel);
    }

    public static boolean shouldPersist(String rddLabel) {
        boolean ans = false;
        for (Frame frame : frames) {
            if(frame.shouldPersist(rddLabel)) {
                ans = true;
                break;
            }
        }
        System.out.println(ans + ", check " + rddLabel + " in " + frames.stream().map(x->x.rddLabels.toString()).reduce((x,y)->x+","+y).get());
        return ans;
    }

    public static void addRdd(JavaPairRDD<?, ?> rdd) {
        if (frames.size() == 0) createNewFrame();
        Frame frame = frames.getLast();
        frame.addRdd(rdd);
    }

    public static void cleanPersistedCses() {
        if (frames.size()>2) {
            Frame frame = frames.getFirst();
            frame.cleanPersistedCses();
            frames.removeFirst();
        }
    }

}
