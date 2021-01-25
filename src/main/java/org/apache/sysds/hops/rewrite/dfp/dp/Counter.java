package org.apache.sysds.hops.rewrite.dfp.dp;


import org.apache.sysds.hops.rewrite.dfp.coordinate.SingleCse;

import java.util.HashMap;

public class Counter<T> {

    HashMap<T, Integer> counter = new HashMap<>();
    Counter() {
    }

    void increment(T t) {
        if (counter.containsKey(t)) {
            int v = counter.get(t);
            counter.put(t, v + 1);
            if (t instanceof SingleCse)
            System.out.println("increment " + (v + 1)+" "+t);
        } else {
            counter.put(t, 1);
            if (t instanceof SingleCse)
            System.out.println("increment " + 1+" "+t);
        }
    }

    void decrement(T t) {
        if (counter.containsKey(t)) {
            int v = counter.get(t);
            if (v > 0) {
                counter.put(t, v - 1);
                if (t instanceof SingleCse)
                    System.out.println("decrement "+ (v - 1)+" "+t);
            } else {
                System.out.println("error: counter negative");
                System.exit(0);
            }
        } else {
            System.out.println("error: counter negative");
            System.exit(0);
        }
    }


    Integer getValue(T t) {
        return counter.getOrDefault(t, -1);
    }


}
