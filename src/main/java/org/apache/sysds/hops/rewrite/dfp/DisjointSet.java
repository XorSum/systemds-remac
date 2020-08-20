package org.apache.sysds.hops.rewrite.dfp;

import java.util.ArrayList;

public class DisjointSet {
    private ArrayList<Integer> fa;
    private ArrayList<Integer> cnt;

    public DisjointSet(int size) {
        fa = new ArrayList<>();
        cnt = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            fa.add(i);
            cnt.add(1);
        }
    }

    public int find(int x) {
        if (fa.get(x) == x) {
            return x;
        } else {
            return find(fa.get(x));
        }
    }

    public int count(int x){
        return cnt.get(x);
    }

    public boolean merge(int x, int y) {
        int fx = find(x);
        int fy = find(y);
        if (fx != fy) {
            fa.set(fy, fx);
            cnt.set(fx,cnt.get(fx)+cnt.get(fy));
            return true;
        } else {
            return false;
        }
    }

    public boolean isSame(int x, int y) {
        int fx = find(x);
        int fy = find(y);
        return fx == fy;
    }

}
