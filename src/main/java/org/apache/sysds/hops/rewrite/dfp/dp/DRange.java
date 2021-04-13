package org.apache.sysds.hops.rewrite.dfp.dp;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Objects;

public class DRange {

    ArrayList<Integer> index;
    Pair<Integer, Integer> range;
    Boolean cseRangeTransposeType = null;

    public DRange revverse() {
        ArrayList<Integer> arr = new ArrayList<>();
        arr.add(index.get(0));
        arr.add(index.get(0) + index.get(2) - index.get(1) + 1);
        arr.add(index.get(2));
        DRange ans = new DRange(arr);
        return ans;
    }

    public DRange(ArrayList<Integer> index) {
        this.index = index;
        int begin = index.get(0);
        int end = index.get(index.size() - 1);
        range = Pair.of(begin, end);
    }

    Pair<Integer, Integer> getRange() {
        return this.range;
    }

    int getLeft() {
        return this.range.getLeft();
    }

    int getRight() {
        return this.range.getRight();
    }

    Pair<Integer, Integer> getLeftRange() {
        if (index.size() != 3) {
            return null;
        }
        return Pair.of(index.get(0), index.get(1) - 1);
    }

    Pair<Integer, Integer> getRighttRange() {
        if (index.size() != 3) {
            return null;
        }
        return Pair.of(index.get(1), index.get(2));
    }

    @Override
    public String toString() {
        return "DRange{" + index + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DRange dRange = (DRange) o;
        return Objects.equals(index, dRange.index) && Objects.equals(range, dRange.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, range);
    }
}
