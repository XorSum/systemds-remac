package org.apache.sysds.hops.rewrite.dfp.coordinate;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Objects;

import static java.lang.Integer.min;
import static java.lang.Integer.reverse;
import static org.apache.commons.lang3.ObjectUtils.max;

public class Range {
    public int left;
    public int right;
    public boolean transpose;

    public static Range of(int l, int r, boolean t) {
        Range range = new Range();
        range.left = l;
        range.right = r;
        range.transpose = t;
        return range;
    }

    public boolean intersect(Range other) {
        // 相交
        return max(this.left, other.left) <= min(this.right, other.right);
    }

    public boolean contain(Range other) {
        // 包含
        return this.left <= other.left && other.right <= this.right;
    }

    public boolean conflict(Range other) {
        // 两者相交但是并不包含
        return this.intersect(other) && !this.contain(other) && !other.contain(this);
    }

    @Override
    public String toString() {
        return "(" + left + "," + right + "," + transpose + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range range = (Range) o;
        return left == range.left && right == range.right && transpose == range.transpose;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, transpose);
    }
}