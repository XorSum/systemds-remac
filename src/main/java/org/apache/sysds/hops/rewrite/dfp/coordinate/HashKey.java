package org.apache.sysds.hops.rewrite.dfp.coordinate;

public class HashKey {
    public long left;
    public long right;

    public static HashKey of(Long l, Long r) {
        HashKey key = new HashKey();
        key.left = l;
        key.right = r;
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        HashKey o = (HashKey) obj;
        return left == o.left && right == o.right;
    }

    @Override
    public int hashCode() {
        return (int) (left * (1e9 + 7) + right);
    }

    @Override
    public String toString() {
        return "HashKey[" + left + "," + right + "]";
    }
}
