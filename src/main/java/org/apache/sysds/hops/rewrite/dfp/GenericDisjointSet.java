package org.apache.sysds.hops.rewrite.dfp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class GenericDisjointSet<E> {
    private HashMap<E, E> parent = new HashMap<>();
    private HashMap<E, HashSet<E>> elements = new HashMap<>();

    public boolean exist(E e) {
        return elements.containsKey(e);
    }

    private void initElement(E e) {
        if (!exist(e)) {
            HashSet<E> set = new HashSet<>();
            set.add(e);
            elements.put(e, set);
            parent.put(e, e);
        }
    }

    public E find(E e) {
        initElement(e);
        E e2 = parent.get(e);
        if (e2.equals(e)) {
            return e2;
        } else {
            E e3 = find(e2);
            parent.put(e,e3);
            return e3;
        }
    }

    public Set<E> keys() {
        return parent.keySet();
    }

    public HashSet<E> elements(E x) {
        initElement(x);
        return elements.get(find(x));
    }

    public boolean merge(E x, E y) {
        E fx = find(x);
        E fy = find(y);
        if (!fx.equals(fy)) {
            parent.put(fy, fx);
            HashSet<E> set = elements.get(fy);
            elements.get(fx).addAll(set);
            set.clear();
            return true;
        } else {
            return false;
        }
    }

    public boolean isSame(E x, E y) {
        E fx = find(x);
        E fy = find(y);
        return fx.equals(fy);
    }

}
