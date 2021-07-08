package org.apache.sysds.hops.rewrite.dfp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GenericDisjointSet<E> {
    public ConcurrentHashMap<E, E> parent = new ConcurrentHashMap<>();
    public ConcurrentHashMap<E, HashSet<E>> elements = new ConcurrentHashMap<>();

    public ReentrantReadWriteLock rwl;
    public Lock r;
    public Lock w;

    public GenericDisjointSet() {
        rwl = new ReentrantReadWriteLock();
        r = rwl.readLock();
        w = rwl.writeLock();
    }

    public boolean exist(E e) {
        r.lock();
        try {
            return elements.containsKey(e);
        } finally {
            r.unlock();
        }
    }

    public E find(E e) {
        r.lock();
        try {
            return parent.getOrDefault(e, e);
        } finally {
            r.unlock();
        }
    }

    public synchronized boolean merge(E x, E y) {
        // set x as the parent of y
        E fx = find(x);
        E fy = find(y);

        if (!fx.equals(fy)) {
            w.lock();
            fx = find(x);
            fy = find(y);
            if (!fx.equals(fy)) {

                elements.computeIfAbsent(fx, k -> {
                    HashSet<E> set1 = new HashSet<>();
                    set1.add(k);
                    return set1;
                });
                elements.computeIfAbsent(fy, k -> {
                    HashSet<E> set1 = new HashSet<>();
                    set1.add(k);
                    return set1;
                });
                HashSet<E> sety = elements.get(fy);
                HashSet<E> setx = elements.get(fx);
                setx.addAll(sety);
                sety.clear();

                for (E child : setx) {
                    parent.put(child, x);
                }
                parent.put(y,x);
            }

            w.unlock();
            return true;
        } else {
            return false;
        }

    }

}
