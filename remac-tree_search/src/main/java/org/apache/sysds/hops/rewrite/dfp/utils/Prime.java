package org.apache.sysds.hops.rewrite.dfp.utils;

public class Prime {

    private static boolean isPrime(long x) {
        for (long i = 2; i * i <= x; i++) {
            if (x % i == 0) return false;
        }
        return true;
    }

    private static long [] primes =  {2,3,5,7,11,13,17,19,23,29};

    public static long getPrime(int i) {
        if (i<primes.length) {
            return primes[i];
        }
        int cnt = primes.length;
        long x = primes[primes.length-1];
        while (cnt <= i) {
            x++;
            if (isPrime(x)) cnt++;
        }
        return x;
    }

    public static void main(String[] args) {
        for (int i = 0; i <= 20; i++) {
            System.out.println(i + " " + getPrime(i));
        }
    }


}
