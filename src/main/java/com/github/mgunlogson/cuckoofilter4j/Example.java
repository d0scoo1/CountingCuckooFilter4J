package com.github.mgunlogson.cuckoofilter4j;

import com.github.mgunlogson.cuckoofilter4j.CountingCuckooFilter;
import com.github.mgunlogson.cuckoofilter4j.FilterTable;
import com.google.common.hash.Funnels;

public class Example {

    public static void main(String[] args) {
        CountingCuckooFilter<Integer> filter = new CountingCuckooFilter.Builder<>(Funnels.integerFunnel(), 5000000).build();
        for (int i = 0; i < 260; i++) {
            System.out.println(filter.put(1));
        }

    }

    private static void benchmark(){
        CountingCuckooFilter<Integer> filter = new CountingCuckooFilter.Builder<>(Funnels.integerFunnel(), 5000000).build();
        long st = System.currentTimeMillis();
        for (int i = 0; i < 5000000; i++) {
            boolean t =  filter.put(i);
        }
        long et = System.currentTimeMillis();
        System.out.println((et-st));

        long st1 = System.currentTimeMillis();
        for (int i = 0; i < 5000000; i++) {
            boolean t =  filter.delete(i);
        }
        long et1 = System.currentTimeMillis();
        System.out.println((et1-st1));

         st1 = System.currentTimeMillis();
        for (int i = 0; i < 5000000; i++) {
            boolean t =  filter.mightContain(i);
        }
         et1 = System.currentTimeMillis();
        System.out.println((et1-st1));
    }
    private static void deleteTest(){
        CountingCuckooFilter<Integer> filter = new CountingCuckooFilter.Builder<>(Funnels.integerFunnel(), 12).build();
        for (int i = 1; i < 7; i++) {
            for (int j = 0; j < 4; j++) {
                filter.put(i);
            }
        }
        filter.printMemBlock();
        System.out.println(filter.delete(1));
        System.out.println(filter.delete(1));
        filter.printMemBlock();
        System.out.println(filter.delete(111));
        System.out.println(filter.mightContain(1));
    }

    private static void vicTest(){
        CountingCuckooFilter<Integer> filter = new CountingCuckooFilter.Builder<>(Funnels.integerFunnel(), 12).build();
        for (int i = 1; i < 7; i++) {
            for (int j = 0; j < 4; j++) {
                filter.put(i);
            }
        }
        filter.printMemBlock();
        filter.put(7);
        filter.put(7);
        filter.printMemBlock("7");
        filter.put(8);
        filter.put(8);
        filter.printMemBlock("8");
        filter.put(8);
        filter.put(8);
        filter.printMemBlock("8");

        System.out.println(filter.victims.size());
        if(filter.victims.size() > 0){
            long[] vs = filter.victims.get(0);
            System.out.println(Long.toBinaryString(vs[2])+","+Long.toBinaryString(vs[3]));
        }

    }




    private static void inserTest(){
       // CountingCuckooFilter<Integer> filter = new CountingCuckooFilter.Builder<>(Funnels.integerFunnel(), 12).build();
        FilterTable ft = FilterTable.create(8, 2);
        ft.clearTagAndSet(0,0,1);
        ft.clearTagAndSet(0,1,0);
        ft.clearTagAndSet(0,2,1);
        ft.clearTagAndSet(0,3,3);
        ft.clearTagAndSet(0,4,0);
        ft.clearTagAndSet(0,5,5);
        ft.printMemBlock();
        ft.swapRandomInBucket(0,7);

        ft.printMemBlock();
    }


}
