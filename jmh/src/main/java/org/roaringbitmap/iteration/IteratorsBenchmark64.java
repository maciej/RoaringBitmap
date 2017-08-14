package org.roaringbitmap.iteration;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.LongIteratorFlyweight;
import org.roaringbitmap.longlong.ReverseLongIteratorFlyweight;
import org.roaringbitmap.longlong.RoaringTreeMap;
/**
 * Created by Borislav Ivanov on 4/2/15.
 */
@BenchmarkMode({Mode.SampleTime, Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class IteratorsBenchmark64 {

   @Benchmark
   public long testBoxed_a(BenchmarkState benchmarkState) {
      Iterator<Long> intIterator = benchmarkState.bitmap_a.iterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;
   }

   @Benchmark
   public long testStandard_a(BenchmarkState benchmarkState) {

      LongIterator intIterator = benchmarkState.bitmap_a.getLongIterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testFlyweight_a(BenchmarkState benchmarkState) {

      LongIteratorFlyweight intIterator = benchmarkState.flyweightIterator;

      intIterator.wrap(benchmarkState.bitmap_a);

      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testBoxed_b(BenchmarkState benchmarkState) {
      Iterator<Long> intIterator = benchmarkState.bitmap_b.iterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;
   }


   @Benchmark
   public long testStandard_b(BenchmarkState benchmarkState) {

      LongIterator intIterator = benchmarkState.bitmap_b.getLongIterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testFlyweight_b(BenchmarkState benchmarkState) {

      LongIteratorFlyweight intIterator = benchmarkState.flyweightIterator;

      intIterator.wrap(benchmarkState.bitmap_b);

      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testBoxed_c(BenchmarkState benchmarkState) {
      Iterator<Long> intIterator = benchmarkState.bitmap_c.iterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;
   }


   @Benchmark
   public long testStandard_c(BenchmarkState benchmarkState) {

      LongIterator intIterator = benchmarkState.bitmap_c.getLongIterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testFlyweight_c(BenchmarkState benchmarkState) {

      LongIteratorFlyweight intIterator = benchmarkState.flyweightIterator;

      intIterator.wrap(benchmarkState.bitmap_c);

      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testReverseStandard_a(BenchmarkState benchmarkState) {

      LongIterator intIterator = benchmarkState.bitmap_a.getReverseLongIterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testReverseFlyweight_a(BenchmarkState benchmarkState) {

      ReverseLongIteratorFlyweight intIterator = benchmarkState.flyweightReverseIterator;

      intIterator.wrap(benchmarkState.bitmap_a);

      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testReverseStandard_b(BenchmarkState benchmarkState) {

      LongIterator intIterator = benchmarkState.bitmap_b.getReverseLongIterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testReverseFlyweight_b(BenchmarkState benchmarkState) {

      ReverseLongIteratorFlyweight intIterator = benchmarkState.flyweightReverseIterator;

      intIterator.wrap(benchmarkState.bitmap_b);

      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testReverseStandard_c(BenchmarkState benchmarkState) {

      LongIterator intIterator = benchmarkState.bitmap_c.getReverseLongIterator();
      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   @Benchmark
   public long testReverseFlyweight_c(BenchmarkState benchmarkState) {

      ReverseLongIteratorFlyweight intIterator = benchmarkState.flyweightReverseIterator;

      intIterator.wrap(benchmarkState.bitmap_c);

      long result = 0;
      while (intIterator.hasNext()) {
         result = intIterator.next();

      }
      return result;

   }

   
   @State(Scope.Benchmark)
   public static class BenchmarkState {

      final RoaringTreeMap bitmap_a;

      final RoaringTreeMap bitmap_b;

      final RoaringTreeMap bitmap_c;

      final LongIteratorFlyweight flyweightIterator = new LongIteratorFlyweight();

      final ReverseLongIteratorFlyweight flyweightReverseIterator = new ReverseLongIteratorFlyweight();

      public BenchmarkState() {

         final long[] data = takeSortedAndDistinct(new Random(0xcb000a2b9b5bdfb6l), 100000);
         bitmap_a = RoaringTreeMap.bitmapOf(data);

         bitmap_b = new RoaringTreeMap();
         for (int k = 0; k < (1 << 30); k += 32)
            bitmap_b.add(k);
         
         bitmap_c = new RoaringTreeMap();
         for (int k = 0; k < (1 << 30); k += 3)
            bitmap_c.add(k);
         
      }

      private long[] takeSortedAndDistinct(Random source, int count) {

         LinkedHashSet<Long> longs = new LinkedHashSet<>(count);

         for (int size = 0; size < count; size++) {
            long next;
            do {
               next = Math.abs(source.nextLong());
            } while (!longs.add(next));
         }

         long[] unboxed = toArray(longs);
         Arrays.sort(unboxed);
         return unboxed;
      }

      private long[] toArray(LinkedHashSet<? extends Number> boxedLongs) {
    	  	long[] longs = new long[boxedLongs.size()];
         int i = 0;
         for (Number n : boxedLongs) {
            longs[i++] = n.longValue();
         }
         return longs;
      }
   }

}
