package org.roaringbitmap.buffer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.Test;

import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class TestParallelAggregation {

    @Test
    public void testOr() {
        BufferParallelAggregation parallelAggregation = new BufferParallelAggregation(
                Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        e.printStackTrace();
                    }
                }).build()));

        int[] array1 = {1232, 3324, 123, 43243, 1322, 7897, 8767};
        int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
        int[] array3 =
                {1232, 3324, 123, 43243, 1322, 7897, 8767, 39173, 39174, 39175, 39176, 39177, 39178, 39179};
        int[] array4 = {};
        MutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
        MutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
        MutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
        MutableRoaringBitmap data4 = MutableRoaringBitmap.bitmapOf(array4);
        assertEquals(data3, parallelAggregation.or(data1, data2));
        assertEquals(data1, parallelAggregation.or(data1));
        assertEquals(data1, parallelAggregation.or(data1, data4));
    }
}
