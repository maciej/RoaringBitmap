package org.roaringbitmap.buffer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.*;

import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class TestBufferParallelAggregation {

    private static BufferParallelAggregation aggregator;

    @BeforeClass
    public static void setup() {
        aggregator = new BufferParallelAggregation(
                Executors.newFixedThreadPool(4, new ThreadFactoryBuilder().setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        e.printStackTrace();
                    }
                }).build()));
    }

    @AfterClass
    public static void tearDown() {
        aggregator.executorService.shutdown();
        aggregator = null;
    }

    @Test
    public void testOr() {
        int[] array1 = {1232, 3324, 123, 43243, 1322, 7897, 8767};
        int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
        int[] array3 =
                {1232, 3324, 123, 43243, 1322, 7897, 8767, 39173, 39174, 39175, 39176, 39177, 39178, 39179};
        int[] array4 = {};
        MutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
        MutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
        MutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
        MutableRoaringBitmap data4 = MutableRoaringBitmap.bitmapOf(array4);
        assertEquals(data3, aggregator.or(data1, data2));
        assertEquals(data1, aggregator.or(data1));
        assertEquals(data1, aggregator.or(data1, data4));
    }

    @Test
    public void testAnd() {
        int[] array1 = {39173, 39174};
        int[] array2 = {39173, 39174, 39175, 39176, 39177, 39178, 39179};
        int[] array3 = {39173, 39174};
        int[] array4 = {};
        MutableRoaringBitmap data1 = MutableRoaringBitmap.bitmapOf(array1);
        MutableRoaringBitmap data2 = MutableRoaringBitmap.bitmapOf(array2);
        MutableRoaringBitmap data3 = MutableRoaringBitmap.bitmapOf(array3);
        MutableRoaringBitmap data4 = MutableRoaringBitmap.bitmapOf(array4);
        Assert.assertEquals(data3, aggregator.and(data1, data2));
        Assert.assertEquals(new MutableRoaringBitmap(), aggregator.and(data4));
    }

}
