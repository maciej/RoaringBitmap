package org.roaringbitmap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestUtil {

    @Test
    public void testUtilUnsignedIntersection() {
        short data1[] = {-19, -17, -15, -13, -11, -9, -7, -5, -3, -1};
        short data2[] = {-18, -16, -14, -12, -10, -8, -1};
        Assert.assertTrue(Util.unsignedIntersects(data1, data1.length, data2, data2.length));
        short data3[] = {-19, -17, -15, -13, -11, -9, -7};
        short data4[] = {-18, -16, -14, -12, -10, -8, -6, -4, -2, 0};
        Assert.assertFalse(Util.unsignedIntersects(data3, data3.length, data4, data4.length));
        short data5[] = {};
        short data6[] = {};
        Assert.assertFalse(Util.unsignedIntersects(data5, data5.length, data6, data6.length));
    }

    @Test
    public void testBranchyUnsignedBinarySearch() {
        short data1[] = {-19, -17, -15, -13, -11, -9, -7, -5, -3};
        Assert.assertEquals(8, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, data1[8]));
        Assert.assertEquals(0, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, data1[0]));
        Assert.assertEquals(data1.length-1, Util.branchyUnsignedBinarySearch(data1, data1.length-1, data1.length, data1[data1.length-1]));
        Assert.assertEquals(-1, Util.branchyUnsignedBinarySearch(data1, 0, 0, (short)0));
        Assert.assertEquals(-10, Util.branchyUnsignedBinarySearch(data1, 0, data1.length, (short) -1));
    }

    @Test
    public void testCompare() {
        Assert.assertTrue(Util.compareUnsigned((short)1,(short)2) < 0);
        Assert.assertTrue(Util.compareUnsigned((short)-32333,(short)2) > 0);
        Assert.assertTrue(Util.compareUnsigned((short)2, (short)-32333) < 0);
        Assert.assertTrue(Util.compareUnsigned((short)0,(short)0) ==0);

    }

    @Test
    public void testPartialRadixSortEmpty() {
        int[] data = new int[] {};
        int[] test = Arrays.copyOf(data, data.length);
        Util.partialRadixSort(test);
        Assert.assertArrayEquals(data, test);
    }

    @Test
    public void testPartialRadixSortIsStableInSameKey() {
        int[] data = new int[] {25, 1, 0, 10};
        int[] test = Arrays.copyOf(data, data.length);
        Util.partialRadixSort(test);
        Assert.assertArrayEquals(data, test);
    }

    @Test
    public void testPartialRadixSortSortsKeysCorrectly() {
        int key1 = 1 << 16;
        int key2 = 1 << 17;
        int[] data = new int[] {key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10};
        // sort by keys, leave values stable
        int[] expected = new int[] {0, 25, 10, key1 | 1, key1 | 10, key1, key2 | 25, key2 | 10};
        int[] test = Arrays.copyOf(data, data.length);
        Util.partialRadixSort(test);
        Assert.assertArrayEquals(expected, test);
    }

    @Test
    public void testPartialRadixSortSortsKeysCorrectlyWithDuplicates() {
        int key1 = 1 << 16;
        int key2 = 1 << 17;
        int[] data = new int[] {key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10,
                                key2 | 25, key1 | 1, 0, key2 | 10, 25, key1 | 10, key1, 10};
        // sort by keys, leave values stable
        int[] expected = new int[] {0, 25, 10, 0, 25, 10, key1 | 1, key1 | 10,  key1, key1 | 1, key1 | 10,  key1,
                                    key2 | 25, key2 | 10, key2 | 25, key2 | 10};
        int[] test = Arrays.copyOf(data, data.length);
        Util.partialRadixSort(test);
        Assert.assertArrayEquals(expected, test);
    }
}
