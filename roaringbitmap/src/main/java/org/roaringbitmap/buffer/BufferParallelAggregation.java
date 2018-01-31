package org.roaringbitmap.buffer;

import org.roaringbitmap.Util;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;

@SuppressWarnings("Duplicates")
public class BufferParallelAggregation {

    private static final int DEFAULT_BATCH_SIZE = 8;

    final ExecutorService executorService;
    private final int batchSize;

    @SuppressWarnings("WeakerAccess")
    public BufferParallelAggregation(ExecutorService executorService) {
        this(executorService, DEFAULT_BATCH_SIZE);
    }

    @SuppressWarnings("WeakerAccess")
    public BufferParallelAggregation(ExecutorService executorService, int batchSize) {
        this.executorService = executorService;
        this.batchSize = batchSize;
    }

    public MutableRoaringBitmap or(ImmutableRoaringBitmap... bitmaps) {
        MutableRoaringBitmap b = new MutableRoaringBitmap();
        if (bitmaps.length == 0) {
            return b;
        } else if (bitmaps.length == 1) {
            return bitmaps[0].toMutableRoaringBitmap();
        }

        PriorityQueue<MappeableContainerPointer> heap = buildContainerKeyHeap(bitmaps);

        final ArrayList<SingleContainer> resultList = new ArrayList<>();

        final Phaser phaser = new Phaser();
        phaser.register();

        for (MultipleContainers[] mcb = nextContainersBatch(heap); mcb[0] != null; mcb = nextContainersBatch(heap)) {
            final MultipleContainers[] mcbf = mcb;

            phaser.register();
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    SingleContainer[] aggregated = new SingleContainer[batchSize];
                    for (int i = 0; i < mcbf.length; i++) {
                        if (mcbf[i] == null)
                            break;
                        aggregated[i] = unionOf(mcbf[i]);
                    }

                    synchronized (resultList) {
                        for (SingleContainer singleContainer : aggregated) {
                            if (singleContainer == null)
                                break;
                            resultList.add(singleContainer);
                        }
                    }
                    phaser.arrive();
                }
            });
        }

        // wait
        phaser.arriveAndAwaitAdvance();

        Collections.sort(resultList);

        for (SingleContainer singleContainer : resultList) {
            b.getMappeableRoaringArray().append(singleContainer.key, singleContainer.container);
        }

        return b;
    }

    public MutableRoaringBitmap and(ImmutableRoaringBitmap... bitmaps) {
        final int bitmapCount = bitmaps.length;
        MutableRoaringBitmap b = new MutableRoaringBitmap();
        if (bitmaps.length == 0) {
            return b;
        } else if (bitmaps.length == 1) {
            return bitmaps[0].toMutableRoaringBitmap();
        }

        PriorityQueue<MappeableContainerPointer> heap = buildContainerKeyHeap(bitmaps);

        final ArrayList<SingleContainer> resultList = new ArrayList<>();

        final Phaser phaser = new Phaser();
        phaser.register();

        for (MultipleContainers[] mcb = nextContainersBatch(heap); mcb[0] != null; mcb = nextContainersBatch(heap)) {
            final MultipleContainers[] mcbf = mcb;

            phaser.register();
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    SingleContainer[] aggregated = new SingleContainer[batchSize];
                    for (int i = 0; i < mcbf.length; i++) {
                        if (mcbf[i] != null && mcbf[i].containers.size() >= bitmapCount) {
                            aggregated[i] = intersectionOf(mcbf[i]);
                        }
                    }

                    synchronized (resultList) {
                        for (SingleContainer singleContainer : aggregated) {
                            if (singleContainer != null) resultList.add(singleContainer);
                        }
                    }
                    phaser.arrive();
                }
            });
        }

        // wait
        phaser.arriveAndAwaitAdvance();

        Collections.sort(resultList);

        for (SingleContainer singleContainer : resultList) {
            b.getMappeableRoaringArray().append(singleContainer.key, singleContainer.container);
        }

        return b;
    }

    private static PriorityQueue<MappeableContainerPointer> buildContainerKeyHeap(ImmutableRoaringBitmap... bitmaps) {
        PriorityQueue<MappeableContainerPointer> heap = new PriorityQueue<>(bitmaps.length, new Comparator<MappeableContainerPointer>() {
            @Override
            public int compare(MappeableContainerPointer o1, MappeableContainerPointer o2) {
                return Util.compareUnsigned(o1.key(), o2.key());
            }
        });

        for (ImmutableRoaringBitmap bitmap : bitmaps) {
            MappeableContainerPointer cp = bitmap.getContainerPointer();
            if (cp.hasContainer())
                heap.add(cp);
        }

        return heap;
    }

    private static MultipleContainers nextContainers(PriorityQueue<MappeableContainerPointer> containerHeap) {
        MappeableContainerPointer initial = containerHeap.poll();
        if (initial == null) {
            return null;
        }
        short initialKey = initial.key();

        List<MappeableContainer> containers = new ArrayList<>();
        containers.add(initial.getContainer());

        initial.advance();
        if (initial.hasContainer())
            containerHeap.add(initial);

        while (true) {
            MappeableContainerPointer next = containerHeap.peek();
            if (next == null || next.key() != initialKey) {
                break;
            }

            containerHeap.poll();

            containers.add(next.getContainer());

            next.advance();
            if (next.hasContainer())
                containerHeap.add(next);
        }

        return new MultipleContainers(containers, initialKey, -1);
    }

    private MultipleContainers[] nextContainersBatch(PriorityQueue<MappeableContainerPointer> containerHeap) {
        MultipleContainers[] containers = new MultipleContainers[batchSize];
        for (int i = 0; i < batchSize; i++) {
            MultipleContainers mc = nextContainers(containerHeap);
            if (mc == null) {
                break;
            }

            containers[i] = mc;
        }
        return containers;
    }

    private static SingleContainer unionOf(MultipleContainers mc) {
        if (mc.containers.size() == 1) {
            return new SingleContainer(mc.key, mc.containers.get(0), mc.idx);
        }

        MappeableContainer r = mc.containers.get(0).toBitmapContainer().lazyOR(mc.containers.get(1));
        for (int i = 2; i < mc.containers.size(); i++) {
            r = r.lazyIOR(mc.containers.get(i));
        }
        r = r.repairAfterLazy();

        return new SingleContainer(mc.key, r, mc.idx);
    }

    private static SingleContainer intersectionOf(MultipleContainers mc) {
        if (mc.containers.size() == 1) {
            return new SingleContainer(mc.key, mc.containers.get(0), mc.idx);
        }

        MappeableContainer r = mc.containers.get(0).and(mc.containers.get(1));
        if (r.getCardinality() == 0)
            return null;

        for (int i = 2; i < mc.containers.size(); i++) {
            r = r.iand(mc.containers.get(i));

            if (r.getCardinality() == 0)
                return null;
        }

        return new SingleContainer(mc.key, r, mc.idx);
    }

    private static class SingleContainer implements Comparable<SingleContainer> {

        SingleContainer(short key, MappeableContainer container, int idx) {
            this.key = key;
            this.container = container;
            this.idx = idx;
        }

        short key;
        MappeableContainer container;
        int idx;

        @Override
        public int compareTo(SingleContainer o) {
            return Short.compare(key, o.key);
        }

    }

    private static class MultipleContainers {
        List<MappeableContainer> containers;
        short key;
        int idx;

        MultipleContainers(List<MappeableContainer> containers, short key, int idx) {
            this.containers = containers;
            this.key = key;
            this.idx = idx;
        }
    }

}
