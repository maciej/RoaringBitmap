package org.roaringbitmap;

import java.util.*;
import java.util.concurrent.ExecutorService;

public class ParallelAggregation {

    private final ExecutorService executorService;

    public ParallelAggregation(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public RoaringBitmap or(RoaringBitmap... bitmaps) {
        RoaringBitmap b = new RoaringBitmap();

        PriorityQueue<SingleContainer> heap = new PriorityQueue<>();
        for (RoaringBitmap bitmap : bitmaps) {
            RoaringArray hlc = bitmap.highLowContainer;
            for (int i = 0; i < hlc.size; i++) {
                heap.add(new SingleContainer(hlc.keys[i], hlc.values[i]));
            }
        }

        final List<SingleContainer> resultList = new ArrayList<>();

        for (MultipleContainers mc = nextContainers(heap); mc != null; mc = nextContainers(heap)) {
            final MultipleContainers mcf = mc;

            if (mcf.containers.size() == 0) {
                throw new IllegalStateException();
            }

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    SingleContainer singleContainer = unionOfMultipleContainers(mcf);

                    synchronized (resultList) {
                        resultList.add(singleContainer.idx, singleContainer);
                    }

                }
            });
        }

        for (SingleContainer singleContainer : resultList) {
            b.highLowContainer.append(singleContainer.key, singleContainer.container);
        }

        return b;
    }

    private static MultipleContainers nextContainers(PriorityQueue<SingleContainer> containerHeap) {
        SingleContainer initial = containerHeap.poll();
        if (initial == null) {
            return null;
        }

        List<Container> containers = new ArrayList<>();
        containers.add(initial.container);

        while (true) {
            SingleContainer next = containerHeap.peek();
            if (next == null || next.key != initial.key) {
                break;
            }
            containerHeap.poll();

            containers.add(next.container);
        }

        return new MultipleContainers(containers, initial.key, 0);
    }

    // assumes mc.containers.length >= 2
    private static SingleContainer unionOfMultipleContainers(MultipleContainers mc) {
        Container r = mc.containers.get(0).lazyOR(mc.containers.get(1));
        for (int i = 2; i < mc.containers.size(); i++) {
            r = r.lazyIOR(mc.containers.get(i));
        }
        r = r.repairAfterLazy();

        return new SingleContainer(mc.key, r, mc.idx);
    }

    private static class SingleContainer implements Comparable<SingleContainer> {

        SingleContainer(short key, Container container) {
            this.key = key;
            this.container = container;
        }

        SingleContainer(short key, Container container, int idx) {
            this.key = key;
            this.container = container;
            this.idx = idx;
        }

        short key;
        Container container;
        int idx;

        @Override
        public int compareTo(SingleContainer o) {
            return Short.compare(key, o.key);
        }

    }

    private static class MultipleContainers {
        List<Container> containers;
        short key;
        int idx;

        MultipleContainers(List<Container> containers, short key, int idx) {
            this.containers = containers;
            this.key = key;
            this.idx = idx;
        }
    }

}
