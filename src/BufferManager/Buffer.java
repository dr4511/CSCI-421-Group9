package BufferManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import StorageManager.Page;
import StorageManager.StorageManager;

public class Buffer {
    // Configuration from command-line arguments.
    private int pageSizeBytes;
    private int bufferSizeBytes;
    private int capacityPages;

    // State
    private int pinnedCount;

    // Fixed-size frame table; length == capacityPages.
    private Page[] pages;

    // Metadata keyed by pageId.
    private Map<Integer, Integer> pageToFrame;
    private Map<Integer, Integer> pinCounts;
    private Map<Integer, Long> lastAccessTick;

    // Monotonic counter used for simple tick-based LRU.
    private long accessTick;

    // Placeholder dependency
    private StorageManager storageManager;

    /**
     * Build buffer from command line values.
     * capacityPages = bufferSizeBytes / pageSizeBytes
     */
    public Buffer(int pageSizeBytes, int bufferSizeBytes, StorageManager storageManager) {
        if (pageSizeBytes <= 0) {
            throw new IllegalStateException("pageSizeBytes must be > 0.");
        }
        if (bufferSizeBytes <= 0) {
            throw new IllegalStateException("bufferSizeBytes must be > 0.");
        }

        this.pageSizeBytes = pageSizeBytes;
        this.bufferSizeBytes = bufferSizeBytes;
        this.capacityPages = this.bufferSizeBytes / this.pageSizeBytes;

        if (this.capacityPages <= 0) {
            throw new IllegalStateException(
                "bufferSizeBytes must be >= pageSizeBytes so at least one page can fit in the buffer."
            );
        }

        this.pages = new Page[this.capacityPages];
        this.pageToFrame = new HashMap<>(this.capacityPages);
        this.pinCounts = new HashMap<>(this.capacityPages);
        this.lastAccessTick = new HashMap<>(this.capacityPages);
        this.accessTick = 0L;
        this.pinnedCount = 0;
        this.storageManager = storageManager;
    }

    public int getCapacityPages() {
        return this.capacityPages;
    }

    public int getBufferSizeBytes() {
        return this.bufferSizeBytes;
    }

    public int getPageSizeBytes() {
        return this.pageSizeBytes;
    }

    public void shutdown() {
        if (this.pages == null) {
            return;
        }

        flushAll();

        this.pages = null;
        this.pageToFrame.clear();
        this.pinCounts.clear();
        this.lastAccessTick.clear();
        this.pinnedCount = 0;
    }

    public int pinnedCount() {
        return this.pinnedCount;
    }

    /**
     * This does not auto-pin. Caller must explicitly pin/unpin.
     */
    public Page getPage(int pageId) {
        Integer frameIndex = this.pageToFrame.get(pageId);
        if (frameIndex != null) {
            touch(pageId);
            return this.pages[frameIndex];
        }

        if (this.pageToFrame.size() >= this.capacityPages) {
            evict();
        }

        Page loaded = this.storageManager.readPage(pageId, this.pageSizeBytes);
        
        if (loaded == null) {
            throw new IllegalStateException("Storage returned null for pageId=" + pageId);
        }

        int freeFrameIndex = findFreeFrameIndex();
        if (freeFrameIndex < 0) {
            throw new IllegalStateException("No free frame slot available after eviction.");
        }

        this.pages[freeFrameIndex] = loaded;
        this.pageToFrame.put(pageId, freeFrameIndex);
        this.pinCounts.put(pageId, 0);
        
        touch(pageId);

        return loaded;
    }

    public void pinPage(int pageId) {
        ensureInBuffer(pageId);

        int currentPins = this.pinCounts.get(pageId);
        this.pinCounts.put(pageId, currentPins + 1);
        this.pinnedCount++;
        touch(pageId);
    }

    public void unpinPage(int pageId) {
        ensureInBuffer(pageId);

        int currentPins = this.pinCounts.get(pageId);
        if (currentPins == 0) {
            throw new IllegalStateException("Page " + pageId + " is not pinned.");
        }

        this.pinCounts.put(pageId, currentPins - 1);
        this.pinnedCount--;
        touch(pageId);
    }

    public void markDirty(int pageId) {
        ensureInBuffer(pageId);

        pages[pageToFrame.get(pageId)].markDirty();

        touch(pageId);
    }

    public void flushPage(int pageId) {
        ensureInBuffer(pageId);

        if (!pages[pageToFrame.get(pageId)].isDirty()) {
            return;
        }

        int frameIndex = this.pageToFrame.get(pageId);
        byte[] serializedPage = Pages.serializePage(this.pages[frameIndex]);
        this.storageManager.writePage(pageId, serializedPage);
        pages[pageToFrame.get(pageId)].cleanDirty();
    }

    public void flushAll() {
        List<Integer> pageIds = new ArrayList<>(this.pageToFrame.keySet());
        for (int pageId : pageIds) {
            flushPage(pageId);
        }
    }

    public void evict() {
        Integer evictPageId = null;
        long oldestTick = Long.MAX_VALUE;

        for (int pageId : this.pageToFrame.keySet()) {
            if (this.pinCounts.get(pageId) != 0) {
                continue;
            }

            long pageTick = this.lastAccessTick.getOrDefault(pageId, Long.MIN_VALUE);
            if (evictPageId == null || pageTick < oldestTick) {
                evictPageId = pageId;
                oldestTick = pageTick;
            }
        }

        if (evictPageId == null) {
            throw new IllegalStateException(
                "Buffer is full and all pages are pinned. Try again once current DB operations finish."
            );
        }

        flushPage(evictPageId);
        removeFromBuffer(evictPageId);
    }

    private void ensureInBuffer(int pageId) {
        if (!this.pageToFrame.containsKey(pageId)) {
            getPage(pageId);
        }
    }

    private int findFreeFrameIndex() {
        for (int i = 0; i < this.pages.length; i++) {
            if (this.pages[i] == null) {
                return i;
            }
        }
        return -1;
    }

    private void removeFromBuffer(int pageId) {
        Integer frameIndex = this.pageToFrame.remove(pageId);
        if (frameIndex == null) {
            return;
        }

        this.pages[frameIndex] = null;
        this.pinCounts.remove(pageId);
        this.lastAccessTick.remove(pageId);
    }

    private void touch(int pageId) {
        this.accessTick++;
        this.lastAccessTick.put(pageId, this.accessTick);
    }
}
