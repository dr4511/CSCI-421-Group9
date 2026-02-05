package StorageManager;

public class Buffer {
    // Configuration
    // Page size in bytes (fixed at DB creation time).
    private int pageSizeBytes;
    // Maximum number of pages the buffer may hold.
    private int capacityPages;

    // State
    // Count of pages currently pinned (non-evictable).
    private int pinnedCount;
    // True once initialize() has successfully completed.
    private boolean initialized;

    // Metadata structures (stubs for Phase 1 / later phases)
    // Maps pageId -> frame metadata (stub placeholder).
    private Object pageTable;
    // Array/list of frames and their contents (stub placeholder).
    private Object frameTable;
    // Storage manager used for page IO (stub placeholder).
    private Object storageManager;

    /**
     * Default constructor for phased initialization.
     */
    public Buffer() {
        // Defer configuration until setters/initialize are called.
    }

    /**
     * Construct a buffer with fixed configuration.
     */
    public Buffer(int pageSizeBytes, int capacityPages) {
        // Store configuration; actual allocation happens in initialize().
    }

    /**
     * @return maximum number of pages the buffer can hold.
     */
    public int getCapacityPages() {
        // Getter for configured capacity in pages.
        return this.capacityPages;
    }

    /**
     * @return page size in bytes.
     */
    public int getPageSizeBytes() {
        // Getter for configured page size.
        return this.pageSizeBytes;
    }

    /**
     * Inject the storage manager dependency used for page IO.
     */
    public void setStorageManager(Object storageManager) {
        // Store reference to storage manager for page reads/writes.
    }

    /**
     * Initialize internal data structures and mark the buffer ready.
     */
    public void initialize() {
        // Validate configuration, allocate tables/frames, set initialized=true.
    }

    /**
     * Shutdown the buffer, flushing dirty pages and releasing resources.
     */
    public void shutdown() {
        // Flush dirty pages via storage manager, then clear structures.
    }

    /**
     * @return current number of pinned pages.
     */
    public int pinnedCount() {
        // Count or return tracked pinned pages.
        return 0;
    }

    /**
     * Fetch a page into the buffer or return an existing resident page.
     */
    public Object getPage(int pageId) {
        // If resident: update LRU and return page object.
        // If not: request from storage manager, evict if needed, then return.
        return null;
    }

    /**
     * Create a new page and insert it into the buffer.
     */
    public Object createPage() {
        // Request new page id from storage manager and place into a frame.
        return null;
    }

    /**
     * Pin a page to prevent eviction.
     */
    public void pinPage(int pageId) {
        // Increase pin count for the page/frame.
    }

    /**
     * Unpin a page to allow eviction.
     */
    public void unpinPage(int pageId) {
        // Decrease pin count; allow eviction when count hits zero.
    }

    /**
     * Mark a page as dirty so it will be written on eviction/shutdown.
     */
    public void markDirty(int pageId) {
        // Set dirty flag on the page/frame metadata.
    }

    /**
     * Flush a single page if dirty.
     */
    public void flushPage(int pageId) {
        // If dirty: write to storage manager and clear dirty flag.
    }

    /**
     * Flush all dirty pages in the buffer.
     */
    public void flushAll() {
        // Iterate over frames and flush any dirty pages.
    }

    /**
     * Evict a page using the replacement policy when space is needed.
     */
    public void evict() {
        // Choose LRU victim that is not pinned; flush if dirty; remove from tables.
    }

    /**
     * Replace the current replacement policy implementation.
     */
    public void replacePage(Object replacementPolicy) {
        // Swap out the policy object used for LRU decisions.
    }
}
