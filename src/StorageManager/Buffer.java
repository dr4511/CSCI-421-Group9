package StorageManager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

import Catalog.Catalog;

public class Buffer {
    // In-memory page store: key is page id from Page object.
    private final HashMap<Integer, Page> pagesById;

    private final int pageSizeBytes;
    private final int capacityPages;
    private final Path dbFilePath;
    private final Catalog catalog;

    public Buffer(int pageSizeBytes, int bufferSizeBytes, String dbFilePath, Catalog catalog) {
        if (pageSizeBytes <= 0) {
            throw new IllegalArgumentException("pageSizeBytes must be > 0.");
        }
        if (bufferSizeBytes <= 0) {
            throw new IllegalArgumentException("bufferSizeBytes must be > 0.");
        }
        if (dbFilePath == null || dbFilePath.isBlank()) {
            throw new IllegalArgumentException("dbFilePath must be non-empty.");
        }

        int computedCapacity = bufferSizeBytes / pageSizeBytes;
        if (computedCapacity <= 0) {
            throw new IllegalArgumentException(
                "bufferSizeBytes must be >= pageSizeBytes so at least one page fits in buffer."
            );
        }

        this.pageSizeBytes = pageSizeBytes;
        this.capacityPages = computedCapacity;
        this.dbFilePath = Path.of(dbFilePath);
        this.pagesById = new HashMap<>(this.capacityPages);
        this.catalog = catalog;
    }

    /**
     * Returns the page from buffer if present; otherwise reads from hardware.
     */
    public Page getPage(int id) {
        Page inBuffer = this.pagesById.get(id);
        if (inBuffer != null) {
            onPageAccess(inBuffer);
            return inBuffer;
        }

        Page loadedPage = readPageFromHW(id);
        if (loadedPage == null) {
            throw new IllegalStateException("Storage returned null for page id " + id + ".");
        }

        addPageToBuffer(loadedPage);
        onPageAccess(loadedPage);
        return loadedPage;
    }

    /**
     * Creates a new page id (free list first), creates an empty page, and puts it in buffer.
     */
    public Page createNewPage() {
        Page newPage;
        int newPageId;

        Integer freePageId = catalog.getFreePageListHead();
        if (freePageId != -1) {
            Page freePage = readPageFromHW(freePageId);
            // update free pages (dont forget setting Catalog heaD)
            // handle in storage manager for free page tracking?
            newPage = freePage;
            newPageId = freePage.getPageID();
        } else {
            newPageId = appendNewPageToHW();
            newPage = new Page(newPageId, pageSizeBytes);
            // Update catalog to keep track of last used id
            catalog.setLastPageId(++newPageId);
        }
        
        updateTableLastPageLink();
        addPageToBuffer(newPage);
        onPageAccess(newPage);

        return newPage;
    }

    private void addPageToBuffer(Page page) {
        if (page == null) {
            throw new IllegalArgumentException("Cannot buffer a null page.");
        }

        evictPageIfNeeded();

        int pageId = page.getPageID();
        this.pagesById.put(pageId, page);
    }

    private void evictPageIfNeeded() {
        if (this.pagesById.size() < this.capacityPages) {
            return;
        }

        evictLeastRecentlyUsedPage();
    }

    private void evictLeastRecentlyUsedPage() {
        if (this.pagesById.isEmpty()) {
            return;
        }

        Page[] pageArray = this.pagesById.values().toArray(new Page[0]);
        Arrays.sort(pageArray, Comparator.comparingLong(this::getLastAccessTimestamp));

        Page evictPage = pageArray[0];
        int evictPageId = evictPage.getPageID();

        writePageToHW(evictPage);
        this.pagesById.remove(evictPageId);
    }

    /**
     * Uses ByteBuffer as the hardware IO boundary.
     */
    private Page readPageFromHW(int id) {
        ByteBuffer rawPageBytes = readPageBytesFromHW(id);
        return Page.deserializePage(rawPageBytes);
    }

    /**
     * Uses ByteBuffer as the hardware IO boundary.
     */
    private void writePageToHW(Page page) {
        int pageId = page.getPageID();

        ByteBuffer rawPageBytes = page.serializePage();
        
        writePageBytesToHW(pageId, rawPageBytes);
    }

    private void onPageAccess(Page page) {
        // TODO(team): update page's last-access timestamp inside Page object. delegate to Page.updateLastAccessTimestamp(...) once available.
    }

    private long getLastAccessTimestamp(Page page) {
        // TODO(team): return page.getLastAccessTimestamp();
        throw new UnsupportedOperationException("Page LRU timestamp accessor is not implemented yet.");
    }

    private int appendNewPageToHW() {
        try (RandomAccessFile raf = new RandomAccessFile(this.dbFilePath.toFile(), "rw");
             FileChannel channel = raf.getChannel()) {

            int newPageId = catalog.getLastPageId() + 1;
            long offset = pageOffset(newPageId);

            ByteBuffer emptyPage = ByteBuffer.allocate(this.pageSizeBytes);
            channel.position(offset);
            while (emptyPage.hasRemaining()) {
                channel.write(emptyPage);
            }

            return newPageId;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append new page to database file.", e);
        }
    }

    // TODO: Figure out how to implement
    private void updateTableLastPageLink(Page updatePage, Page newPage) {
        updatePage.setNextPage(newPage.getPageID());
    }

    private ByteBuffer readPageBytesFromHW(int pageId) {
        try (RandomAccessFile raf = new RandomAccessFile(this.dbFilePath.toFile(), "rw");
             FileChannel channel = raf.getChannel()) {

            long offset = pageOffset(pageId);
            long fileSize = channel.size();
            if (offset + this.pageSizeBytes > fileSize) {
                throw new IllegalStateException("Page " + pageId + " does not exist in DB file.");
            }

            ByteBuffer rawPageBytes = ByteBuffer.allocate(this.pageSizeBytes);
            channel.position(offset);

            while (rawPageBytes.hasRemaining()) {
                int read = channel.read(rawPageBytes);
                if (read == -1) {
                    break;
                }
            }

            if (rawPageBytes.position() != this.pageSizeBytes) {
                throw new IllegalStateException("Could not read a full page for page id " + pageId + ".");
            }

            rawPageBytes.flip();
            return rawPageBytes;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read page " + pageId + " from DB file.", e);
        }
    }

    private void writePageBytesToHW(int pageId, ByteBuffer rawPageBytes) {
        ByteBuffer normalizedPageBytes = normalizePageBytes(rawPageBytes);

        try (RandomAccessFile raf = new RandomAccessFile(this.dbFilePath.toFile(), "rw");
             FileChannel channel = raf.getChannel()) {

            long offset = pageOffset(pageId);
            channel.position(offset);
            while (normalizedPageBytes.hasRemaining()) {
                channel.write(normalizedPageBytes);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write page " + pageId + " to DB file.", e);
        }
    }

    private ByteBuffer normalizePageBytes(ByteBuffer rawPageBytes) {
        ByteBuffer source = rawPageBytes.duplicate();
        if (source.remaining() > this.pageSizeBytes) {
            throw new IllegalArgumentException("Serialized page exceeds configured page size.");
        }

        ByteBuffer normalized = ByteBuffer.allocate(this.pageSizeBytes);
        normalized.put(source);
        while (normalized.hasRemaining()) {
            normalized.put((byte) 0);
        }

        normalized.flip();
        return normalized;
    }

    private long pageOffset(int pageId) {
        return (long) pageId * this.pageSizeBytes;
    }
}
