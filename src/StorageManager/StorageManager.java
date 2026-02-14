package StorageManager;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;

public class StorageManager {
    public enum AlterOperation {
        ADD,
        DROP
    }

    // Core dependencies/configuration.
    private final Buffer buffer;
    private final String dbFilePath;
    private final int pageSizeBytes;
    private final int bufferSizeBytes;
    private final Catalog catalog;

    public StorageManager(String dbFilePath, int pageSizeBytes, int bufferSizeBytes, Catalog catalog) {
        if (dbFilePath == null || dbFilePath.isBlank()) {
            throw new IllegalArgumentException("dbFilePath must be non-empty.");
        }
        if (pageSizeBytes <= 0) {
            throw new IllegalArgumentException("pageSizeBytes must be > 0.");
        }
        if (bufferSizeBytes <= 0) {
            throw new IllegalArgumentException("bufferSizeBytes must be > 0.");
        }
        if (catalog == null) {
            throw new IllegalArgumentException("catalog must be non-null.");
        }

        this.dbFilePath = dbFilePath;
        this.pageSizeBytes = pageSizeBytes;
        this.bufferSizeBytes = bufferSizeBytes;
        this.catalog = catalog;

        this.buffer = new Buffer(pageSizeBytes, bufferSizeBytes, dbFilePath, catalog);
    }

    public Buffer getBuffer() {
        return this.buffer;
    }

    public String getDbFilePath() {
        return this.dbFilePath;
    }

    public int getPageSizeBytes() {
        return this.pageSizeBytes;
    }

    public int getBufferSizeBytes() {
        return this.bufferSizeBytes;
    }

    /**
     * CREATE TABLE flow.
     */
    public boolean createTable(TableSchema table) {
        Page newPage = this.buffer.createNewPage();
        table.setHeadPageId(newPage.getPageID());
        return this.catalog.addTable(table);
    }

    /**
     * SELECT * FROM <table> flow.
     * Reads pages one by one via buffer and returns raw row bytes in traversal order.
     */
    public void selectAllTable(TableSchema table) {
        int currPageId = table.getHeadPageId();
        while (true) {
            Page page = this.buffer.getPage(currPageId);
            // print page
            int nextPageId = page.getNextPage();
            if (nextPageId == -1){
                break;
            }
            currPageId = nextPageId;
        }
    }

    /**
     * DROP TABLE flow.
     * Marks all table pages free and removes schema from catalog.
     */
    public void dropTable(TableSchema table) {
        int currPageId = table.getHeadPageId();
        while (currPageId != -1) {
            Page page = this.buffer.getPage(currPageId);

            int nextPageId = page.getNextPage();
            freePage(page);
            currPageId = nextPageId;
        }
    }

    /**
     * INSERT INTO <table> flow.
     */
    public boolean insertIntoTable(TableSchema table, byte[] recordBytes) {
        if (recordBytes == null) {
            throw new IllegalArgumentException("recordBytes must be non-null.");
        }

        // First row in table: allocate first page and set table head.
        if (table.getHeadPageId() == -1) {
            Page firstPage = this.buffer.createNewPage();
            table.setHeadPageId(firstPage.getPageID());
        }

        int[] lastTwoPagesId = findTailPagesId(table.getHeadPageId());
        int beforeTailPageId = lastTwoPagesId[0];
        int tailPageId = lastTwoPagesId[1];

        Page tailPage = this.buffer.getPage(tailPageId);

        if (recordBytes.length + 2 * Integer.BYTES > tailPage.getFreeSpace()){
            Page page1 = this.buffer.createNewPage();
            Page page2 = this.buffer.createNewPage();

            // set page1 nextPage to page2 id
            page1.setNextPage(page2.getPageID());

            tailPage.split(page1, page2);

            if (beforeTailPageId == -1){
                table.setHeadPageId(page1.getPageID());
            } else {
                updateTablePageLink(beforeTailPageId, page1.getPageID());
            }

            return true;
        }

        return tailPage.addRecord(recordBytes);
    }

    /**
     * TRICK IS TO JUST CREATE COMPLETELY NEW TABLES
     * COPY OVER ALL THE DATA TO THE NEW TABLES
     */
    public boolean alterTable(
        TableSchema table,
        AlterOperation operation,
        AttributeSchema addAttribute,
        String dropAttributeName
    ) {

        if (operation == AlterOperation.ADD) {
            if (addAttribute == null) {
                throw new IllegalArgumentException("addAttribute must be provided for ADD.");
            }

            boolean added = table.addAttribute(addAttribute);
            if (!added) {
                return false;
            }
        }
        else if (operation == AlterOperation.DROP) {
            if (dropAttributeName == null || dropAttributeName.isBlank()) {
                throw new IllegalArgumentException("dropAttributeName must be provided for DROP.");
            }

            AttributeSchema pk = table.getPrimaryKey();
            if (pk != null && pk.getName().equals(dropAttributeName.toLowerCase())) {
                return false;
            }

            boolean dropped = table.dropAttribute(dropAttributeName);
            if (!dropped) {
                return false;
            }
        } else {
            throw new IllegalArgumentException("Unsupported alter operation: " + operation);
        }

        // Rebuild table pages: create new pages, copy transformed data, relink chain, free old pages.
        int oldHeadPageId = table.getHeadPageId();
        if (oldHeadPageId == -1) {
            return true;
        }

        int currentOldPageId = oldHeadPageId;
        int newHeadPageId = -1;
        int newPrevPageId = -1;

        while (currentOldPageId != -1) {
            Page oldPage = this.buffer.getPage(currentOldPageId);
            int nextOldPageId = oldPage.getNextPage();

            Page newPage = this.buffer.createNewPage();
            List<byte[]> oldRecords = oldPage.getRecords();

            for (byte[] oldRecord : oldRecords) {
                byte[] rewrittenRecord = rewriteRecordForAlter(
                    oldRecord, operation, addAttribute, dropAttributeName
                );

                boolean inserted = newPage.addRecord(rewrittenRecord);
                if (!inserted) {
                    throw new IllegalStateException(
                        "ALTER rebuild failed: rewritten record did not fit in new page."
                    );
                }
            }

            if (newHeadPageId == -1) {
                newHeadPageId = newPage.getPageID();
                table.setHeadPageId(newHeadPageId);
            } else {
                updateTablePageLink(newPrevPageId, newPage.getPageID());
            }

            newPrevPageId = newPage.getPageID();

            freePage(oldPage);
            currentOldPageId = nextOldPageId;
        }

        return true;
    }

    public void freePage (Page page) {
        // clear page of data beside pageid
        page.cleanData();
        // setDirty inside of page where data cleaned
        page.setDirty();

        int currFreePageId = catalog.getFreePageListHead();
        if (currFreePageId == -1){
            catalog.setFreePageListHead(page.getPageID());
            return;
        }

        while (true) {
            Page currPage = this.buffer.getPage(currFreePageId);
            int nextPageId = currPage.getNextPage();

            if (nextPageId == -1) {
                currPage.setNextPage(page.getPageID());
                return;
            }

            currFreePageId = nextPageId;
        }
    }

    /**
     * Update linked-list pointer between pages.
     */
    public void updateTablePageLink(int fromPageId, int toPageId) {
        Page fromPage = this.buffer.getPage(fromPageId);
        fromPage.setNextPage(toPageId);
    }

    private int[] findTailPagesId(int headPageId) {
        int current = headPageId;
        int prev = -1;
        while (true) {
            Page page = this.buffer.getPage(current);
            int next = page.getNextPage();
            if (next == -1) {
                int[] result = new int[2];
                result[0] = prev;
                result[1] = current;
                return result;
            }
            prev = current;
            current = next;
        }
    }
}
