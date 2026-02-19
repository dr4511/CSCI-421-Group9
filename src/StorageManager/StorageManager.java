package StorageManager;

import java.util.List;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Record;

public class StorageManager {
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
    public boolean alterTablePages(TableSchema oldTableSchema, TableSchema newTableSchema) {
        if (oldTableSchema == null || newTableSchema == null) {
            throw new IllegalArgumentException("oldTableSchema and newTableSchema must be non-null.");
        }

        // Rebuild table pages: iterate old chain, rewrite each record, and build a new chain.
        int oldHeadPageId = oldTableSchema.getHeadPageId();
        if (oldHeadPageId == -1) {
            newTableSchema.setHeadPageId(-1);
            return true;
        }

        int currentOldPageId = oldHeadPageId;
        int newHeadPageId = -1;
        int prevNewPageId = -1;
        Page currentNewPage = null;

        while (currentOldPageId != -1) {
            Page oldPage = this.buffer.getPage(currentOldPageId);
            int nextOldPageId = oldPage.getNextPage();

            List<byte[]> oldRecords = oldPage.getRecords();

            for (byte[] oldRecord : oldRecords) {
                byte[] rewrittenRecord = rewriteRecordForAlter(
                    oldRecord,
                    oldTableSchema,
                    newTableSchema
                );

                if (currentNewPage == null) {
                    currentNewPage = this.buffer.createNewPage();
                    currentNewPage.setNextPage(-1);
                    newHeadPageId = currentNewPage.getPageID();
                    newTableSchema.setHeadPageId(newHeadPageId);
                }

                boolean inserted = currentNewPage.addRecord(rewrittenRecord);
                if (!inserted) {
                    int oldNextFromCurrent = currentNewPage.getNextPage();
                    Page splitPage1 = this.buffer.createNewPage();
                    Page splitPage2 = this.buffer.createNewPage();
                    splitPage1.setNextPage(splitPage2.getPageID());
                    currentNewPage.split(splitPage1, splitPage2);
                    splitPage2.setNextPage(oldNextFromCurrent);

                    if (prevNewPageId == -1) {
                        newTableSchema.setHeadPageId(splitPage1.getPageID());
                        newHeadPageId = splitPage1.getPageID();
                    } else {
                        updateTablePageLink(prevNewPageId, splitPage1.getPageID());
                    }

                    freePage(currentNewPage);

                    prevNewPageId = splitPage1.getPageID();
                    currentNewPage = splitPage2;

                    boolean insertedInNewPage = splitPage2.addRecord(rewrittenRecord);
                    if (!insertedInNewPage) {
                        throw new IllegalStateException(
                            "ALTER rebuild failed: rewritten record did not fit in empty new page."
                        );
                    }
                }
            }

            freePage(oldPage);
            currentOldPageId = nextOldPageId;
        }

        // Keep a valid table head if old chain had pages but no records.
        if (newHeadPageId == -1) {
            Page emptyHead = this.buffer.createNewPage();
            emptyHead.setNextPage(-1);
            newHeadPageId = emptyHead.getPageID();
            newTableSchema.setHeadPageId(newHeadPageId);
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

    private byte[] rewriteRecordForAlter(byte[] oldRecord, TableSchema oldTable, TableSchema newTable) {
        // Deserialize using old schema, rebuild using new schema.
        Record oldRec = Record.fromBytes(oldRecord, oldTable);
        Record newRec = new Record(newTable.getAttributeCount());

        for (int newAttrIndex = 0; newAttrIndex < newTable.getAttributeCount(); newAttrIndex++) {
            AttributeSchema newAttr = newTable.getAttributes().get(newAttrIndex);
            int oldAttrIndex = oldTable.getAttributeIndex(newAttr.getName());

            if (oldAttrIndex != -1) {
                newRec.setValue(newAttrIndex, oldRec.getValue(oldAttrIndex));
                continue;
            }

            // Attribute exists only in new schema (ADD case): use default/null.
            newRec.setValue(newAttrIndex, newAttr.getDefaultValue());
        }

        return newRec.toBytes(newTable);
    }
}
