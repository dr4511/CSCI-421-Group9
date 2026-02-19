package StorageManager;

import java.io.File;
import java.util.List;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Record;

public class StorageManager {
    // Core dependencies/configuration.
    private final Buffer buffer;
    private final int pageSizeBytes;
    private final int bufferSizePages;
    private final Catalog catalog;

    public StorageManager(File dbFile, int pageSizeBytes, int bufferSizePages, Catalog catalog) {
        if (dbFile == null || !dbFile.exists()) {
            throw new IllegalArgumentException("dbFile must be non-null and exist.");
        }
        if (pageSizeBytes <= 0) {
            throw new IllegalArgumentException("pageSizeBytes must be > 0.");
        }
        if (bufferSizePages <= 0) {
            throw new IllegalArgumentException("bufferSizePages must be > 0.");
        }
        if (catalog == null) {
            throw new IllegalArgumentException("catalog must be non-null.");
        }

        this.pageSizeBytes = pageSizeBytes;
        this.bufferSizePages =bufferSizePages;
        this.catalog = catalog;

        this.buffer = new Buffer(pageSizeBytes, bufferSizePages, dbFile, catalog);
    }

    public Buffer getBuffer() {
        return this.buffer;
    }

    public int getPageSizeBytes() {
        return this.pageSizeBytes;
    }

    public int getBufferSizePages() {
        return this.bufferSizePages;
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
    public boolean insertIntoTable(TableSchema table, Object[] values) {
        if (table == null || values == null) {
            throw new IllegalArgumentException("table and values must be non-null.");
        }
        if (values.length != table.getAttributeCount()) {
            throw new IllegalArgumentException("Value count does not match table attribute count.");
        }

        Record incomingRecord = new Record(values.clone());
        int recordSizeBytes = 0;
        for (AttributeSchema attr : table.getAttributes()) {
            recordSizeBytes += incomingRecord.calculateAttributeSize(attr, incomingRecord.getValue(table.getAttributes().indexOf(attr)));
        }

        if (hasPrimaryKeyViolation(table, incomingRecord)) {
            return false;
        }

        int[] lastTwoPagesId = findTailPagesId(table.getHeadPageId());
        int beforeTailPageId = lastTwoPagesId[0];
        int tailPageId = lastTwoPagesId[1];

        Page tailPage = this.buffer.getPage(tailPageId);

        boolean inserted = tailPage.addRecord(incomingRecord, recordSizeBytes);
        if (inserted) {
            return true;
        }

        Page page1 = this.buffer.createNewPage();
        Page page2 = this.buffer.createNewPage();

        // set page1 nextPage to page2 id
        page1.setNextPage(page2.getPageID());

        tailPage.split(page1, page2, recordSizeBytes);

        if (beforeTailPageId == -1){
            table.setHeadPageId(page1.getPageID());
        } else {
            updateTablePageLink(beforeTailPageId, page1.getPageID());
        }

        freePage(tailPage);

        return page2.addRecord(incomingRecord, recordSizeBytes);
    }

    /**
     * TRICK IS TO JUST CREATE COMPLETELY NEW TABLES
     * COPY OVER ALL THE DATA TO THE NEW TABLES
     */
    public boolean alterTablePages(TableSchema oldTableSchema, TableSchema newTableSchema) {
        if (oldTableSchema == null || newTableSchema == null) {
            throw new IllegalArgumentException("oldTableSchema and newTableSchema must be non-null.");
        }

        // Rebuild table pages: iterate old chain, rewrite each record, and insert via insertIntoTable.
        int oldHeadPageId = oldTableSchema.getHeadPageId();
        Page newHeadPage = this.buffer.createNewPage();
        newHeadPage.setNextPage(-1);
        newTableSchema.setHeadPageId(newHeadPage.getPageID());

        if (oldHeadPageId == -1) {
            return true;
        }

        int currentOldPageId = oldHeadPageId;

        while (currentOldPageId != -1) {
            Page oldPage = this.buffer.getPage(currentOldPageId);
            int nextOldPageId = oldPage.getNextPage();

            List<Record> oldRecords = oldPage.getRecords();

            for (Record oldRecord : oldRecords) {
                Record rewrittenRecord = rewriteRecordForAlter(oldRecord, oldTableSchema, newTableSchema);
                boolean inserted = insertIntoTable(newTableSchema, rewrittenRecord.getValues());
                if (!inserted) {
                    throw new IllegalStateException("ALTER rebuild failed while inserting rewritten record.");
                }
            }

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

    public void evictAll() {
        this.buffer.evictAll();
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

    private Record rewriteRecordForAlter(Record oldRec, TableSchema oldTable, TableSchema newTable) {
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

        return newRec;
    }

    private boolean hasPrimaryKeyViolation(TableSchema table, Record candidate) {
        AttributeSchema pk = table.getPrimaryKey();
        if (pk == null) {
            return false;
        }

        int pkIndex = table.getAttributeIndex(pk.getName());
        if (pkIndex < 0) {
            return false;
        }

        Object candidatePkValue = candidate.getValue(pkIndex);
        if (candidatePkValue == null) {
            return true;
        }

        int pageId = table.getHeadPageId();
        while (pageId != -1) {
            Page page = this.buffer.getPage(pageId);
            List<Record> existingRecords = page.getRecords();

            for (Record record : existingRecords) {
                Object existingPkValue = record.getValue(pkIndex);
                if (existingPkValue != null && existingPkValue.equals(candidatePkValue)) {
                    return true;
                }
            }

            pageId = page.getNextPage();
        }

        return false;
    }
}
