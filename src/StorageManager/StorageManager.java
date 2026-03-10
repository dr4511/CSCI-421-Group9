package StorageManager;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Record;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class StorageManager {
    // Core dependencies/configuration.
    private final Buffer buffer;
    private final int pageSizeBytes;
    private final int bufferSizePages;
    private final Catalog catalog;
    private static final int SLOT_ENTRY_BYTES = Integer.BYTES * 2;

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
     * Reads pages one by one via buffer and prints SQL-style table output.
     */
    public void selectAllTable(TableSchema table) {
        if (table == null) {
            throw new IllegalArgumentException("table must be non-null.");
        }

        int columnCount = table.getAttributeCount();
        if (columnCount == 0) {
            System.out.println("(no columns)");
            return;
        }

        // Caluclate column widths first
        int[] widths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            widths[i] = table.getAttributes().get(i).getName().length();
        }

        int currPageId = table.getHeadPageId();
        while (currPageId != -1) { // iterate every page
            Page page = this.buffer.getPage(currPageId);
            for (byte[] data : page.getRecords()) { // iterate every record in page
                Record record = Record.fromBytes(data, table);
                for (int i = 0; i < columnCount; i++) { // iterate every cell in record
                    int cellWidth = formatSelectCell(record.getValue(i)).length();
                    if (cellWidth > widths[i]) {
                        widths[i] = cellWidth;
                    }
                }
            }
            currPageId = page.getNextPage();
        }

        // print pages, should be in cache from LRU
        String border = buildSelectBorder(widths);
        System.out.println(border);
        System.out.println(buildSelectHeader(table, widths));
        System.out.println(border);

        currPageId = table.getHeadPageId();
        while (currPageId != -1) {
            Page page = this.buffer.getPage(currPageId);
            for (byte[] data : page.getRecords()) {
                Record record = Record.fromBytes(data, table);
                System.out.println(buildSelectRow(record, widths));
            }
            currPageId = page.getNextPage();
        }

        System.out.println(border);
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
        catalog.dropTable(table.getName());
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
        if (hasPrimaryKeyViolation(table, incomingRecord)) {
            return false;
        }

        AttributeSchema primaryKey = table.getPrimaryKey();
        if (primaryKey == null) {
            throw new IllegalStateException("Primary key not found.");
        }
        int pkIndex = table.getAttributeIndex(primaryKey.getName());
        byte[] incomingBytes = incomingRecord.toBytes(table);
        Object incomingPk = incomingRecord.getValue(pkIndex);

        int prevPageId = -1;
        int pageId = table.getHeadPageId();
        while (pageId != -1) {
            Page page = this.buffer.getPage(pageId);
            if (shouldInsertInPage(table, page, pkIndex, incomingPk)) {
                int insertIndex = findInsertIndexInPage(table, page, pkIndex, incomingPk);
                insertIntoPageOrSplit(table, prevPageId, pageId, insertIndex, incomingBytes, pkIndex, incomingPk);
                return true;
            }

            prevPageId = pageId;
            pageId = page.getNextPage();
        }

        return true;
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

            for (byte[] data : oldPage.getRecords()) {
                Record oldRecord = Record.fromBytes(data, oldTableSchema);
                Record rewrittenRecord = rewriteRecordForAlter(oldRecord, oldTableSchema, newTableSchema);
                boolean inserted = insertIntoTable(newTableSchema, rewrittenRecord.getValues());
                if (!inserted) {
                    throw new IllegalStateException("ALTER rebuild failed while inserting rewritten record.");
                }
            }

            currentOldPageId = nextOldPageId;
        }

        catalog.dropTable(oldTableSchema.getName());
        catalog.addTable(newTableSchema);

        return true;
    }

    public void freePage (Page page) {
        int pageId = page.getPageID();
        Page pageToFree = this.buffer.getPage(pageId);
        int oldFreeHead = catalog.getFreePageListHead();

        if (oldFreeHead == pageId) {
            return;
        }

        pageToFree.cleanData();
        pageToFree.setNextPage(oldFreeHead);
        catalog.setFreePageListHead(pageId);
    }

    public void evictAll() {
        this.buffer.evictAll();
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
            for (byte[] data : page.getRecords()) {
                Record record = Record.fromBytes(data, table);
                Object existingPkValue = record.getValue(pkIndex);
                if (existingPkValue != null && existingPkValue.equals(candidatePkValue)) {
                    return true;
                }
            }

            pageId = page.getNextPage();
        }

        return false;
    }

    private int findInsertIndexInPage(TableSchema table, Page page, int pkIndex, Object incomingPk) {
        List<byte[]> records = page.getRecords();
        for (int i = 0; i < records.size(); i++) {
            Record existingRecord = Record.fromBytes(records.get(i), table);
            Object existingPk = existingRecord.getValue(pkIndex);
            if (comparePrimaryKeys(incomingPk, existingPk) <= 0) {
                return i;
            }
        }
        return records.size();
    }

    private void insertIntoPageOrSplit(TableSchema table, int prevPageId, int pageId, int insertIndex, byte[] incomingBytes, int pkIndex, Object incomingPk) {
        Page page = this.buffer.getPage(pageId);
        int nextPageId = page.getNextPage();

        List<byte[]> candidateRecords = new ArrayList<>(page.getRecords());
        if (insertIndex < 0 || insertIndex > candidateRecords.size()) {
            throw new IllegalStateException("Invalid insertion position.");
        }
        candidateRecords.add(insertIndex, incomingBytes);

        if (page.getFreeSpace() >= incomingBytes.length + SLOT_ENTRY_BYTES) {
            rewritePageRecords(page, candidateRecords, nextPageId);
            return;
        }

        Page leftPage = this.buffer.createNewPage();
        Page rightPage = this.buffer.createNewPage();
        leftPage.setNextPage(rightPage.getPageID());
        rightPage.setNextPage(nextPageId);
        page.split(leftPage, rightPage);

        if (prevPageId == -1) {
            table.setHeadPageId(leftPage.getPageID());
        } else {
            Page prevPage = this.buffer.getPage(prevPageId);
            prevPage.setNextPage(leftPage.getPageID());
        }

        freePage(page);

        Page targetPage = leftPage;
        int targetPrevPageId = prevPageId;

        if (!rightPage.getRecords().isEmpty()) {
            Object rightFirstPk = firstPrimaryKeyInPage(table, rightPage, pkIndex);
            if (comparePrimaryKeys(incomingPk, rightFirstPk) > 0) {
                targetPage = rightPage;
                targetPrevPageId = leftPage.getPageID();
            }
        }

        int nextInsertIndex = findInsertIndexInPage(table, targetPage, pkIndex, incomingPk);
        insertIntoPageOrSplit(table, targetPrevPageId, targetPage.getPageID(), nextInsertIndex, incomingBytes, pkIndex, incomingPk);
    }

    private Object firstPrimaryKeyInPage(TableSchema table, Page page, int pkIndex) {
        byte[] firstRecord = page.getRecords().get(0);
        return Record.fromBytes(firstRecord, table).getValue(pkIndex);
    }

    private Object lastPrimaryKeyInPage(TableSchema table, Page page, int pkIndex) {
        List<byte[]> records = page.getRecords();
        byte[] lastRecord = records.get(records.size() - 1);
        return Record.fromBytes(lastRecord, table).getValue(pkIndex);
    }

    private boolean shouldInsertInPage(TableSchema table, Page page, int pkIndex, Object incomingPk) {
        if (page.getRecords().isEmpty()) {
            return true;
        }

        Object pageLastPk = lastPrimaryKeyInPage(table, page, pkIndex);
        return comparePrimaryKeys(incomingPk, pageLastPk) <= 0 || page.getNextPage() == -1;
    }

    @SuppressWarnings("unchecked")
    private int comparePrimaryKeys(Object left, Object right) {
        Comparable<Object> comparableLeft = (Comparable<Object>) left;
        return comparableLeft.compareTo(right);
    }

    private void rewritePageRecords(Page page, List<byte[]> records, int nextPageId) {
        page.cleanData();
        page.setNextPage(nextPageId);
        for (byte[] record : records) {
            if (!page.addRecord(record)) {
                throw new IllegalStateException("Failed to insert record into page.");
            }
        }
    }

    private String buildSelectBorder(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : widths) {
            sb.append("-".repeat(width + 2)).append("+");
        }
        return sb.toString();
    }

    private String buildSelectHeader(TableSchema table, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String header = table.getAttributes().get(i).getName();
            sb.append(" ").append(String.format("%-" + widths[i] + "s", header)).append(" |");
        }
        return sb.toString();
    }

    private String buildSelectRow(Record row, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String cell = formatSelectCell(row.getValue(i));
            sb.append(" ").append(String.format("%-" + widths[i] + "s", cell)).append(" |");
        }
        return sb.toString();
    }

    private String formatSelectCell(Object value) {
        if (value == null) {
            return "NULL";
        }
        return String.valueOf(value);
    }

}
