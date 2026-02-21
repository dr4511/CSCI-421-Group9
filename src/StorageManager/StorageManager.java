package StorageManager;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Record;
import java.io.File;
import java.util.List;

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

        int totalRows = 0;
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
                totalRows++;
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
        byte[] recordBytes = incomingRecord.toBytes(table);

        if (hasPrimaryKeyViolation(table, incomingRecord)) {
            return false;
        }

        int[] lastTwoPagesId = findTailPagesId(table.getHeadPageId());
        int beforeTailPageId = lastTwoPagesId[0];
        int tailPageId = lastTwoPagesId[1];

        Page tailPage = this.buffer.getPage(tailPageId);

        boolean inserted = tailPage.addRecord(recordBytes);
        if (inserted) {
            return true;
        }

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

        freePage(tailPage);

        return page2.addRecord(recordBytes);
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

    private void printSelectRows(TableSchema table, List<Record> rows) {
        int columnCount = table.getAttributeCount();
        if (columnCount == 0) {
            System.out.println("(no columns)");
            return;
        }

        int[] widths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            widths[i] = table.getAttributes().get(i).getName().length();
        }

        for (Record row : rows) {
            for (int i = 0; i < columnCount; i++) {
                String cell = formatSelectCell(row.getValue(i));
                if (cell.length() > widths[i]) {
                    widths[i] = cell.length();
                }
            }
        }

        String border = buildSelectBorder(widths);
        System.out.println(border);
        System.out.println(buildSelectHeader(table, widths));
        System.out.println(border);
        for (Record row : rows) {
            System.out.println(buildSelectRow(row, widths));
        }
        System.out.println(border);
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
