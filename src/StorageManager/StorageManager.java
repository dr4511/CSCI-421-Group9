package StorageManager;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Record;
import WhereTree.IOperandNode;
import WhereTree.IWhereTree;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class StorageManager {
    // Core dependencies/configuration.
    private final Buffer buffer;
    private final int pageSizeBytes;
    private final int bufferSizePages;
    private final Catalog catalog;
    private int nextTemporaryTableId;
    private static final int SLOT_ENTRY_BYTES = Integer.BYTES * 2;
    private static final int UNKNOWN_PREVIOUS_PAGE_ID = -2;

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
        this.nextTemporaryTableId = 0;

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
        if (table == null) {
            throw new IllegalArgumentException("table must be non-null.");
        }
        if (this.catalog.tableExists(table.getName())) {
            return false;
        }

        initializeTableStorage(table);
        initializeBPlusTreeMetadata(table);
        this.catalog.addTable(table);
        return true;
    }

    /**
     * Creates a transient table for a cartesian-product join.
     * The returned schema is not added to the catalog and must be dropped after query completion.
     * TODO: Move temporary table tracking into Catalog once the catalog layer owns temp-table lifecycle.
     */
    public TableSchema createTemporaryJoinTable(List<TableSchema> sourceTables) {
        TableSchema tempTable = new TableSchema("__temp_join_" + nextTemporaryTableId++);
        for (TableSchema sourceTable : sourceTables) {
            for (AttributeSchema attribute : sourceTable.getAttributes()) {
                String qualifiedName = attribute.getName().contains(".")
                        ? attribute.getName()
                        : sourceTable.getName() + "." + attribute.getName();

                AttributeSchema tempAttribute = new AttributeSchema(
                    qualifiedName,
                    attribute.getDataType(),
                    false,
                    attribute.isNotNull(),
                    attribute.getDefaultValue()
                );

                tempTable.addAttribute(tempAttribute);
            }
        }

        initializeTableStorage(tempTable);
        return tempTable;
    }

    /**
     * Materializes a cartesian-product join into a transient table and returns its schema.
     */
    public TableSchema materializeCartesianProduct(List<TableSchema> sourceTables) {
        TableSchema tempTable = createTemporaryJoinTable(sourceTables);
        Object[] joinedValues = new Object[tempTable.getAttributeCount()];
        materializeCartesianProductRecursive(sourceTables, 0, joinedValues, 0, tempTable);
        return tempTable;
    }

    /**
     * Materializes an ORDERBY result into a transient table.
     * The returned table keeps the same columns in the same order as the input table.
     */
    public TableSchema orderByTable(TableSchema table, AttributeSchema orderAttribute) {
        if (table == null || orderAttribute == null) {
            throw new IllegalArgumentException("table and orderAttribute must be non-null.");
        }

        String orderAttributeName = orderAttribute.getName();
        if (table.getAttribute(orderAttributeName) == null) {
            throw new IllegalArgumentException("Order attribute does not belong to table: " + orderAttribute.getName());
        }

        TableSchema orderedTable = new TableSchema("__temp_orderby_" + nextTemporaryTableId++);
        for (AttributeSchema attribute : table.getAttributes()) {
            boolean isOrderAttribute = attribute.getName().equals(orderAttributeName);
            AttributeSchema orderedAttribute = new AttributeSchema(
                attribute.getName(),
                attribute.getDataType(),
                isOrderAttribute,
                attribute.isNotNull(),
                attribute.getDefaultValue()
            );
            orderedTable.addAttribute(orderedAttribute);
        }

        initializeTableStorage(orderedTable);

        int currPageId = table.getHeadPageId();
        while (currPageId != -1) {
            Page page = this.buffer.getPage(currPageId);
            for (byte[] data : page.getRecords()) {
                Record record = Record.fromBytes(data, table);
                insertIntoTable(orderedTable, record.getValues(), false);
            }
            currPageId = page.getNextPage();
        }

        return orderedTable;
    }

    /**
     * Drops a transient join table and frees all of its pages.
     */
    public void dropTemporaryTable(TableSchema table) {
        if (table == null) {
            throw new IllegalArgumentException("table must be non-null.");
        }
        freeTablePages(table);
    }

    /**
     * Appends a record to the end of a table without primary-key ordering.
     * Intended for transient join tables and other internal rewrites.
     */
    public void appendRecordToTable(TableSchema table, Object[] values) {
        if (table == null || values == null) {
            throw new IllegalArgumentException("table and values must be non-null.");
        }
        if (values.length != table.getAttributeCount()) {
            throw new IllegalArgumentException("Value count does not match table attribute count.");
        }

        Record record = new Record(values.clone());
        byte[] recordBytes = record.toBytes(table);
        appendRecordBytes(table, recordBytes);
    }

    public TableSchema filterWhere(TableSchema table, IWhereTree whereTree) {
        // -create new table (copy) of schema of old table 
        // -for each page in table: 
        // For each record in page: 
        //    If wheretree(record): // if tree evals to true we insert
		// 	insert into new table
		// return new table 
        TableSchema resultTable = new TableSchema("__temp_where_" + nextTemporaryTableId++);
        for (AttributeSchema attr : table.getAttributes()) {
            resultTable.addAttribute(new AttributeSchema(attr.getName(), attr.getDataType(), false, attr.isNotNull(), attr.getDefaultValue()));
        }

        initializeTableStorage(resultTable);
        // resultTable.setHeadPageId(-1);   Do we need to make new pages or overwrite for this copy table
        int pageId = table.getHeadPageId();
        while(pageId != -1) {
            Page page = buffer.getPage(pageId);

            // Derialize records from byte[] to record
            List<byte[]> recordData = page.getRecords();
            for( byte[] data : recordData){
                Record record = Record.fromBytes(data, table);
                if(whereTree.evaluate(record, table)){
                    appendRecordToTable(resultTable, record.getValues());
                }
            }

            pageId = page.getNextPage();
        }
        return resultTable;
    }

    public TableSchema deleteWhere(TableSchema table, IWhereTree whereTree) {
        TableSchema resultTable = copyTableSchema(table);
        initializeTableStorage(resultTable);
        catalog.dropTable(table.getName());
        catalog.addTable(resultTable);

        int pageId = table.getHeadPageId();
        while(pageId != -1) {
            Page page = buffer.getPage(pageId);
            int nextPageId = page.getNextPage();

         
            // Derialize records from byte[] to record
            List<byte[]> recordData = page.getRecords();
            for( byte[] data : recordData){
                Record record = Record.fromBytes(data, table);
                // If where evaluates to true, do not add to table
                if(whereTree != null && !whereTree.evaluate(record, table)){
                    if (!insertIntoTable(resultTable, record.getValues())) {
                        throw new IllegalStateException("DELETE rebuild failed while inserting kept record.");
                    }
                }
            }
            pageId = nextPageId;
        }

        freeTablePages(table);
        return resultTable;
    }

    public void updateWhere(TableSchema table, AttributeSchema attr, IOperandNode newValue, IWhereTree whereTree) {
        TableSchema resultTable = copyTableSchema(table);
        initializeTableStorage(resultTable);
        catalog.dropTable(table.getName());
        catalog.addTable(resultTable);

        int idx = table.getAttributeIndex(attr.getName());
        int pageId = table.getHeadPageId();
        while (pageId != -1) {
            Page page = buffer.getPage(pageId);
            int nextPageId = page.getNextPage();
            for (byte[] data : page.getRecords()) {
                Record record = Record.fromBytes(data, table);
                Object[] values = record.getValues().clone();
                if (whereTree == null || whereTree.evaluate(record, table)) {
                    Object computed = newValue.getValue(record, table);
                    if (computed == null && attr.isNotNull()) {
                        throw new IllegalArgumentException(
                            "Cannot set attribute '" + attr.getName() + "' to NULL (NOTNULL constraint)");
                    }
                    values[idx] = computed;
                }
                if (!insertIntoTable(resultTable, values)) {
                    throw new IllegalStateException("UPDATE rebuild failed while inserting rewritten record.");
                }
            }
            pageId = nextPageId;
        }
        freeTablePages(table);
    }

    private void initializeTableStorage(TableSchema table) {
        Page newPage = this.buffer.createNewPage();
        newPage.setNextPage(-1);
        table.setHeadPageId(newPage.getPageID());
        table.setTailPageId(newPage.getPageID());
    }

    private TableSchema copyTableSchema(TableSchema table) {
        TableSchema copy = new TableSchema(table.getName());
        for (AttributeSchema attr : table.getAttributes()) {
            copy.addAttribute(new AttributeSchema(
                attr.getName(),
                attr.getDataType(),
                attr.isPrimaryKey(),
                attr.isNotNull(),
                attr.isUnique(),
                attr.getDefaultValue()
            ));
        }
        return copy;
    }

    private void materializeCartesianProductRecursive(List<TableSchema> sourceTables, int tableIndex, Object[] joinedValues, int valueOffset, TableSchema tempTable) {
        if (tableIndex == sourceTables.size()) {
            appendRecordToTable(tempTable, joinedValues);
            return;
        }

        TableSchema sourceTable = sourceTables.get(tableIndex);
        int currPageId = sourceTable.getHeadPageId();
        while (currPageId != -1) {
            Page page = this.buffer.getPage(currPageId);
            for (byte[] data : page.getRecords()) {
                Record record = Record.fromBytes(data, sourceTable);
                copyRecordValues(record, joinedValues, valueOffset);
                materializeCartesianProductRecursive(
                    sourceTables,
                    tableIndex + 1,
                    joinedValues,
                    valueOffset + sourceTable.getAttributeCount(),
                    tempTable
                );
            }
            currPageId = page.getNextPage();
        }
    }

    private void copyRecordValues(Record sourceRecord, Object[] joinedValues, int valueOffset) {
        for (int i = 0; i < sourceRecord.getNumAttributes(); i++) {
            joinedValues[valueOffset + i] = sourceRecord.getValue(i);
        }
    }

    private void appendRecordBytes(TableSchema table, byte[] recordBytes) {
        int tailId = table.getTailPageId();

        if (tailId != -1) {
            Page tailPage = this.buffer.getPage(tailId);
            if (tailPage.addRecord(recordBytes)) {
                return;
            }
        }

        Page newPage = this.buffer.createNewPage();
        if (!newPage.addRecord(recordBytes)) {
            throw new IllegalStateException("Record is too large to fit in a single page.");
        }

        if (tailId == -1) {
            table.setHeadPageId(newPage.getPageID());
        } else {
            Page tailPage = this.buffer.getPage(tailId);
            tailPage.setNextPage(newPage.getPageID());
        }

        table.setTailPageId(newPage.getPageID());
    }

    /**
     * SELECT * FROM <table> flow.
     * Reads pages one by one via buffer and prints SQL-style table output.
     */
    public void selectAllTable(TableSchema table) {
        if (table == null) {
            throw new IllegalArgumentException("table must be non-null.");
        }

        selectTableColumns(table, table.getAttributes());
    }

    /**
     * SELECT <attr1>, <attr2>, ... FROM <table> flow.
     * Reads pages one by one via buffer and prints only the selected columns.
     */
    public void selectTableColumns(TableSchema table, List<AttributeSchema> selectedAttributes) {
        if (table == null) {
            throw new IllegalArgumentException("table must be non-null.");
        }
        if (selectedAttributes == null) {
            throw new IllegalArgumentException("selectedAttributes must be non-null.");
        }

        int columnCount = selectedAttributes.size();
        if (columnCount == 0) {
            System.out.println("(no columns)");
            return;
        }

        int[] selectedIndexes = resolveSelectedIndexes(table, selectedAttributes);

        // Caluclate column widths first
        int[] widths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            widths[i] = selectedAttributes.get(i).getName().length();
        }

        int currPageId = table.getHeadPageId();
        while (currPageId != -1) { // iterate every page
            Page page = this.buffer.getPage(currPageId);
            for (byte[] data : page.getRecords()) { // iterate every record in page
                Record record = Record.fromBytes(data, table);
                for (int i = 0; i < columnCount; i++) { // iterate every cell in record
                    int cellWidth = formatSelectCell(record.getValue(selectedIndexes[i])).length();
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
        System.out.println(buildSelectHeader(selectedAttributes, widths));
        System.out.println(border);

        currPageId = table.getHeadPageId();
        while (currPageId != -1) {
            Page page = this.buffer.getPage(currPageId);
            for (byte[] data : page.getRecords()) {
                Record record = Record.fromBytes(data, table);
                System.out.println(buildSelectRow(record, selectedIndexes, widths));
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
        freeTablePages(table);
        catalog.dropTable(table.getName());
    }

    /**
     * INSERT INTO <table> flow.
     */
    public boolean insertIntoTable(TableSchema table, Object[] values) {
        return insertIntoTable(table, values, true);
    }

    /**
     * INSERT helper for cases like ORDERBY temp tables where duplicate sort-key values are allowed.
     */
    public boolean insertIntoTable(TableSchema table, Object[] values, boolean checkPrimaryKeyDuplicates) {
        if (table == null || values == null) {
            throw new IllegalArgumentException("table and values must be non-null.");
        }
        if (values.length != table.getAttributeCount()) {
            throw new IllegalArgumentException("Value count does not match table attribute count.");
        }

        Record incomingRecord = new Record(values.clone());
        if (shouldUseIndexedInsert(table, checkPrimaryKeyDuplicates)) {
            return insertIntoTableUsingIndex(table, incomingRecord);
        }

        if (checkPrimaryKeyDuplicates && hasPrimaryKeyViolation(table, incomingRecord)) {
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
                insertIntoPageOrSplit(table, prevPageId, pageId, insertIndex, incomingBytes, pkIndex, incomingPk, null);
                return true;
            }

            prevPageId = pageId;
            pageId = page.getNextPage();
        }

        return true;
    }

    private boolean shouldUseIndexedInsert(TableSchema table, boolean checkPrimaryKeyDuplicates) {
        return this.catalog.isIndexing()
            && checkPrimaryKeyDuplicates
            && table.getPrimaryKey() != null
            && this.catalog.getTable(table.getName()) == table;
    }

    private boolean insertIntoTableUsingIndex(TableSchema table, Record incomingRecord) {
        AttributeSchema primaryKey = table.getPrimaryKey();
        int pkIndex = table.getAttributeIndex(primaryKey.getName());
        Object incomingPk = incomingRecord.getValue(pkIndex);
        if (incomingPk == null) {
            return false;
        }

        initializeBPlusTreeMetadata(table);
        BPlusTree index = new BPlusTree(this.buffer, table);
        if (index.contains(incomingPk)) {
            return false;
        }

        byte[] incomingBytes = incomingRecord.toBytes(table);
        int targetPageId = index.findTablePageForKey(incomingPk);
        if (targetPageId == -1) {
            targetPageId = table.getHeadPageId();
        }

        Page targetPage = this.buffer.getPage(targetPageId);
        int insertIndex = findInsertIndexInPage(table, targetPage, pkIndex, incomingPk);
        int insertedPageId = insertIntoPageOrSplit(table, UNKNOWN_PREVIOUS_PAGE_ID, targetPageId, insertIndex, incomingBytes, pkIndex, incomingPk, index);
        index.upsert(incomingPk, insertedPageId);
        return true;
    }

    private void initializeBPlusTreeMetadata(TableSchema table) {
        if (!this.catalog.isIndexing()) {
            return;
        }

        AttributeSchema primaryKey = table.getPrimaryKey();
        if (primaryKey == null) {
            return;
        }

        if (table.getBtreeN() < 2) {
            table.setBtreeN(Math.max(2, BPlusTreeNode.computeN(this.pageSizeBytes, primaryKey)));
        }
    }

    private int findPreviousPageId(TableSchema table, int targetPageId) {
        int prevPageId = -1;
        int pageId = table.getHeadPageId();

        while (pageId != -1 && pageId != targetPageId) {
            Page page = this.buffer.getPage(pageId);
            prevPageId = pageId;
            pageId = page.getNextPage();
        }

        if (pageId == -1) {
            throw new IllegalStateException("Indexed insert target page is not in table page chain.");
        }

        return prevPageId;
    }

    /**
     * TRICK IS TO JUST CREATE COMPLETELY NEW TABLES
     * COPY OVER ALL THE DATA TO THE NEW TABLES
     */
    public boolean alterTablePages(TableSchema oldTableSchema, TableSchema newTableSchema) {
        if (oldTableSchema == null || newTableSchema == null) {
            throw new IllegalArgumentException("oldTableSchema and newTableSchema must be non-null.");
        }

        int oldHeadPageId = oldTableSchema.getHeadPageId();
        initializeTableStorage(newTableSchema);
        newTableSchema.setBtreeRootPageId(-1);
        catalog.dropTable(oldTableSchema.getName());
        catalog.addTable(newTableSchema);

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

        freeTablePages(oldTableSchema);

        return true;
    }

    public void freePage (Page page) {
        freePageById(page.getPageID());
    }

    private void freePageById(int pageId) {
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
        this.buffer.close();
    }

    private void freeTablePages(TableSchema table) {
        int currPageId = table.getHeadPageId();
        while (currPageId != -1) {
            Page page = this.buffer.getPage(currPageId);

            int nextPageId = page.getNextPage();
            freePage(page);
            currPageId = nextPageId;
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

    private int findInsertIndexInPage(TableSchema table, Page page, int pkIndex, Object incomingPk) {
        List<byte[]> records = page.getRecords();
        for (int i = 0; i < records.size(); i++) {
            Record existingRecord = Record.fromBytes(records.get(i), table);
            Object existingPk = existingRecord.getValue(pkIndex);
            if (comparePrimaryKeys(incomingPk, existingPk) < 0) {
                return i;
            }
        }
        return records.size();
    }

    private int insertIntoPageOrSplit(TableSchema table, int prevPageId, int pageId, int insertIndex, byte[] incomingBytes, int pkIndex, Object incomingPk, BPlusTree index) {
        Page page = this.buffer.getPage(pageId);
        int nextPageId = page.getNextPage();

        List<byte[]> candidateRecords = new ArrayList<>(page.getRecords());
        if (insertIndex < 0 || insertIndex > candidateRecords.size()) {
            throw new IllegalStateException("Invalid insertion position.");
        }
        candidateRecords.add(insertIndex, incomingBytes);

        if (page.getFreeSpace() >= incomingBytes.length + SLOT_ENTRY_BYTES) {
            rewritePageRecords(page, candidateRecords, nextPageId);
            updateTailAfterInsert(table, page.getPageID(), nextPageId);
            return page.getPageID();
        }

        int leftPageId = this.buffer.createNewPage().getPageID();
        int rightPageId = this.buffer.createNewPage().getPageID();
        int mid = candidateRecords.size() / 2;
        rewritePageRecords(leftPageId, new ArrayList<>(candidateRecords.subList(0, mid)), rightPageId);
        rewritePageRecords(rightPageId, new ArrayList<>(candidateRecords.subList(mid, candidateRecords.size())), nextPageId);

        if (prevPageId == UNKNOWN_PREVIOUS_PAGE_ID) {
            prevPageId = findPreviousPageId(table, pageId);
        }

        if (prevPageId == -1) {
            table.setHeadPageId(leftPageId);
        } else {
            Page prevPage = this.buffer.getPage(prevPageId);
            prevPage.setNextPage(leftPageId);
        }

        freePageById(pageId);
        updateTailAfterInsert(table, rightPageId, nextPageId);
        updateIndexPointersAfterSplit(table, leftPageId, rightPageId, pkIndex, index);
        return insertIndex < mid ? leftPageId : rightPageId;
    }

    private void updateTailAfterInsert(TableSchema table, int insertedPageId, int nextPageId) {
        if (nextPageId == -1) {
            table.setTailPageId(insertedPageId);
        }
    }

    private void updateIndexPointersAfterSplit(TableSchema table, int leftPageId, int rightPageId, int pkIndex, BPlusTree index) {
        if (index == null) {
            return;
        }

        updateIndexPointersForPage(table, leftPageId, pkIndex, index);
        updateIndexPointersForPage(table, rightPageId, pkIndex, index);
    }

    private void updateIndexPointersForPage(TableSchema table, int pageId, int pkIndex, BPlusTree index) {
        List<byte[]> records = new ArrayList<>(this.buffer.getPage(pageId).getRecords());
        for (byte[] data : records) {
            Record record = Record.fromBytes(data, table);
            index.upsert(record.getValue(pkIndex), pageId);
        }
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
        return comparePrimaryKeys(incomingPk, pageLastPk) < 0 || page.getNextPage() == -1;
    }

    @SuppressWarnings("unchecked")
    private int comparePrimaryKeys(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }

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

    private void rewritePageRecords(int pageId, List<byte[]> records, int nextPageId) {
        rewritePageRecords(this.buffer.getPage(pageId), records, nextPageId);
    }

    private String buildSelectBorder(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : widths) {
            sb.append("-".repeat(width + 2)).append("+");
        }
        return sb.toString();
    }

    private int[] resolveSelectedIndexes(TableSchema table, List<AttributeSchema> selectedAttributes) {
        int[] selectedIndexes = new int[selectedAttributes.size()];
        for (int i = 0; i < selectedAttributes.size(); i++) {
            AttributeSchema selectedAttribute = selectedAttributes.get(i);
            if (selectedAttribute == null) {
                throw new IllegalArgumentException("selectedAttributes cannot contain null entries.");
            }

            int attributeIndex = table.getAttributeIndex(selectedAttribute.getName());
            if (attributeIndex == -1) {
                throw new IllegalArgumentException("Selected attribute does not belong to table: " + selectedAttribute.getName());
            }

            selectedIndexes[i] = attributeIndex;
        }
        return selectedIndexes;
    }

    private String buildSelectHeader(List<AttributeSchema> selectedAttributes, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String header = selectedAttributes.get(i).getName();
            sb.append(" ").append(String.format("%-" + widths[i] + "s", header)).append(" |");
        }
        return sb.toString();
    }

    private String buildSelectRow(Record row, int[] selectedIndexes, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < widths.length; i++) {
            String cell = formatSelectCell(row.getValue(selectedIndexes[i]));
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
