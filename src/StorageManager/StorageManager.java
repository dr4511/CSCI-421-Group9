package StorageManager;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Record;
import WhereTree.IOperandNode;
import WhereTree.IWhereTree;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class StorageManager {
    // Core dependencies/configuration.
    private final Buffer buffer;
    private final int pageSizeBytes;
    private final int bufferSizePages;
    private final Catalog catalog;
    private int nextTemporaryTableId;
    private static final int SLOT_ENTRY_BYTES = Integer.BYTES * 2;
    private final Map<String, BPlusTree> uniqueIndexes = new HashMap<>();

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

        // Rebuild in-memory unique-index B+ trees from catalog
        rebuildUniqueIndexesFromCatalog();
    }

    private void rebuildUniqueIndexesFromCatalog() {
        for (TableSchema table : catalog.getAllTables()) {
            for (AttributeSchema attr : table.getAttributes()) {
                // Primary keys are enforced separately via hasPrimaryKeyViolation; skip them.
                if (attr.isUnique() && !attr.isPrimaryKey() && attr.hasUniqueIndex()) {
                    String key = table.getName() + "." + attr.getName();
                    TableSchema proxy = buildUniqueIndexProxy(table, attr);
                    uniqueIndexes.put(key, new BPlusTree(buffer, proxy, attr));
                }
            }
        }
    }

    private TableSchema buildUniqueIndexProxy(TableSchema realTable, AttributeSchema attr) {
        TableSchema proxy = new TableSchema(realTable) {
            @Override public int  getBtreeRootPageId()      { return attr.getUniqueIndexRootPageId(); }
            @Override public void setBtreeRootPageId(int id){ attr.setUniqueIndexRootPageId(id); }
            @Override public int  getBtreeN()               { return attr.getUniqueIndexN(); }
            @Override public void setBtreeN(int n)          { attr.setUniqueIndexN(n); }
            @Override public boolean hasBtreeIndex()        { return attr.getUniqueIndexRootPageId() != -1; }
        };
        return proxy;
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

        // Build a unique-column B+ tree for each UNIQUE (non-PK) attribute.
        for (AttributeSchema attr : table.getAttributes()) {
            if (attr.isUnique() && !attr.isPrimaryKey()) {
                attr.setUniqueIndexN(BPlusTreeNode.computeN(pageSizeBytes, attr));
                String key = table.getName() + "." + attr.getName();
                TableSchema proxy = buildUniqueIndexProxy(table, attr);
                uniqueIndexes.put(key, new BPlusTree(buffer, proxy, attr));
            }
        }

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
                    attribute.isUnique(),
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
                attribute.isUnique(),
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
            resultTable.addAttribute(new AttributeSchema(attr.getName(), attr.getDataType(), false, attr.isNotNull(),attr.isUnique() ,attr.getDefaultValue()));
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
    TableSchema resultTable = new TableSchema("__temp_where_" + nextTemporaryTableId++);
    for (AttributeSchema attr : table.getAttributes()) {
        resultTable.addAttribute(new AttributeSchema(
            attr.getName(), attr.getDataType(), false,
            attr.isNotNull(), attr.isUnique(), attr.getDefaultValue()));
    }
    initializeTableStorage(resultTable);

    AttributeSchema pk = table.getPrimaryKey();
    BPlusTree pkTree = (catalog.isIndexing() && table.hasBtreeIndex() && pk != null)
        ? new BPlusTree(buffer, table, pk)
        : null;

    int pageId = table.getHeadPageId();
    while (pageId != -1) {
        Page page = buffer.getPage(pageId);
        List<byte[]> recordData = page.getRecords();
        for (byte[] data : recordData) {
            Record record = Record.fromBytes(data, table);
            if (whereTree != null && !whereTree.evaluate(record, table)) {
                // Record survives — copy it to the result table
                appendRecordToTable(resultTable, record.getValues());
            } else if (catalog.isIndexing() && pkTree != null) {
                // Record is being deleted — remove it from the PK index
                int pkIdx = table.getAttributeIndex(pk.getName());
                Object pkVal = record.getValue(pkIdx);
                pkTree.upsert(pkVal, -1);
            }
        }
        pageId = page.getNextPage();
    }

    freeTablePages(table);
    table.setHeadPageId(resultTable.getHeadPageId());
    table.setTailPageId(resultTable.getTailPageId());
    return resultTable;
}

    public void updateWhere(TableSchema table, AttributeSchema attr, IOperandNode newValue, IWhereTree whereTree) {
    TableSchema resultTable = new TableSchema(table.getName());
    for (AttributeSchema a : table.getAttributes()) {
        resultTable.addAttribute(new AttributeSchema(
            a.getName(), a.getDataType(), a.isPrimaryKey(),
            a.isNotNull(), a.isUnique(), a.getDefaultValue()));
    }
    initializeTableStorage(resultTable);

    AttributeSchema pk = table.getPrimaryKey();
    BPlusTree pkTree = (catalog.isIndexing() && table.hasBtreeIndex() && pk != null)
        ? new BPlusTree(buffer, table, pk)
        : null;

    int idx = table.getAttributeIndex(attr.getName());
    int pageId = table.getHeadPageId();
    while (pageId != -1) {
        Page page = buffer.getPage(pageId);
        for (byte[] data : page.getRecords()) {
            Record record = Record.fromBytes(data, table);
            Object[] values = record.getValues().clone();
            if (whereTree == null || whereTree.evaluate(record, table)) {
                Object computed = newValue.getValue(record, table);
                if (computed == null && attr.isNotNull()) {
                    throw new IllegalArgumentException(
                        "Cannot set attribute '" + attr.getName() + "' to NULL (NOTNULL constraint)");
                }
                if (attr.isUnique()) {
                    String key = table.getName() + "." + attr.getName();
                    BPlusTree tree = uniqueIndexes.get(key);
                    if (tree != null && tree.contains(computed)) {
                        Object oldVal = record.getValue(idx);
                        if (oldVal == null || !oldVal.equals(computed)) {
                            throw new IllegalArgumentException(
                                "UNIQUE constraint violation on column '" + attr.getName() + "'");
                        }
                    }
                }

                // If the PK itself is being updated, remove the old PK entry from the index
                if (catalog.isIndexing() && pkTree != null && attr.isPrimaryKey()) {
                    int pkIdx = table.getAttributeIndex(pk.getName());
                    Object oldPkVal = record.getValue(pkIdx);
                    pkTree.upsert(oldPkVal, -1);
                }

                values[idx] = computed;
            }

            // insertIntoTable will re-index the new PK value via updateUniqueIndexes
            insertIntoTable(resultTable, values);
        }
        pageId = page.getNextPage();
    }

    freeTablePages(table);
    table.setHeadPageId(resultTable.getHeadPageId());
    table.setTailPageId(resultTable.getTailPageId());
}

    private void initializeTableStorage(TableSchema table) {
        Page newPage = this.buffer.createNewPage();
        newPage.setNextPage(-1);
        table.setHeadPageId(newPage.getPageID());
        table.setTailPageId(newPage.getPageID());
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
    if (checkPrimaryKeyDuplicates && hasPrimaryKeyViolation(table, incomingRecord)) {
        return false;
    }

    for (AttributeSchema attr : table.getAttributes()) {
        if (attr.isUnique()) {
            int idx = table.getAttributeIndex(attr.getName());
            Object value = values[idx];
            String key = table.getName() + "." + attr.getName();
            BPlusTree tree = uniqueIndexes.get(key);
            if (tree != null && tree.contains(value)) {
                return false;
            }
        }
    }

    AttributeSchema primaryKey = table.getPrimaryKey();
    if (primaryKey == null) {
        throw new IllegalStateException("Primary key not found.");
    }
    int pkIndex = table.getAttributeIndex(primaryKey.getName());
    byte[] incomingBytes = incomingRecord.toBytes(table);
    Object incomingPk = incomingRecord.getValue(pkIndex);

    if (catalog.isIndexing() && table.hasBtreeIndex()) {
        return insertWithIndex(table, values, incomingBytes, pkIndex, incomingPk);
    } else {
        return insertBruteForce(table, values, incomingBytes, pkIndex, incomingPk);
    }
}

/**
 * Index-assisted insertion: uses the primary key B+ tree to locate the correct
 * page directly, skipping the linear page scan.
 */
private boolean insertWithIndex(TableSchema table, Object[] values, byte[] incomingBytes, int pkIndex, Object incomingPk) {
    BPlusTree pkTree = new BPlusTree(buffer, table, table.getPrimaryKey());
    int targetPageId = pkTree.findTablePageForKey(incomingPk);

    if (targetPageId == -1) {
        // Key not yet in index — fall back to brute force for placement
        return insertBruteForce(table, values, incomingBytes, pkIndex, incomingPk);
    }

    int prevPageId = findPrevPageId(table, targetPageId);
    Page targetPage = buffer.getPage(targetPageId);
    int insertIndex = findInsertIndexInPage(table, targetPage, pkIndex, incomingPk);
    insertIntoPageOrSplit(table, prevPageId, targetPageId, insertIndex, incomingBytes, pkIndex, incomingPk);
    updateUniqueIndexes(table, values, targetPageId);
    return true;
}
/**
 * Brute-force insertion: linear scan through pages to find the correct position.
 * Used when indexing is disabled or the table has no B+ tree index.
 */
private boolean insertBruteForce(TableSchema table, Object[] values, byte[] incomingBytes, int pkIndex, Object incomingPk) {
    int prevPageId = -1;
    int pageId = table.getHeadPageId();
    while (pageId != -1) {
        Page page = this.buffer.getPage(pageId);
        if (shouldInsertInPage(table, page, pkIndex, incomingPk)) {
            int insertIndex = findInsertIndexInPage(table, page, pkIndex, incomingPk);
            insertIntoPageOrSplit(table, prevPageId, pageId, insertIndex, incomingBytes, pkIndex, incomingPk);
            updateUniqueIndexes(table, values, pageId);
            return true;
        }
        prevPageId = pageId;
        pageId = page.getNextPage();
    }
    return true;
}
/**
 * Updates all unique-column B+ tree indexes after a successful insertion.
 */
private void updateUniqueIndexes(TableSchema table, Object[] values, int insertedPageId) {
    for (AttributeSchema attr : table.getAttributes()) {
        if (attr.isUnique()) {
            int idx = table.getAttributeIndex(attr.getName());
            Object value = values[idx];
            String key = table.getName() + "." + attr.getName();
            BPlusTree tree = uniqueIndexes.get(key);
            if (tree != null) {
                tree.insert(value, insertedPageId);
            }
        }
    }
}
/**
 * Walks the page chain to find the page immediately before targetPageId.
 * Returns -1 if targetPageId is the head page.
 */
private int findPrevPageId(TableSchema table, int targetPageId) {
    int prevPageId = -1;
    int currPageId = table.getHeadPageId();
    while (currPageId != -1 && currPageId != targetPageId) {
        prevPageId = currPageId;
        currPageId = buffer.getPage(currPageId).getNextPage();
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
        int mid = candidateRecords.size() / 2;
        rewritePageRecords(leftPage, new ArrayList<>(candidateRecords.subList(0, mid)), rightPage.getPageID());
        rewritePageRecords(rightPage, new ArrayList<>(candidateRecords.subList(mid, candidateRecords.size())), nextPageId);

        if (prevPageId == -1) {
            table.setHeadPageId(leftPage.getPageID());
        } else {
            Page prevPage = this.buffer.getPage(prevPageId);
            prevPage.setNextPage(leftPage.getPageID());
        }

        freePage(page);
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