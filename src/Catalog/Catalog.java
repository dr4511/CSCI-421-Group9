package Catalog;

import Common.DataType;
import java.io.*;
import java.util.*;

public class Catalog {

    private final HashMap<String, TableSchema> tables;
    private int freePageListHead;
    private int pageSize;
    private boolean indexing;
    private int lastPageId;

    /**
     * Constructor for loading a catalog from an existing file.
     */
    private Catalog(int pageSize, boolean indexing, int freePageListHead, int lastPageId) {
        this.tables = new HashMap<>();
        this.freePageListHead = freePageListHead;
        this.pageSize = pageSize;
        this.indexing = indexing;
        this.lastPageId = lastPageId;
    }

    /**
     * Constructor for creating a new catalog.
     */
    private Catalog(int pageSize, boolean indexing) {
        this(pageSize, indexing, -1, -1);
    }

    /**
     * Adds a table to the catalog.
     * @return false if a table with the same name already exists in the catalog.
     */
    public boolean addTable(TableSchema table) {
        String name = table.getName();
        if (tables.containsKey(name)) {
            return false;
        }
        tables.put(name, table);
        return true;
    }

    /**
     * @return the TableSchema for the table with the given name, or null if no table exists.
     */
    public TableSchema getTable(String name) {
        return tables.get(name.toLowerCase());
    }

    /**
     * Removes the table with the given name from the catalog.
     * @return false if no table with the given name exists in the catalog.
     */
    public boolean dropTable(String name) {
        return tables.remove(name.toLowerCase()) != null;
    }

    /**
     * @return true if a table with the given name exists in the catalog, false otherwise.
     */
    public boolean tableExists(String name) {
        return tables.containsKey(name.toLowerCase());
    }

    /**
     * @return a list of all tables in the catalog.
     */
    public List<TableSchema> getAllTables() {
        return tables.values().stream().toList();
    }

    /**
     * @return the page ID of the head of the free page list, or -1 if the free page list is empty.
     */
    public int getFreePageListHead() {
        return freePageListHead;
    }

    /**
     * Sets the page ID of the head of the free page list.
     */
    public void setFreePageListHead(int pageId) {
        this.freePageListHead = pageId;
    }

    public int getPageSize() {
        return pageSize;
    }


    public boolean isIndexing() {
        return indexing;
    }

    public int getLastPageId() {
        return this.lastPageId;
    }

    public void setLastPageId(int lastPageId) {
        this.lastPageId = lastPageId;
    }

    /**
     * Saves the catalog to a file at the given path.
     */
    public void saveToFile(String path) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)))) {
            out.writeInt(pageSize);
            out.writeBoolean(indexing);
            out.writeInt(freePageListHead);
            out.writeInt(lastPageId);

            out.writeInt(tables.size());
            for (TableSchema table : tables.values()) {
                writeTable(out, table);
            }
        }
    }

    /**
     * Initializes a Catalog by loading from the file at the given path, or creating a new Catalog if no file exists.
     * @return the initialized Catalog
     */
    public static Catalog initialize(String catalogPath, int pageSize, boolean indexing) throws Exception {
        File catalogFile = new File(catalogPath);

        if (catalogFile.exists()) {
            return loadFromFile(catalogPath);
        }

        File parent = catalogFile.getParentFile();
        if (parent != null &&
            parent.exists() == false &&
            parent.mkdirs() == false) {
            throw new Exception("Failed to create database directory");
        }

        return new Catalog(pageSize, indexing);
    }

    /**
     * Loads a Catalog from the file at the given path.
     * @return the loaded Catalog
     */
    private static Catalog loadFromFile(String path) throws IOException {
        Catalog catalog;
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))) {
            int pageSize = in.readInt();
            boolean indexing = in.readBoolean();
            int freePageListHead = in.readInt();
            int lastPageId = in.readInt();
            catalog = new Catalog(pageSize, indexing, freePageListHead, lastPageId);

            int tableCount = in.readInt();
            for (int i = 0; i < tableCount; i++) {
                TableSchema table = readTable(in);
                catalog.tables.put(table.getName(), table);
            }
        }
        return catalog;
    }

    private static void writeTable(DataOutputStream out, TableSchema table) throws IOException {
        out.writeUTF(table.getName());
        out.writeInt(table.getHeadPageId());

        List<AttributeSchema> attrs = table.getAttributes();
        out.writeInt(attrs.size());
        for (AttributeSchema attr : attrs) {
            writeAttribute(out, attr);
        }
    }

    private static TableSchema readTable(DataInputStream in) throws IOException {
        String name = in.readUTF();
        TableSchema table = new TableSchema(name);
        table.setHeadPageId(in.readInt());

        int attrCount = in.readInt();
        for (int i = 0; i < attrCount; i++) {
            table.addAttribute(readAttribute(in));
        }
        return table;
    }

    private static void writeAttribute(DataOutputStream out, AttributeSchema attr) throws IOException {
        out.writeUTF(attr.getName());

        DataType dataType = attr.getDataType();
        out.writeUTF(dataType.getType().name());
        out.writeInt(dataType.getMaxLength());

        out.writeBoolean(attr.isPrimaryKey());
        out.writeBoolean(attr.isNotNull());

        writeDefaultValue(out, dataType, attr.getDefaultValue());
    }

    private static AttributeSchema readAttribute(DataInputStream in) throws IOException {
        String name = in.readUTF();

        DataType.Type type = DataType.Type.valueOf(in.readUTF());
        int maxLength = in.readInt();

        DataType dataType;
        if (type == DataType.Type.CHAR || type == DataType.Type.VARCHAR) {
            dataType = new DataType(type, maxLength);
        } else {
            dataType = new DataType(type);
        }

        boolean isPK = in.readBoolean();
        boolean isNN = in.readBoolean();
        Object defVal = readDefaultValue(in, dataType);

        return new AttributeSchema(name, dataType, isPK, isNN, defVal);
    }

    private static void writeDefaultValue(DataOutputStream out, DataType dataType, Object value) throws IOException {
        out.writeBoolean(value != null);
        if (value == null) return;

        switch (dataType.getType()) {
            case INTEGER:
                out.writeInt((int) value);
                break;
            case DOUBLE:
                out.writeDouble((double) value);
                break;
            case BOOLEAN:
                out.writeBoolean((boolean) value);
                break;
            case CHAR:
            case VARCHAR:
                out.writeUTF((String) value);
                break;
        }
    }

    private static Object readDefaultValue(DataInputStream in, DataType dataType) throws IOException {
        if (in.readBoolean() == false)
            return null;

        switch (dataType.getType()) {
            case INTEGER:
                return in.readInt();
            case DOUBLE:
                return in.readDouble();
            case BOOLEAN:
                return in.readBoolean();
            case CHAR:
            case VARCHAR:
                return in.readUTF();
            default:
                return null;
        }
    }
}