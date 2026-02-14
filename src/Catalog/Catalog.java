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

    public static Catalog initialize(String catalogPath, int pageSize, boolean indexing) throws Exception {
        File catalogFile = new File(catalogPath);
        Catalog catalog;

        System.out.println("Accessing database location...");

        if (catalogFile.exists()) {
            System.out.println("Database found. Restarting database...");
            catalog = loadFromFile(catalogPath);
            System.out.println("Ignoring provided page size. Using prior size of " + catalog.getPageSize() + "...");
        } else {
            File parent = catalogFile.getParentFile();
            if (parent != null &&
                parent.exists() == false &&
                parent.mkdirs() == false) {
                throw new Exception("Failed to create database directory");
            }
            
            catalog = new Catalog(pageSize, indexing);
            System.out.println("No database found. Creating new database...");
        }
        return catalog;
    }

    private Catalog(int pageSize, boolean indexing) {
        this.tables = new HashMap<>();
        this.freePageListHead = -1;
        this.pageSize = pageSize;
        this.indexing = indexing;
    }

    private Catalog() {
        this.tables = new LinkedHashMap<>();
    }

    public boolean addTable(TableSchema table) {
        String name = table.getName();
        if (tables.containsKey(name)) {
            return false;
        }
        tables.put(name, table);
        return true;
    }

    public TableSchema getTable(String name) {
        return tables.get(name.toLowerCase());
    }

    public boolean dropTable(String name) {
        return tables.remove(name.toLowerCase()) != null;
    }

    public boolean tableExists(String name) {
        return tables.containsKey(name.toLowerCase());
    }

    public List<TableSchema> getAllTables() {
        return tables.values().stream().toList();
    }

    public int getFreePageListHead() {
        return freePageListHead;
    }

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

    public static Catalog loadFromFile(String path) throws IOException {
        Catalog catalog = new Catalog();
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))) {
            catalog.pageSize = in.readInt();
            catalog.indexing = in.readBoolean();
            catalog.freePageListHead = in.readInt();
            catalog.lastPageId = in.readInt();

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