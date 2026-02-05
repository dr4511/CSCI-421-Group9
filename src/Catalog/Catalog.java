package Catalog;

import java.util.HashMap;

public class Catalog {

    private HashMap<String, TableSchema> tables;

    public Catalog() {
        this.tables = new HashMap<>();
    }

    public boolean addTable(TableSchema table)
    {
        if (tables.containsKey(table.getName())) {
            return false;
        }

        tables.put(table.getName(), table);
        return true;
    }

    public boolean dropTable(String tableName)
    {
        String name = tableName.toLowerCase();
        if (tables.containsKey(name) == false) {
            return false;
        }

        tables.remove(name);
        return true;
    }

    // todo: serialize
}
