package CommandParsers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class ParseCreate {
    
    /**
     * Handles CREATE TABLE command parsing and validation
     * @param command The full CREATE TABLE command string
     * @return true if successfully parsed, false otherwise
     */
    public static boolean handleCreateCommand(String command) {
        try {
            // Pattern: CREATE TABLE <tableName> ( <columns> )
            Pattern createPattern = Pattern.compile(
                "CREATE\\s+TABLE\\s+(\\w+)\\s*\\(\\s*(.+)\\s*\\)",
                Pattern.CASE_INSENSITIVE
            );
            
            Matcher matcher = createPattern.matcher(command);
            
            if (!matcher.matches()) {
                System.out.println("Error: Invalid CREATE TABLE syntax.");
                System.out.println("Expected: CREATE TABLE <tableName> ( <attr> <type> [constraints], ... )");
                return false;
            }
            
            String tableName = matcher.group(1);
            String columnsStr = matcher.group(2);
            
            // Validate table name
            if (!isValidIdentifier(tableName)) {
                System.out.println("Error: Invalid table name '" + tableName + "'. Must start with letter and contain only letters, numbers, and underscores.");
                return false;
            }
            
            // Parse columns
            List<ColumnDefinition> columns = parseColumns(columnsStr);
            
            if (columns == null || columns.isEmpty()) {
                System.out.println("Error: No valid columns defined.");
                return false;
            }
            
            // Validate column definitions
            if (!validateColumns(columns, tableName)) {
                return false;
            }
            
            // Success - print parsed information
            System.out.println("Successfully parsed CREATE TABLE command:");
            System.out.println("  Table: " + tableName);
            System.out.println("  Columns:");
            for (ColumnDefinition col : columns) {
                System.out.println("    - " + col);
            }
            
            return true;
            
        } catch (Exception e) {
            System.out.println("Error parsing CREATE command: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Parses column definitions from the columns string
     */
    private static List<ColumnDefinition> parseColumns(String columnsStr) {
        List<ColumnDefinition> columns = new ArrayList<>();
        
        // Split by comma (but be careful with CHAR(n) and VARCHAR(n))
        String[] columnTokens = splitColumns(columnsStr);
        
        for (String columnToken : columnTokens) {
            columnToken = columnToken.trim();
            if (columnToken.isEmpty()) {
                continue;
            }
            
            ColumnDefinition col = parseColumnDefinition(columnToken);
            if (col == null) {
                System.out.println("Error: Invalid column definition: " + columnToken);
                return null;
            }
            columns.add(col);
        }
        
        return columns;
    }
    
    /**
     * Splits columns by comma, respecting parentheses in type definitions
     */
    private static String[] splitColumns(String columnsStr) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int parenDepth = 0;
        
        for (char c : columnsStr.toCharArray()) {
            if (c == '(') {
                parenDepth++;
                current.append(c);
            } else if (c == ')') {
                parenDepth--;
                current.append(c);
            } else if (c == ',' && parenDepth == 0) {
                result.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        if (current.length() > 0) {
            result.add(current.toString());
        }
        
        return result.toArray(new String[0]);
    }
    
    /**
     * Parses a single column definition
     */
    private static ColumnDefinition parseColumnDefinition(String columnDef) {
        columnDef = columnDef.trim();
        
        // Pattern: <name> <type> [PRIMARYKEY] [NOTNULL]
        // Types: INTEGER, DOUBLE, CHAR(n), VARCHAR(n)
        String[] tokens = columnDef.split("\\s+");
        
        if (tokens.length < 2) {
            return null;
        }
        
        String name = tokens[0];
        
        if (!isValidIdentifier(name)) {
            System.out.println("Error: Invalid column name '" + name + "'");
            return null;
        }
        
        // Parse type (might include size like CHAR(5))
        String typeStr = tokens[1];
        DataType type = parseDataType(typeStr);
        
        if (type == null) {
            return null;
        }
        
        // Parse constraints
        boolean primaryKey = false;
        boolean notNull = false;
        
        // Check remaining tokens and the type string for constraints
        String remainingStr = columnDef.substring(name.length()).trim();
        remainingStr = remainingStr.substring(typeStr.length()).trim();
        
        // Also check if constraints are concatenated to type (e.g., "INTEGERPRIMARYKEY")
        String upperTypeDef = columnDef.toUpperCase();
        
        if (upperTypeDef.contains("PRIMARYKEY")) {
            primaryKey = true;
        }
        if (upperTypeDef.contains("NOTNULL")) {
            notNull = true;
        }
        
        return new ColumnDefinition(name, type, primaryKey, notNull);
    }
    
    /**
     * Parses data type from string
     */
    private static DataType parseDataType(String typeStr) {
        String upperType = typeStr.toUpperCase();
        
        // Remove any trailing constraints that might be concatenated
        upperType = upperType.replaceAll("(PRIMARYKEY|NOTNULL).*", "").trim();
        
        if (upperType.equals("INTEGER")) {
            return new DataType("INTEGER", -1);
        } else if (upperType.equals("DOUBLE")) {
            return new DataType("DOUBLE", -1);
        } else if (upperType.startsWith("CHAR(") && upperType.contains(")")) {
            int size = extractSize(upperType, "CHAR");
            if (size <= 0) {
                System.out.println("Error: Invalid CHAR size");
                return null;
            }
            return new DataType("CHAR", size);
        } else if (upperType.startsWith("VARCHAR(") && upperType.contains(")")) {
            int size = extractSize(upperType, "VARCHAR");
            if (size <= 0) {
                System.out.println("Error: Invalid VARCHAR size");
                return null;
            }
            return new DataType("VARCHAR", size);
        } else {
            System.out.println("Error: Unknown data type '" + typeStr + "'");
            System.out.println("Valid types: INTEGER, DOUBLE, CHAR(n), VARCHAR(n)");
            return null;
        }
    }
    
    /**
     * Extracts size from CHAR(n) or VARCHAR(n)
     */
    private static int extractSize(String typeStr, String typeName) {
        try {
            int start = typeStr.indexOf('(') + 1;
            int end = typeStr.indexOf(')');
            String sizeStr = typeStr.substring(start, end).trim();
            return Integer.parseInt(sizeStr);
        } catch (Exception e) {
            return -1;
        }
    }
    
    /**
     * Validates column definitions
     */
    private static boolean validateColumns(List<ColumnDefinition> columns, String tableName) {
        int primaryKeyCount = 0;
        List<String> columnNames = new ArrayList<>();
        
        for (ColumnDefinition col : columns) {
            // Check for duplicate column names
            if (columnNames.contains(col.name.toUpperCase())) {
                System.out.println("Error: Duplicate column name '" + col.name + "'");
                return false;
            }
            columnNames.add(col.name.toUpperCase());
            
            // Count primary keys
            if (col.primaryKey) {
                primaryKeyCount++;
            }
        }
        
        // Check primary key constraint
        if (primaryKeyCount == 0) {
            System.out.println("Error: Table must have exactly one PRIMARY KEY column");
            return false;
        }
        
        if (primaryKeyCount > 1) {
            System.out.println("Error: Table cannot have multiple PRIMARY KEY columns");
            return false;
        }
        
        return true;
    }
    
    /**
     * Validates identifier (table/column name)
     */
    private static boolean isValidIdentifier(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        
        // Must start with letter, contain only letters, numbers, underscores
        return name.matches("^[a-zA-Z][a-zA-Z0-9_]*$");
    }
    
    // Helper classes
    
    static class ColumnDefinition {
        String name;
        DataType type;
        boolean primaryKey;
        boolean notNull;
        
        ColumnDefinition(String name, DataType type, boolean primaryKey, boolean notNull) {
            this.name = name;
            this.type = type;
            this.primaryKey = primaryKey;
            this.notNull = notNull;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append(" ").append(type);
            if (primaryKey) sb.append(" PRIMARYKEY");
            if (notNull) sb.append(" NOTNULL");
            return sb.toString();
        }
    }
    
    static class DataType {
        String typeName;
        int size; // -1 for types without size
        
        DataType(String typeName, int size) {
            this.typeName = typeName;
            this.size = size;
        }
        
        @Override
        public String toString() {
            if (size > 0) {
                return typeName + "(" + size + ")";
            }
            return typeName;
        }
    }
}