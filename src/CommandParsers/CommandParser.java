package CommandParsers;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class CommandParser {
    
    // CREATE TABLE <tableName> ( <attr> <type> <constraints>, ... );
    public static ParsedCommand parseCreate(String cmd) {
        Pattern p = Pattern.compile(
            "CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.+)\\)",
            Pattern.CASE_INSENSITIVE
        );
        Matcher m = p.matcher(cmd);
        
        if (!m.matches()) {
            return new ParsedCommand(false, "Invalid CREATE TABLE syntax");
        }
        
        String tableName = m.group(1);
        String columnsStr = m.group(2);
        
        List<Column> columns = parseColumns(columnsStr);
        if (columns == null || columns.isEmpty()) {
            return new ParsedCommand(false, "No valid columns found");
        }
        
        // Validate: must have exactly one primary key
        long pkCount = columns.stream().filter(c -> c.primaryKey).count();
        if (pkCount != 1) {
            return new ParsedCommand(false, "Table must have exactly one PRIMARY KEY");
        }
        
        ParsedCommand result = new ParsedCommand(true, "CREATE");
        result.tableName = tableName;
        result.columns = columns;
        return result;
    }
    
    // SELECT * FROM <tableName>;
    public static ParsedCommand parseSelect(String cmd) {
        Pattern p = Pattern.compile(
            "SELECT\\s+\\*\\s+FROM\\s+(\\w+)",
            Pattern.CASE_INSENSITIVE
        );
        Matcher m = p.matcher(cmd);
        
        if (!m.matches()) {
            return new ParsedCommand(false, "Invalid SELECT syntax. Expected: SELECT * FROM <tableName>");
        }
        
        ParsedCommand result = new ParsedCommand(true, "SELECT");
        result.tableName = m.group(1);
        return result;
    }
    
    // INSERT <tableName> VALUES ( <row1>, <row2>, ... );
    public static ParsedCommand parseInsert(String cmd) {
        Pattern p = Pattern.compile(
            "INSERT\\s+(\\w+)\\s+VALUES\\s*\\((.+)\\)",
            Pattern.CASE_INSENSITIVE
        );
        Matcher m = p.matcher(cmd);
        
        if (!m.matches()) {
            return new ParsedCommand(false, "Invalid INSERT syntax. Expected: INSERT <tableName> VALUES (...)");
        }
        
        String tableName = m.group(1);
        String valuesStr = m.group(2);
        
        List<String> values = parseValues(valuesStr);
        
        ParsedCommand result = new ParsedCommand(true, "INSERT");
        result.tableName = tableName;
        result.values = values;
        return result;
    }
    
    // DROP TABLE <tableName>;
    public static ParsedCommand parseDrop(String cmd) {
        Pattern p = Pattern.compile(
            "DROP\\s+TABLE\\s+(\\w+)",
            Pattern.CASE_INSENSITIVE
        );
        Matcher m = p.matcher(cmd);
        
        if (!m.matches()) {
            return new ParsedCommand(false, "Invalid DROP syntax. Expected: DROP TABLE <tableName>");
        }
        
        ParsedCommand result = new ParsedCommand(true, "DROP");
        result.tableName = m.group(1);
        return result;
    }
    
    // ALTER TABLE <tableName> ADD/DROP ...
    public static ParsedCommand parseAlter(String cmd) {
        // ALTER TABLE <tableName> ADD <attrName> <type> [NOTNULL] [DEFAULT <value>]
        Pattern addPattern = Pattern.compile(
            "ALTER\\s+TABLE\\s+(\\w+)\\s+ADD\\s+(\\w+)\\s+(.+)",
            Pattern.CASE_INSENSITIVE
        );
        
        // ALTER TABLE <tableName> DROP <attrName>
        Pattern dropPattern = Pattern.compile(
            "ALTER\\s+TABLE\\s+(\\w+)\\s+DROP\\s+(\\w+)",
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher addMatcher = addPattern.matcher(cmd);
        Matcher dropMatcher = dropPattern.matcher(cmd);
        
        if (addMatcher.matches()) {
            String tableName = addMatcher.group(1);
            String attrName = addMatcher.group(2);
            String rest = addMatcher.group(3);
            
            // Parse type and constraints from rest
            Column col = parseAlterAddColumn(attrName, rest);
            if (col == null) {
                return new ParsedCommand(false, "Invalid ALTER TABLE ADD syntax");
            }
            
            ParsedCommand result = new ParsedCommand(true, "ALTER_ADD");
            result.tableName = tableName;
            result.column = col;
            return result;
            
        } else if (dropMatcher.matches()) {
            ParsedCommand result = new ParsedCommand(true, "ALTER_DROP");
            result.tableName = dropMatcher.group(1);
            result.attrName = dropMatcher.group(2);
            return result;
            
        } else {
            return new ParsedCommand(false, "Invalid ALTER TABLE syntax");
        }
    }
    
    // Parse columns from CREATE TABLE
    private static List<Column> parseColumns(String columnsStr) {
        List<Column> columns = new ArrayList<>();
        String[] parts = smartSplit(columnsStr, ',');
        
        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty()) continue;
            
            Column col = parseColumn(part);
            if (col == null) return null;
            columns.add(col);
        }
        
        return columns;
    }
    
    // Parse single column definition
    private static Column parseColumn(String def) {
        def = def.trim();
        String[] tokens = def.split("\\s+");
        
        if (tokens.length < 2) return null;
        
        String name = tokens[0];
        String typeStr = tokens[1];
        
        // Extract base type (handle CHAR(5), VARCHAR(10), etc.)
        String type;
        if (typeStr.contains("(")) {
            type = typeStr.substring(0, typeStr.indexOf('('));
        } else {
            type = typeStr;
        }
        type = parseType(type);
        if (type == null) return null;
        
        // Check for constraints
        String upper = def.toUpperCase();
        boolean primaryKey = upper.contains("PRIMARYKEY");
        boolean notNull = upper.contains("NOTNULL");
        
        return new Column(name, type, primaryKey, notNull, null);
    }
    
    // Parse column for ALTER TABLE ADD
    private static Column parseAlterAddColumn(String name, String rest) {
        rest = rest.trim();
        String[] tokens = rest.split("\\s+");
        
        if (tokens.length < 1) return null;
        
        String typeStr = tokens[0];
        String type = parseType(typeStr);
        if (type == null) return null;
        
        // Check for NOTNULL and DEFAULT
        String upper = rest.toUpperCase();
        boolean notNull = upper.contains("NOTNULL");
        String defaultValue = null;
        
        if (upper.contains("DEFAULT")) {
            // Extract default value
            Pattern p = Pattern.compile("DEFAULT\\s+(.+)", Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(rest);
            if (m.find()) {
                defaultValue = m.group(1).trim();
            }
        }
        
        return new Column(name, type, false, notNull, defaultValue);
    }
    
    // Parse data type (INTEGER, DOUBLE, CHAR(n), VARCHAR(n))
    private static String parseType(String typeStr) {
        String upper = typeStr.toUpperCase();
        upper = upper.replaceAll("(PRIMARYKEY|NOTNULL).*", "").trim();
        
        if (upper.equals("INTEGER") || upper.equals("DOUBLE")) {
            return upper;
        } else if (upper.matches("CHAR\\(\\d+\\)")) {
            return upper;
        } else if (upper.matches("VARCHAR\\(\\d+\\)")) {
            return upper;
        }
        
        return null;
    }
    
    // Parse VALUES for INSERT
    private static List<String> parseValues(String valuesStr) {
        List<String> values = new ArrayList<>();
        String[] parts = smartSplit(valuesStr, ',');
        
        for (String part : parts) {
            values.add(part.trim());
        }
        
        return values;
    }
    
    // Smart split that respects parentheses
    private static String[] smartSplit(String str, char delimiter) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        
        for (char c : str.toCharArray()) {
            if (c == '(') {
                depth++;
                current.append(c);
            } else if (c == ')') {
                depth--;
                current.append(c);
            } else if (c == delimiter && depth == 0) {
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
    
    // Data classes
    
    public static class ParsedCommand {
        public boolean success;
        public String commandType;
        public String errorMessage;
        
        public String tableName;
        public List<Column> columns;
        public Column column;
        public String attrName;
        public List<String> values;
        
        public ParsedCommand(boolean success, String typeOrError) {
            this.success = success;
            if (success) {
                this.commandType = typeOrError;
            } else {
                this.errorMessage = typeOrError;
            }
        }
        
        @Override
        public String toString() {
            if (!success) {
                return "ERROR: " + errorMessage;
            }
            
            StringBuilder sb = new StringBuilder();
            sb.append(commandType).append(" command\n");
            sb.append("  Table: ").append(tableName).append("\n");
            
            if (columns != null) {
                sb.append("  Columns:\n");
                for (Column col : columns) {
                    sb.append("    - ").append(col).append("\n");
                }
            }
            
            if (column != null) {
                sb.append("  Column: ").append(column).append("\n");
            }
            
            if (attrName != null) {
                sb.append("  Attribute: ").append(attrName).append("\n");
            }
            
            if (values != null) {
                sb.append("  Values: ").append(values).append("\n");
            }
            
            return sb.toString();
        }
    }
    
    public static class Column {
        public String name;
        public String type;
        public boolean primaryKey;
        public boolean notNull;
        public String defaultValue;
        
        public Column(String name, String type, boolean primaryKey, boolean notNull, String defaultValue) {
            this.name = name;
            this.type = type;
            this.primaryKey = primaryKey;
            this.notNull = notNull;
            this.defaultValue = defaultValue;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append(" ").append(type);
            if (primaryKey) sb.append(" PRIMARYKEY");
            if (notNull) sb.append(" NOTNULL");
            if (defaultValue != null) sb.append(" DEFAULT ").append(defaultValue);
            return sb.toString();
        }
    }
}