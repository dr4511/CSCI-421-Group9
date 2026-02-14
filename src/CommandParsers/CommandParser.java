package CommandParsers;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.DataType;
import java.util.ArrayList;
import java.util.List;

public class CommandParser {
    private final List<Token> tokens;
    private final Catalog catalog;
    private int pos;

    public CommandParser(List<Token> tokens, Catalog catalog) {
        this.tokens = tokens;
        this.catalog = catalog;
        this.pos = 0;
    }

    public void parse() throws Exception {
        Token first = peek();

        // empty
        if (first == null)
            return;

        if (first.type != Token.Type.WORD)
            throw new Exception("Expected a command, got '" + first.value + "'");

        switch (first.value) {
            case "CREATE":
                parseCreateTable();
                break;
            case "SELECT":
                parseSelect();
                break;
            case "INSERT":
                parseInsert();
                break;
            case "DROP":
                parseDropTable();
                break;
            case "ALTER":
                parseAlterTable();
                break;
            default:
                throw new Exception("Unknown command: " + first.value);
        }
    }

    // CREATE TABLE <tableName> ( <attr> <type> <constraints>, ... );
    private void parseCreateTable() throws Exception {
        expectKeyword("CREATE");
        expectKeyword("TABLE");
        String tableName = consumeWord();

        if (catalog.tableExists(tableName)) {
            throw new Exception("Table already exists: " + tableName);
        }

        expect(Token.Type.LPAREN);

        TableSchema table = new TableSchema(tableName);

        parseAttributeDef(table);
        // parse remaining attributes separated by commas
        while (peek() != null && peek().type == Token.Type.COMMA) {
            consume();
            parseAttributeDef(table);
        }

        expect(Token.Type.RPAREN);
        expectEnd();

        int pkCount = 0;
        for (AttributeSchema a : table.getAttributes()) {
            if (a.isPrimaryKey()) {
                pkCount++;
            }
        }
        if (pkCount != 1) {
            throw new Exception("Table must have exactly one PRIMARY KEY");
        }

        catalog.addTable(table);
        System.out.println("Table created successfully");
    }

    private void parseAttributeDef(TableSchema table) throws Exception {
        String attrName = consumeWord();
        DataType dataType = parseDataType();
        boolean isPK = false;
        boolean isNN = false;

        while (peek() != null && peek().type == Token.Type.WORD) {
            Token t = peek();
            if (t != null && t.type == Token.Type.WORD) {
                if (t.value.equals("PRIMARYKEY")) {
                    consume();
                    isPK = true;
                    isNN = true;
                } else if (t.value.equals("NOTNULL")) {
                    consume();
                    isNN = true;
                } else {
                    break;
                }
            }
        }

        AttributeSchema attr = new AttributeSchema(attrName, dataType, isPK, isNN, null);
        if (table.addAttribute(attr) == false) {
            throw new Exception("Duplicate attribute name: " + attrName);
        }
    }

    private DataType parseDataType() throws Exception {
        String typeName = consumeWord();

        switch (typeName) {
            case "INTEGER":
                return new DataType(DataType.Type.INTEGER);
            case "DOUBLE":
                return new DataType(DataType.Type.DOUBLE);
            case "BOOLEAN":
                return new DataType(DataType.Type.BOOLEAN);
            case "CHAR":
            case "VARCHAR": {
                expect(Token.Type.LPAREN);

                Token n = consume();
                if (n.type != Token.Type.NUMBER || n.value.contains(".")) {
                    throw new Exception("Expected integer size for " + typeName);
                }

                int size = Integer.parseInt(n.value);
                if (size <= 0) {
                    throw new Exception(typeName + " size must be positive");
                }

                expect(Token.Type.RPAREN);
                DataType.Type dataType = typeName.equals("CHAR") ? DataType.Type.CHAR : DataType.Type.VARCHAR;
                return new DataType(dataType, size);
            }
            default:
                throw new Exception("Unknown data type: " + typeName);
        }
    }
    
    // SELECT * FROM <tableName>;
    private void parseSelect() throws Exception {
        expectKeyword("SELECT");
        expect(Token.Type.STAR);
        expectKeyword("FROM");
        String tableName = consumeWord();
        expectEnd();

        TableSchema table = catalog.getTable(tableName);
        if (table == null) {
            throw new Exception("No such table: " + tableName);
        }

        // read and print records from storage manager

        System.out.println("select test: " + table.getName());
    }

    // INSERT <tableName> VALUES ( <row1>, <row2>, ... );
    private void parseInsert() throws Exception {
        expectKeyword("INSERT");
        String tableName = consumeWord();

        TableSchema table = catalog.getTable(tableName);
        if (table == null) {
            throw new Exception("No such table: " + tableName);
        }

        expectKeyword("VALUES");
        expect(Token.Type.LPAREN);

        List<AttributeSchema> attrs = table.getAttributes();
        int rowNum = 0;
        boolean moreRows = true;

        while (moreRows) {
            // Collect one row of values until ','' or ')'
            List<Token> rowTokens = new ArrayList<>();
            while (peek() != null && peek().type != Token.Type.COMMA && peek().type != Token.Type.RPAREN) {
                rowTokens.add(consume());
            }

            if (rowTokens.size() != attrs.size()) {
                throw new Exception("Row " + rowNum + ": expected " + attrs.size() + " values but got " + rowTokens.size());
            }

            // Validate value type
            Object[] values = new Object[attrs.size()];
            for (int i = 0; i < attrs.size(); i++) {
                values[i] = convertValue(rowTokens.get(i), attrs.get(i));
            }

            // check primary key uniqueness

            // insert record into storage manager

            rowNum++;

            if (peek() != null && peek().type == Token.Type.COMMA) {
                consume();
            } else {
                moreRows = false;
            }
        }

        expect(Token.Type.RPAREN);
        expectEnd();

        System.out.println("Inserted " + rowNum + " rows successfully");
    }

    private Object convertValue(Token token, AttributeSchema attr) throws Exception {
        DataType type = attr.getDataType();
        String attrName = attr.getName();

        // Handle null
        if (token.type == Token.Type.WORD && token.value.equalsIgnoreCase("null")) {
            if (attr.isNotNull()) {
                throw new Exception("Error: Attribute '" + attrName + "' cannot be null");
            }
            return null;
        }

        // convert Object to its actual type and validate constraints
        switch (type.getType()) {
            case INTEGER:
                if (token.type != Token.Type.NUMBER || token.value.contains(".")) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be an integer value");
                }

                return Integer.valueOf(token.value);

            case DOUBLE:
                if (token.type != Token.Type.NUMBER) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be a double value");
                }

                return Double.valueOf(token.value);

            case BOOLEAN:
                if (token.type != Token.Type.WORD ||
                    (token.value.equals("True") == false && token.value.equals("False") == false)) {
                    String errMsg = "Error: Attribute '" + attrName + "' must be True/False";
                    if (attr.isNotNull() == false) {
                        errMsg += " or NULL";
                    }
                    throw new Exception(errMsg);
                }

                return token.value.equals("True");

            case CHAR:
                if (token.type != Token.Type.STRING) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be a string value");
                }

                if (token.value.length() > type.getMaxLength()) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be " + type.getMaxLength() + " characters");
                }
                return token.value;

            case VARCHAR:
                if (token.type != Token.Type.STRING) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be a string value");
                }

                if (token.value.length() > type.getMaxLength()) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be between 0 and " + type.getMaxLength() + " characters");
                }
                return token.value;

            default:
                throw new Exception("Error: Unknown type for attribute '" + attrName + "'");
        }
    }

    // DROP TABLE <tableName>;
    private void parseDropTable() {
        // Pattern p = Pattern.compile(
        //     "DROP\\s+TABLE\\s+(\\w+)",
        //     Pattern.CASE_INSENSITIVE
        // );
        // Matcher m = p.matcher(cmd);
        
        // if (!m.matches()) {
        //     return new ParsedCommand(false, "Invalid DROP syntax. Expected: DROP TABLE <tableName>");
        // }
        
        // ParsedCommand result = new ParsedCommand(true, "DROP");
        // result.tableName = m.group(1);
        // return result;
        System.out.println("Table dropped successfully");
    }

    // ALTER TABLE <tableName> ADD/DROP ...
    private void parseAlterTable() {
        // // ALTER TABLE <tableName> ADD <attrName> <type> [NOTNULL] [DEFAULT <value>]
        // Pattern addPattern = Pattern.compile(
        //     "ALTER\\s+TABLE\\s+(\\w+)\\s+ADD\\s+(\\w+)\\s+(.+)",
        //     Pattern.CASE_INSENSITIVE
        // );
        
        // // ALTER TABLE <tableName> DROP <attrName>
        // Pattern dropPattern = Pattern.compile(
        //     "ALTER\\s+TABLE\\s+(\\w+)\\s+DROP\\s+(\\w+)",
        //     Pattern.CASE_INSENSITIVE
        // );
        
        // Matcher addMatcher = addPattern.matcher(cmd);
        // Matcher dropMatcher = dropPattern.matcher(cmd);
        
        // if (addMatcher.matches()) {
        //     String tableName = addMatcher.group(1);
        //     String attrName = addMatcher.group(2);
        //     String rest = addMatcher.group(3);
            
        //     // Parse type and constraints from rest
        //     Column col = parseAlterAddColumn(attrName, rest);
        //     if (col == null) {
        //         return new ParsedCommand(false, "Invalid ALTER TABLE ADD syntax");
        //     }
            
        //     ParsedCommand result = new ParsedCommand(true, "ALTER_ADD");
        //     result.tableName = tableName;
        //     result.column = col;
        //     return result;
            
        // } else if (dropMatcher.matches()) {
        //     ParsedCommand result = new ParsedCommand(true, "ALTER_DROP");
        //     result.tableName = dropMatcher.group(1);
        //     result.attrName = dropMatcher.group(2);
        //     return result;
            
        // } else {
        //     return new ParsedCommand(false, "Invalid ALTER TABLE syntax");
        // }
        System.out.println("Table altered successfully");
    }

    // ALTER TABLE <tableName> ADD <attrName> <type> [NOTNULL] [DEFAULT <value>]    
    private void parseAlterAdd(TableSchema table) {
        //rest = rest.trim();
        // String[] tokens = rest.split("\\s+");
        
        // if (tokens.length < 1) return null;
        
        // String typeStr = tokens[0];
        // String type = parseType(typeStr);
        // if (type == null) return null;
        
        // // Check for NOTNULL and DEFAULT
        // String upper = rest.toUpperCase();
        // boolean notNull = upper.contains("NOTNULL");
        // String defaultValue = null;
        
        // if (upper.contains("DEFAULT")) {
        //     // Extract default value
        //     Pattern p = Pattern.compile("DEFAULT\\s+(.+)", Pattern.CASE_INSENSITIVE);
        //     Matcher m = p.matcher(rest);
        //     if (m.find()) {
        //         defaultValue = m.group(1).trim();
        //     }
        // }
    }

    // ALTER TABLE <tableName> DROP <attrName>
    private void parseAlterDrop(TableSchema table) {
        
    }

    private Token peek() {
        if (pos >= tokens.size())
            return null;

        return tokens.get(pos);
    }

    private Token consume() throws Exception {
        if (pos >= tokens.size()) {
            throw new Exception("Unexpected end of command");
        }

        return tokens.get(pos++);
    }

    private void expectKeyword(String keyword) throws Exception {
        Token t = consume();
        if (t.type != Token.Type.WORD || t.value.equals(keyword) == false) {
            throw new Exception("Expected '" + keyword + "' but got '" + t.value + "'");
        }
    }

    private void expect(Token.Type type) throws Exception {
        Token t = consume();
        if (t.type != type) {
            throw new Exception("Expected " + type + " but got '" + t.value + "'");
        }
    }

    private String consumeWord() throws Exception {
        Token t = consume();
        if (t.type != Token.Type.WORD) {
            throw new Exception("Expected a name but got '" + t.value + "'");
        }

        return t.value;
    }

    private void expectEnd() throws Exception {
        if (pos < tokens.size()) {
            throw new Exception("Unexpected token after command: '" + tokens.get(pos).value + "'");
        }
    }
}