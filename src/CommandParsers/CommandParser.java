package CommandParsers;

import Catalog.AttributeSchema;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.DataType;
import StorageManager.StorageManager;
import java.util.ArrayList;
import java.util.List;

public class CommandParser {
    private final List<Token> tokens;
    private int pos;  // current index into the token list

    private final Catalog catalog;
    private final StorageManager storageManager;

    public CommandParser(List<Token> tokens, Catalog catalog, StorageManager storageManager) {
        this.tokens = tokens;
        this.catalog = catalog;
        this.storageManager = storageManager;
        this.pos = 0;
    }

    /**
     * Inspects the first token and delegates to the matching statement parser.
     */
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

        // Enforce one primary key across columns
        int pkCount = 0;
        for (AttributeSchema a : table.getAttributes()) {
            if (a.isPrimaryKey()) {
                pkCount++;
            }
        }
        if (pkCount != 1) {
            throw new Exception("Table must have exactly one PRIMARY KEY");
        }

        storageManager.createTable(table);
        System.out.println("Table created successfully");
    }

    /**
     * Parses a single column definition.
     */
    private void parseAttributeDef(TableSchema table) throws Exception {
        String attrName = consumeWord();
        DataType dataType = parseDataType();
        boolean isPK = false;
        boolean isNN = false;

        // Consume trailing constraint keywords until a comma, closing parenthesis, or end of input.
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
                    break; // not a constraint, next column def
                }
            }
        }

        AttributeSchema attr = new AttributeSchema(attrName, dataType, isPK, isNN, null);
        if (table.addAttribute(attr) == false) {
            throw new Exception("Duplicate attribute name: " + attrName);
        }
    }

    /**
     * Parses a data type definition.
     * @return the parsed DataType object
     */
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

        storageManager.selectAllTable(table);

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

            // Column count must match schema
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
            // storageManager.insertRecord(table, values);

            rowNum++;

            // Advance past comma to next row, or stop at closing paren
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

    /**
     * Converts a token to the appropriate Java type based on the attribute schema, and validates constraints.
     * @param token the token representing the value to convert
     * @param attr the attribute schema defining the expected type and constraints
     */
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

                // CHAR enforces an exact max length
                if (token.value.length() > type.getMaxLength()) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be " + type.getMaxLength() + " characters");
                }
                return token.value;

            case VARCHAR:
                if (token.type != Token.Type.STRING) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be a string value");
                }

                // VARCHAR enforces an upper limit on length
                if (token.value.length() > type.getMaxLength()) {
                    throw new Exception("Error: Attribute '" + attrName + "' must be between 0 and " + type.getMaxLength() + " characters");
                }
                return token.value;

            default:
                throw new Exception("Error: Unknown type for attribute '" + attrName + "'");
        }
    }

    // DROP TABLE <tableName>;
    private void parseDropTable() throws Exception{
        expectKeyword("DROP");
        expectKeyword("TABLE");
        
        // Get tablename, check if exists
        String tableName = consumeWord();
        TableSchema oldTable = catalog.getTable(tableName);
        if (oldTable == null) {
            throw new Exception("No such table: " + tableName);
        }
        expectEnd();

        // Get tableSchema class representing table to drop
        TableSchema toDrop = catalog.getTable(tableName);
        storageManager.dropTable(toDrop);
        System.out.println("Table dropped successfully");
    }

    // ALTER TABLE <tableName> ADD/DROP ... by sending new and old table schema to storage manager
    private void parseAlterTable() throws Exception{
        expectKeyword("ALTER");
        expectKeyword("TABLE");

        String tableName = consumeWord();
        TableSchema oldTable = catalog.getTable(tableName);
        if (oldTable == null) {
            throw new Exception("No such table: " + tableName);
        }

        // add or drop
        expect(Token.Type.WORD);
        String action = tokens.get(pos - 1).value;

        TableSchema newTable;
        if (action.equals("ADD")) {
            newTable = parseAlterAdd(oldTable);
        } else if (action.equals("DROP")) {
            newTable = parseAlterDrop(oldTable);
        } else {
            throw new Exception("Expected ADD or DROP but got '" + action + "'");
        }

        storageManager.alterTablePages(oldTable, newTable);
        
    }

    // ALTER TABLE <tableName> ADD <attrName> <type> [NOTNULL] [DEFAULT <value>]    
    private TableSchema parseAlterAdd(TableSchema table) {
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
        return null;
    }

    // ALTER TABLE <tableName> DROP <attrName>
    private TableSchema parseAlterDrop(TableSchema table) {
        return null;
    }

    /**
     * @return the next token without consuming it, or null if at end of input
     */
    private Token peek() {
        if (pos >= tokens.size())
            return null;

        return tokens.get(pos);
    }

    /**
     * @return the next token and advances the position.
     */
    private Token consume() throws Exception {
        if (pos >= tokens.size()) {
            throw new Exception("Unexpected end of command");
        }

        return tokens.get(pos++);
    }

    /**
     * @return the next token if it is a word matching the expected keyword, and advances the position.
     */
    private void expectKeyword(String keyword) throws Exception {
        Token t = consume();
        if (t.type != Token.Type.WORD || t.value.equals(keyword) == false) {
            throw new Exception("Expected '" + keyword + "' but got '" + t.value + "'");
        }
    }

    /**
     * @return the next token if it matches the expected type, and advances the position.
     */
    private void expect(Token.Type type) throws Exception {
        Token t = consume();
        if (t.type != type) {
            throw new Exception("Expected " + type + " but got '" + t.value + "'");
        }
    }

    /**
     * @return the next token if it is a word, and advances the position.
     */
    private String consumeWord() throws Exception {
        Token t = consume();
        if (t.type != Token.Type.WORD) {
            throw new Exception("Expected a name but got '" + t.value + "'");
        }

        return t.value;
    }

    /**
     * @return true if there are no more tokens after the current position, false otherwise.
     */
    private void expectEnd() throws Exception {
        if (pos < tokens.size()) {
            throw new Exception("Unexpected token after command: '" + tokens.get(pos).value + "'");
        }
    }
}