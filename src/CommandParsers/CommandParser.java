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

    /**
     * Parses a CREATE TABLE statement of the form:
     * CREATE TABLE <tableName> (
     *   <attrName> <dataType> [PRIMARY KEY] [NOT NULL],
     *  ...
     * );
     * 
     * Validates that the table does not already exist, attribute names are unique,
     * data types are valid, and that there is one primary key.
     */
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
            throw new Exception("Error: Table must have exactly one PRIMARY KEY");
        }

        storageManager.createTable(table);
        // catalog.addTable(table);
        System.out.println("Table created successfully");
    }

    /**
     * Parses a single column definition.
     * 
     * Example: id INTEGER PRIMARYKEY, name VARCHAR(50) NOTNULL, age INTEGER
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
            throw new Exception("Error: Attribute name already exists");
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
                    throw new Exception("Error: Expected integer size for " + typeName);
                }

                int size = Integer.parseInt(n.value);
                if (size <= 0) {
                    throw new Exception("Error: " + typeName + " size must be positive");
                }

                expect(Token.Type.RPAREN);
                DataType.Type dataType = typeName.equals("CHAR") ? DataType.Type.CHAR : DataType.Type.VARCHAR;
                return new DataType(dataType, size);
            }
            default:
                throw new Exception("Error: Unknown data type: " + typeName);
        }
    }
    
    /**
     * Parses a SELECT statement of the form:
     * SELECT * FROM <tableName>;
     * 
     * Validates that the table exists, then retrieves all records
     * from the storage manager and prints them.
     */
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

        // System.out.println("select test: " + table);
    }

    /**
     * Parses an INSERT statement of the form:
     * INSERT <tableName> VALUES ( <value1>, <value2>, ... );
     * 
     * Validates that the number of values matches the table schema,
     * and that each value can be converted to the appropriate Java type
     * based on the attribute schema. Also checks NOT NULL constraints.
     */
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
                throw new Exception("Error: Row " + rowNum + ": expected " + attrs.size() + " values but got " + rowTokens.size());
            }

            // Validate value type
            Object[] values = new Object[attrs.size()];
            for (int i = 0; i < attrs.size(); i++) {
                values[i] = convertValue(rowTokens.get(i), attrs.get(i));
            }

            // check primary key uniqueness

            // insert record into storage manager
            storageManager.insertIntoTable(table, values);

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

    /**
     * Parses a DROP TABLE statement of the form:
     * DROP TABLE <tableName>;
     * 
     * Validates that the table exists, then deletes the table
     * from the catalog and storage manager.
     */
    private void parseDropTable() throws Exception{
        expectKeyword("DROP");
        expectKeyword("TABLE");
        
        // Get tablename, check if exists
        String tableName = consumeWord();
        TableSchema table = catalog.getTable(tableName);
        if (table == null) {
            throw new Exception("No such table: " + tableName);
        }
        expectEnd();
        
        storageManager.dropTable(table);
        System.out.println("Table dropped successfully");
    }

    // ALTER TABLE <tableName> ADD/DROP ... by sending new and old table schema to storage manager

    /**
     * Parses an ALTER TABLE statement of the form:
     * ALTER TABLE <tableName> ADD <attrName> <type> [NOTNULL] [DEFAULT <value>]
     * ALTER TABLE <tableName> DROP <attrName>
     * 
     * Validates that the table exists, the attribute to add or drop is valid
     * based on the table schema, and then constructs a new TableSchema with
     * the added or dropped attribute and sends it to the storage manager to
     * update the table pages.
     */
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
            throw new Exception("Error: Expected ADD or DROP but got '" + action + "'");
        }

        storageManager.alterTablePages(oldTable, newTable);
        // System.out.println("Table altered successfully");
    }  

    /**
     * Parses an ALTER TABLE ADD statement, which has the form:
     * ALTER TABLE <tableName> ADD <attrName> <type> [NOTNULL] [DEFAULT <value>]
     *
     * Validates that the attribute name does not already exist in the table
     * schema, the data type is valid, and that if NOTNULL is specified then a
     * DEFAULT value is also provided. Then constructs and returns a new
     * TableSchema with the new attribute added.
     */
    private TableSchema parseAlterAdd(TableSchema table) throws Exception {
        String attrName = consumeWord();
        DataType dataType = parseDataType();
        boolean isNN = false;
        Object defaultValue = null;

        // Consume trailing constraint keywords until end of input.
        while (peek() != null && peek().type == Token.Type.WORD) {
            if (peek().value.equals("NOTNULL")) {
                consume();
                isNN = true;
            } else if (peek().value.equals("DEFAULT")) {
                consume();
                Token valueToken = consume();
                defaultValue = convertDefault(valueToken, dataType);
            } else {
                break; // not a constraint, end of command
            }
        }

        expectEnd();

        if (isNN && defaultValue == null) {
            throw new Exception("Error: Not null requires a default value when altering a table");
        }
        
        AttributeSchema newAttr = new AttributeSchema(attrName, dataType, false, isNN, defaultValue);
        TableSchema newTable = new TableSchema(table);

        if (newTable.addAttribute(newAttr) == false) {
            throw new Exception("Error: Attribute name already exists");
        }

        return newTable;
    }

    /**
     * Converts a token to the appropriate Java type for a default value based on the specified data type, and validates constraints.
     * @param token the token representing the default value to convert
     * @param type the data type defining the expected type and constraints for the default value
     * @return the converted default value as an Object of the appropriate Java type
     */
    private Object convertDefault(Token token, DataType type) throws Exception {
        // null 
        if (token.type == Token.Type.WORD && token.value.equalsIgnoreCase("null")) {
            return null;
        }

        // convert Object to its actual type and validate constraints, char must be exact length, varchar must be under max length
        switch (type.getType()) {
            case INTEGER:
                if (token.type != Token.Type.NUMBER || token.value.contains(".")) {
                    throw new Exception("Error: Default value for INTEGER must be an integer");
                }

                return Integer.valueOf(token.value);

            case DOUBLE:
                if (token.type != Token.Type.NUMBER) {
                    throw new Exception("Error: Default value for DOUBLE must be a double");
                }

                return Double.valueOf(token.value);

            case BOOLEAN:
                if (token.type != Token.Type.WORD ||
                    (token.value.equals("True") == false && token.value.equals("False") == false)) {
                    throw new Exception("Error: Default value for BOOLEAN must be True or False");
                }

                return token.value.equals("True");

            case CHAR:
                if (token.type != Token.Type.STRING) {
                    throw new Exception("Error: Default value for CHAR must be a string");
                }

                if (token.value.length() > type.getMaxLength()) {
                    throw new Exception("Error: Default value for CHAR must be " + type.getMaxLength() + " characters");
                }
                return token.value;

            case VARCHAR:
                if (token.type != Token.Type.STRING) {
                    throw new Exception("Error: Default value for VARCHAR must be a string");
                }
                
                // Enforce length constraints
                if (token.value.length() > type.getMaxLength()) {
                    throw new Exception("Error: Default value for VARCHAR must be between 0 and " + type.getMaxLength() + " characters");
                }
                return token.value;

            default:
                throw new Exception("Error: Unknown data type for default value");
        }
    }

    /**
     * Parses an ALTER TABLE DROP statement, which has the form:
     * ALTER TABLE <tableName> DROP <attrName>
     * 
     * Validates that the attribute exists in the table schema and is not a
     * primary key, then constructs and returns a new TableSchema with the
     * attribute removed.
     */
    private TableSchema parseAlterDrop(TableSchema table) throws Exception{
        String attrName = consumeWord();
        expectEnd();

        // prevent dropping primary key
        AttributeSchema attr = table.getAttribute(attrName);
        if (attr == null) {
            throw new Exception("Error: No such attribute: " + attrName);
        }
        if (attr.isPrimaryKey()) {
            throw new Exception("Error: Cannot drop primary key attribute");
        }

        TableSchema newTable = new TableSchema(table);
        if (newTable.dropAttribute(attrName) == false) {
            throw new Exception("Error: No such attribute: " + attrName);
        }

        return newTable;
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