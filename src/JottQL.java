import java.util.Scanner;

import CommandParsers.ParseCreate;

public class JottQL {
    public static void main(String[] args) {
        System.out.println("Starting Database . . .");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Enter Command > ");
            
            String firstLine = scanner.nextLine().trim();
            if (firstLine.equalsIgnoreCase("<QUIT>")) {
                System.out.println("Exiting JottQL...");
                scanner.close();
                // WRITE CATALOG TO HARDWARE
                // PURGE PAGE BUFFER
                return;
            }
            
            String command = readRestOfCommand(scanner, firstLine);
            
            String firstWord = command.split("\\s+")[0];
            
            switch (firstWord.toUpperCase()) {
                case "CREATE":
                    System.out.println("CREATE command: " + command);
                    ParseCreate.handleCreateCommand(command);
                    // IF CALL SUCCSSFUL, SEND TO CATALOG
                    break;
                case "SELECT":
                    // SELECT * FROM <tableName>;
                    System.out.println("SELECT command: " + command);
                    break;
                case "INSERT":
                    // INSERT <tableName> VALUES ( <row1>, <row2>, ... );
                    System.out.println("INSERT command: " + command);
                    break;
                case "DROP":
                    // DROP TABLE <tableName>;
                    System.out.println("DROP command: " + command);
                    break;
                case "ALTER":
                    // ALTER TABLE <tableName> ADD <attrName> <type>;
                    // ALTER TABLE <tableName> ADD <attrName> <type> DEFAULT <value>;
                    // ALTER TABLE <tableName> ADD <attrName> <type> NOTNULL DEFAULT <value>;
                    // ALTER TABLE <tableName> DROP <attrName>;
                    System.out.println("ALTER command: " + command);
                    break;
                case "JAVA":
                    //java JottQL <dbLocation> <pageSize> <bufferSize> <indexing>
                    System.out.println("JAVA command: " + command);
                    break;
                default:
                    System.out.println("Unknown command: " + command);
                    break;
            }
        }
    }
    
    private static String readRestOfCommand(Scanner scanner, String firstLine) {
        StringBuilder command = new StringBuilder();
        if (firstLine.endsWith(";")) {
            return firstLine.substring(0, firstLine.length() - 1).trim();
        }
        command.append(firstLine);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();
            if (line.endsWith(";")) {
                command.append(" ").append(line.substring(0, line.length() - 1).trim());
                break;
            } else {
                command.append(" ").append(line);
            }
        }
        return command.toString().trim();
    }


}