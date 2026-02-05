import java.util.Scanner;

public class JottQL {
        public static void main(String[] args) {
        System.out.println("Starting Database . . .");
        Scanner scanner = new Scanner(System.in);
        while (true) {
                    System.out.print("Enter Command > ");
                    String command = scanner.nextLine().trim();
                    switch (command.toUpperCase()) {
                        case "CREATE":
                            // CREATE TABLE <tableName> (CRE
                            // <attr> <type> <constraints>,
                            // ...
                            // );
                            break;

                        case "SELECT":
                            // SELECT * FROM <tableName>;
                            break;

                        case "INSERT":
                            // INSERT <tableName> VALUES ( <row1>, <row2>, ... );
                            break;

                        case "DROP":
                            // DROP TABLE <tableName>;
                            break;

                        case "ALTER":
                            // ALTER TABLE <tableName> ADD <attrName> <type>;
                            // ALTER TABLE <tableName> ADD <attrName> <type> DEFAULT <value>;
                            // ALTER TABLE <tableName> ADD <attrName> <type> NOTNULL DEFAULT <value>;
                            // ALTER TABLE <tableName> DROP <attrName>;
                            break;

                        case "java":
                            //java JottQL <dbLocation> <pageSize> <bufferSize> <indexing>
                            break;

                        case "<QUIT>":
                            System.out.println("Exiting JottQL...");
                            scanner.close();
                            return;

                        default:
                            System.out.println("Unknown command.");
                            break;
                    }
            }
        }
}

