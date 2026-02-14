import CommandParsers.Token;
import java.util.List;
import java.util.Scanner;
// import BufferManager.Buffer;
// import StorageManager.StorageManager;

public class JottQL {

    public static String dbLocation;
    public static int pageSize;
    public static int bufferSize;
    public static boolean indexing;

    public static void main(String[] args) {

        if (args.length != 4) {
            System.out.println("Usage: java JottQL <dbLocation> <pageSize> <bufferSize> <indexing>");
            return;
        }

        dbLocation = args[0];
        pageSize = Integer.parseInt(args[1]);
        bufferSize = Integer.parseInt(args[2]);
        indexing = Boolean.parseBoolean(args[3]);

        System.out.println("Starting Database...");

        //========================================================
        // TESTING PRINT STATEMENTS
        System.out.println("DB Location: " + dbLocation);
        System.out.println("Page Size: " + pageSize);
        System.out.println("Buffer Size: " + bufferSize);
        System.out.println("Indexing: " + indexing);
        //========================================================

        // StorageManager.initialize();
        // Buffer.initialize(pageSize, bufferSize);

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("Enter Command > ");

            String firstLine = scanner.nextLine().trim();

            if (firstLine.equalsIgnoreCase("<QUIT>")) {
                System.out.println("Exiting JottQL...");
                scanner.close();
                // WRITE CATALOG TO HARDWARE
                // PURGE PAGE BUFFER
                // Buffer.shutdown();
                return;
            }

            String command = readRestOfCommand(scanner, firstLine);
            try {
                List<Token> tokens = Token.tokenize(command);
                for (int i = 0; i < tokens.size(); i++) {
                    System.out.println(tokens.get(i));
                }
            } catch (Exception e) {
                e.printStackTrace();
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
                command.append(" ")
                       .append(line.substring(0, line.length() - 1).trim());
                break;
            } else {
                command.append(" ").append(line);
            }
        }

        return command.toString().trim();
    }
}
