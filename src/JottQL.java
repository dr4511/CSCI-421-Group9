import Catalog.Catalog;
import CommandParsers.CommandParser;
import CommandParsers.Token;
import StorageManager.StorageManager;
import java.io.File;
import java.util.List;
import java.util.Scanner;

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

        // Check for / create new dbLocation folder
        File dbFolder = new File(dbLocation);
        File dbFile = new File(dbLocation + File.separator + dbLocation);
        if (!dbFolder.exists()) {
            dbFolder.mkdir();
            try {
                if (dbFile.createNewFile()) {
                    System.out.println("Created database: " + dbLocation);
                } else {
                    System.out.println("Failed to create database: " + dbLocation);
                    return;
                }
            } catch (Exception e) {
                System.out.println("Failed to create database: " + e.getMessage());
                return;
            }
        } else {
            System.out.println("Using existing database: " + dbLocation);
        }

        System.out.println("Welcome to JottQL!");

        //========================================================
        // TESTING PRINT STATEMENTS
        // System.out.println("DB Location: " + dbLocation);
        // System.out.println("Page Size: " + pageSize);
        // System.out.println("Buffer Size: " + bufferSize);
        // System.out.println("Indexing: " + indexing);
        //========================================================

        // StorageManager.initialize();
        // Buffer.initialize(pageSize, bufferSize);


        String catalogPath = dbLocation + File.separator + "catalog";
        Catalog catalog;

        try {
            catalog = Catalog.initialize(catalogPath, pageSize, indexing);
        } catch (Exception e) {
            System.out.println("Failed to initialize catalog: " + e.getMessage());
            return;
        }
        StorageManager storageManager = new StorageManager(dbLocation, pageSize, bufferSize, catalog);

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("JottQL> ");

            String firstLine = scanner.nextLine().trim();

            if (firstLine.equalsIgnoreCase("<QUIT>")) {
                 try {
                    System.out.println("Purging page buffer...");
                    // PURGE PAGE BUFFER
                    // storageManager.evictAll();

                    System.out.println("Writing catalog to hardware...");
                    catalog.saveToFile(catalogPath);

                    System.out.println("Database saved. Goodbye.");
                } catch (Exception e) {
                    System.out.println("Error shutting down: " + e.getMessage());
                }

                System.out.println("Shutting down the database...");
                scanner.close();
                return;
            }

            String command = readRestOfCommand(scanner, firstLine);

            try {
                List<Token> tokens = Token.tokenize(command);
                CommandParser parser = new CommandParser(tokens, catalog, storageManager);
                parser.parse();
            } catch (Exception e) {
                System.out.println(e.getMessage());
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
