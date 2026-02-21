import Catalog.Catalog;
import CommandParsers.CommandParser;
import CommandParsers.Token;
import StorageManager.StorageManager;
import java.io.File;
import java.io.IOException;
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
        try {
            pageSize = Integer.parseInt(args[1]);
            bufferSize = Integer.parseInt(args[2]);
            indexing = Boolean.parseBoolean(args[3]);
        } catch (NumberFormatException e) {
            System.out.println("Error: pageSize and bufferSize must be integers.");
            return;
        }

        if (pageSize <= 0 || bufferSize <= 0) {
            System.out.println("Error: pageSize and bufferSize must be positive.");
            return;
        }

        // Check for / create new dbLocation folder
        System.out.println("Welcome to JottQL!");

        String catalogPath = dbLocation + File.separator + "catalog";
        File dbFolder = new File(dbLocation);
        File dbFile = new File(dbLocation + File.separator + "db");
        File catalogFile = new File(catalogPath);

        System.out.println("Accessing database location....");

        if (catalogFile.exists()) {
            System.out.println("Database found. Restarting database....");
        } else {
            System.out.println("No database found. Creating new database....");
            if (!dbFolder.exists()) {
                dbFolder.mkdir();
            }
            if (!dbFile.exists()) {
                try {
                    dbFile.createNewFile();
                } catch (Exception e) {
                    System.out.println("Failed to create database: " + e.getMessage());
                    return;
                }
            }
        }

        Catalog catalog;
        try {
            catalog = Catalog.initialize(catalogPath, pageSize, indexing);
        } catch (Exception e) {
            System.out.println("Failed to initialize catalog: " + e.getMessage());
            return;
        }

        if (catalogFile.exists() && catalog.getPageSize() != pageSize) {
            System.out.println("Ignoring provided page size. Using prior size of " + catalog.getPageSize() + "....");
        }
        StorageManager storageManager = new StorageManager(dbFile, catalog.getPageSize(), bufferSize, catalog);

        Scanner scanner = new Scanner(System.in);

        try {
            while (true) {
                System.out.print("JottQL> ");

                if (!scanner.hasNextLine()) {
                    break;
                }

                String firstLine = scanner.nextLine().trim();

                if (firstLine.equalsIgnoreCase("<QUIT>")) {
                    break;
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
        } catch (Exception e) {
            System.out.println("Unrecoverable error: " + e.getMessage());
        } finally {
            try {
                System.out.println("Purging page buffer....");
                storageManager.evictAll();
            } catch (Exception e) {
                System.out.println("Error purging page buffer: " + e.getMessage());
            }

            try {
                System.out.println("Writing catalog to hardware....");
                catalog.saveToFile(catalogPath);
            } catch (IOException e) {
                System.out.println("Error saving catalog: " + e.getMessage());
            }

            System.out.println("Shutting down the database...");
            scanner.close();
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
