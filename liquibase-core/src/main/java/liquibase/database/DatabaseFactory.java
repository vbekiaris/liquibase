package liquibase.database;

import liquibase.database.core.UnsupportedDatabase;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.resource.ResourceAccessor;
import liquibase.servicelocator.ServiceLocator;

import java.util.*;

public class DatabaseFactory {
    private static DatabaseFactory instance;
    private Map<String, SortedSet<Database>> implementedDatabases = new HashMap<String, SortedSet<Database>>();
    private Map<String, SortedSet<Database>> internalDatabases = new HashMap<String, SortedSet<Database>>();
    private Logger log;

    private DatabaseFactory() {
        log = new LogFactory().getLog();
        try {
            Class[] classes = ServiceLocator.getInstance().findClasses(Database.class);

            //noinspection unchecked
            for (Class<? extends Database> clazz : classes) {
                try {
                    register(clazz.getConstructor().newInstance());
                } catch (Throwable e) {
                    throw new UnexpectedLiquibaseException("Error registering "+clazz.getName(), e);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static DatabaseFactory getInstance() {
        if (instance == null) {
            instance = new DatabaseFactory();
        }
        return instance;
    }

    public static void reset() {
        instance = new DatabaseFactory();
    }

    /**
     * Set singleton instance. Primarily used in testing
     */
    public static void setInstance(DatabaseFactory databaseFactory) {
        instance = databaseFactory;
    }

    /**
     * Returns instances of all implemented database types.
     */
    public List<Database> getImplementedDatabases() {
        List<Database> returnList = new ArrayList<Database>();
        for (SortedSet<Database> set : implementedDatabases.values()) {
            returnList.add(set.iterator().next());
        }
        return returnList;
    }

    /**
     * Returns instances of all "internal" database types.
     */
    public List<Database> getInternalDatabases() {
        List<Database> returnList = new ArrayList<Database>();
        for (SortedSet<Database> set : internalDatabases.values()) {
            returnList.add(set.iterator().next());
        }
        return returnList;
    }

    public void register(Database database) {
        Map<String, SortedSet<Database>> map = null;
        if (database instanceof InternalDatabase) {
            map = internalDatabases;
        } else {
            map = implementedDatabases;

        }

        if (!map.containsKey(database.getShortName())) {
            map.put(database.getShortName(), new TreeSet<Database>(new TreeSet<Database>(new DatabaseComparator())));
        }
        map.get(database.getShortName()).add(database);
    }

    public Database findCorrectDatabaseImplementation(DatabaseConnection connection) throws DatabaseException {

        SortedSet<Database> foundDatabases = new TreeSet<Database>(new DatabaseComparator());

        for (Database implementedDatabase : getImplementedDatabases()) {
            if (connection instanceof OfflineConnection) {
                if (((OfflineConnection) connection).isCorrectDatabaseImplementation(implementedDatabase)) {
                    foundDatabases.add(implementedDatabase);
                }
            } else {
                if (implementedDatabase.isCorrectDatabaseImplementation(connection)) {
                    foundDatabases.add(implementedDatabase);
                }
            }
        }

        if (foundDatabases.size() == 0) {
            log.warning("Unknown database: " + connection.getDatabaseProductName());
            UnsupportedDatabase unsupportedDB = new UnsupportedDatabase();
            unsupportedDB.setConnection(connection);
            return unsupportedDB;
        }

        Database returnDatabase;
        try {
            returnDatabase = foundDatabases.iterator().next().getClass().newInstance();
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException(e);
        }

        returnDatabase.setConnection(connection);
        return returnDatabase;
    }

    public Database openDatabase(String url,
                            String username,
                            String password,
                            String propertyProviderClass,
                            ResourceAccessor resourceAccessor) throws DatabaseException {
        return openDatabase(url, username, password, null, null, null, propertyProviderClass, resourceAccessor);
    }

    public Database openDatabase(String url,
                            String username,
                            String password,
                            String driver,
                            String databaseClass,
                            String driverPropertiesFile,
                            String propertyProviderClass,
                            ResourceAccessor resourceAccessor) throws DatabaseException {
        return this.findCorrectDatabaseImplementation(openConnection(url, username, password, driver, databaseClass, driverPropertiesFile, propertyProviderClass, resourceAccessor));
    }

    public DatabaseConnection openConnection(String url,
                                             String username,
                                             String password,
                                             String propertyProvider,
                                             ResourceAccessor resourceAccessor) throws DatabaseException {

        return openConnection(url, username, password, null, null, null, propertyProvider, resourceAccessor);
    }

    public DatabaseConnection openConnection(String url,
                                             String username,
                                             String password,
                                             String driver,
                                             String databaseClass,
                                             String driverPropertiesFile,
                                             String propertyProviderClass,
                                             ResourceAccessor resourceAccessor) throws DatabaseException {
        if (url.startsWith("offline:")) {
            return new OfflineConnection(url, resourceAccessor);
        }

        String dbConnectionClassName = findDatabaseConnectionName(url);
        if (dbConnectionClassName != null && dbConnectionClassName.length() > 0) {
            try
            {
                DatabaseConnection dbConnection = (DatabaseConnection) resourceAccessor.toClassLoader().
                                                                       loadClass(dbConnectionClassName).newInstance();
                dbConnection.openConnection(url, username, password, driver, databaseClass, driverPropertiesFile,
                                            propertyProviderClass,resourceAccessor);

                return dbConnection;
            }
            catch (InstantiationException e)
            {
                throw new DatabaseException("Cannot instantiate database connection", e);
            }
            catch (IllegalAccessException e)
            {
                throw new DatabaseException("Cannot instantiate database connection", e);
            }
            catch (ClassNotFoundException e)
            {
                throw new DatabaseException("Class not found for database connection", e);
            }
            catch (DatabaseException e) {
                throw new DatabaseException(e);
            }
        }
        else {
            throw new DatabaseException("Could not locate a database connection for the given configuration");
        }
    }

    private String findDatabaseConnectionName(String url) {
        for (Database database : this.getImplementedDatabases()) {
            String dbConnClassName = database.getDatabaseConnectionClassName(url);
            if (dbConnClassName != null) {
                return dbConnClassName;
            }
        }

        return null;
    }

    public String findDefaultDriver(String url) {
        for (Database database : this.getImplementedDatabases()) {
            String defaultDriver = database.getDefaultDriver(url);
            if (defaultDriver != null) {
                return defaultDriver;
            }
        }

        return null;
    }

    /**
     * Removes all registered databases, even built in ones.  Useful for forcing a particular database implementation
     */
    public void clearRegistry() {
        implementedDatabases.clear();
    }

    public Database getDatabase(String shortName) {
        if (!implementedDatabases.containsKey(shortName)) {
            return null;
        }
        return implementedDatabases.get(shortName).iterator().next();

    }

    private static class DatabaseComparator implements Comparator<Database> {
        @Override
        public int compare(Database o1, Database o2) {
            return -1 * new Integer(o1.getPriority()).compareTo(o2.getPriority());
        }
    }
}
