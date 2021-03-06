package liquibase.database.jvm;

import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.DatabaseFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.logging.LogFactory;
import liquibase.resource.ResourceAccessor;
import liquibase.util.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * A ConnectionWrapper implementation which delegates completely to an
 * underlying java.sql.connection.
 *
 * @author <a href="mailto:csuml@yahoo.co.uk">Paul Keeble</a>
 */
public class JdbcConnection implements DatabaseConnection {
    private java.sql.Connection con;

    // Allow instantiation via reflection, without an already constructed java.sql.Connection
    public JdbcConnection() {
    }

    public JdbcConnection(java.sql.Connection connection) {
        this.con = connection;
    }


    @Override
    public void attached(Database database) {
        try {
            database.addReservedWords(Arrays.asList(this.getWrappedConnection().getMetaData().getSQLKeywords().toUpperCase().split(",\\s*")));
        } catch (SQLException e) {
            LogFactory.getLogger().info("Error fetching reserved words list from JDBC driver", e);
        }


    }

    @Override
    public String getDatabaseProductName() throws DatabaseException {
        try {
            return con.getMetaData().getDatabaseProductName();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String getDatabaseProductVersion() throws DatabaseException {
        try {
            return con.getMetaData().getDatabaseProductVersion();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public int getDatabaseMajorVersion() throws DatabaseException {
        try {
            return con.getMetaData().getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws DatabaseException {
        try {
            return con.getMetaData().getDatabaseMinorVersion();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String getURL() {
        try {
            return con.getMetaData().getURL();
        } catch (SQLException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }

    @Override
    public String getConnectionUserName() {
        try {
            return con.getMetaData().getUserName();
        } catch (SQLException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }

    /**
     * Returns the connection that this Delegate is using.
     *
     * @return The connection originally passed in the constructor
     */
    public Connection getWrappedConnection() {
        return con;
    }

    public void clearWarnings() throws DatabaseException {
        try {
            con.clearWarnings();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() throws DatabaseException {
        rollback();
        try {
            con.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void commit() throws DatabaseException {
        try {
            if (!con.getAutoCommit()) {
                con.commit();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public Statement createStatement() throws DatabaseException {
        try {
            return con.createStatement();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency, int resultSetHoldability)
            throws DatabaseException {
        try {
            return con.createStatement(resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws DatabaseException {
        try {
            return con.createStatement(resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean getAutoCommit() throws DatabaseException {
        try {
            return con.getAutoCommit();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String getCatalog() throws DatabaseException {
        try {
            return con.getCatalog();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public int getHoldability() throws DatabaseException {
        try {
            return con.getHoldability();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public DatabaseMetaData getMetaData() throws DatabaseException {
        try {
            return con.getMetaData();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public int getTransactionIsolation() throws DatabaseException {
        try {
            return con.getTransactionIsolation();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public Map<String, Class<?>> getTypeMap() throws DatabaseException {
        try {
            return con.getTypeMap();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public SQLWarning getWarnings() throws DatabaseException {
        try {
            return con.getWarnings();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean isClosed() throws DatabaseException {
        try {
            return con.isClosed();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public boolean isReadOnly() throws DatabaseException {
        try {
            return con.isReadOnly();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String nativeSQL(String sql) throws DatabaseException {
        try {
            return con.nativeSQL(sql);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency, int resultSetHoldability)
            throws DatabaseException {
        try {
            return con.prepareCall(sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws DatabaseException {
        try {
            return con.prepareCall(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public CallableStatement prepareCall(String sql) throws DatabaseException {
        try {
            return con.prepareCall(sql);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws DatabaseException {
        try {
            return con.prepareStatement(sql, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws DatabaseException {
        try {
            return con.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws DatabaseException {
        try {
            return con.prepareStatement(sql, autoGeneratedKeys);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws DatabaseException {
        try {
            return con.prepareStatement(sql, columnIndexes);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws DatabaseException {
        try {
            return con.prepareStatement(sql, columnNames);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql) throws DatabaseException {
        try {
            return con.prepareStatement(sql);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void releaseSavepoint(Savepoint savepoint) throws DatabaseException {
        try {
            con.releaseSavepoint(savepoint);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void rollback() throws DatabaseException {
        try {
            if (!con.getAutoCommit() && !con.isClosed()) {
                con.rollback();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void rollback(Savepoint savepoint) throws DatabaseException {
        try {
            if (!con.getAutoCommit()) {
                con.rollback(savepoint);
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws DatabaseException {
        // Fix for Sybase jConnect JDBC driver bug.
        // Which throws DatabaseException(JZ016: The AutoCommit option is already set to false)
        // if con.setAutoCommit(false) called twise or more times with value 'false'.
//        if (con.getAutoCommit() != autoCommit) {
        try {
            con.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
//        }
    }

    public void setCatalog(String catalog) throws DatabaseException {
        try {
            con.setCatalog(catalog);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void setHoldability(int holdability) throws DatabaseException {
        try {
            con.setHoldability(holdability);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void setReadOnly(boolean readOnly) throws DatabaseException {
        try {
            con.setReadOnly(readOnly);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public Savepoint setSavepoint() throws DatabaseException {
        try {
            return con.setSavepoint();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public Savepoint setSavepoint(String name) throws DatabaseException {
        try {
            return con.setSavepoint(name);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void setTransactionIsolation(int level) throws DatabaseException {
        try {
            con.setTransactionIsolation(level);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void setTypeMap(Map<String, Class<?>> map) throws DatabaseException {
        try {
            con.setTypeMap(map);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public Connection getUnderlyingConnection() {
        return con;
    }

    public void openConnection(String url, String username, String password, String driver, String databaseClass,
                               String driverPropertiesFile, String propertyProviderClass, ResourceAccessor resourceAccessor)
    throws DatabaseException
    {

        driver = StringUtils.trimToNull(driver);
        if (driver == null) {
            driver = DatabaseFactory.getInstance().findDefaultDriver(url);
        }

        try {
            Driver driverObject;
            DatabaseFactory databaseFactory = DatabaseFactory.getInstance();
            if (databaseClass != null) {
                databaseFactory.clearRegistry();
                databaseFactory.register((Database) Class.forName(databaseClass, true, resourceAccessor.toClassLoader()).newInstance());
            }

            try {
                if (driver == null) {
                    driver = databaseFactory.findDefaultDriver(url);
                }

                if (driver == null) {
                    throw new RuntimeException("Driver class was not specified and could not be determined from the url (" + url + ")");
                }

                driverObject = (Driver) Class.forName(driver, true, resourceAccessor.toClassLoader()).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Cannot find database driver: " + e.getMessage());
            }

            Properties driverProperties;
            if (propertyProviderClass == null) {
                driverProperties = new Properties();
            } else {
                driverProperties = (Properties) Class.forName(propertyProviderClass, true, resourceAccessor.toClassLoader()).newInstance();
            }

            if (username != null) {
                driverProperties.put("user", username);
            }
            if (password != null) {
                driverProperties.put("password", password);
            }
            if (null != driverPropertiesFile) {
                File propertiesFile = new File(driverPropertiesFile);
                if (propertiesFile.exists()) {
//                    System.out.println("Loading properties from the file:'" + driverPropertiesFile + "'");
                    FileInputStream inputStream = new FileInputStream(propertiesFile);
                    try {
                        driverProperties.load(inputStream);
                    } finally {
                        inputStream.close();
                    }
                } else {
                    throw new RuntimeException("Can't open JDBC Driver specific properties from the file: '"
                                               + driverPropertiesFile + "'");
                }
            }


//            System.out.println("Properties:");
//            for (Map.Entry entry : driverProperties.entrySet()) {
//                System.out.println("Key:'"+entry.getKey().toString()+"' Value:'"+entry.getValue().toString()+"'");
//            }


//            System.out.println("Connecting to the URL:'"+url+"' using driver:'"+driverObject.getClass().getName()+"'");
            Connection connection = driverObject.connect(url, driverProperties);
//            System.out.println("Connection has been created");
            if (connection == null) {
                throw new DatabaseException("Connection could not be created to " + url + " with driver " + driverObject.getClass().getName() + ".  Possibly the wrong driver for the given database URL");
            }

            this.con = connection;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JdbcConnection && this.getUnderlyingConnection().equals(((JdbcConnection) obj).getUnderlyingConnection());

    }

    @Override
    public int hashCode() {
        return this.getUnderlyingConnection().hashCode();
    }
}
