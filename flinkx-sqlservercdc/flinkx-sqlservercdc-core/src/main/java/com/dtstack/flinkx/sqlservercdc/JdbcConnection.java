package com.dtstack.flinkx.sqlservercdc;

import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class JdbcConnection  implements AutoCloseable {

    private static final char STATEMENT_DELIMITER = ';';
    private static final int STATEMENT_CACHE_CAPACITY = 10_000;
    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);
    private final Map<String, PreparedStatement> statementCache = new BoundedConcurrentHashMap<>(STATEMENT_CACHE_CAPACITY, 16, BoundedConcurrentHashMap.Eviction.LIRS,
            new BoundedConcurrentHashMap.EvictionListener<String, PreparedStatement>() {

                @Override
                public void onEntryEviction(Map<String, PreparedStatement> evicted) {
                }

                @Override
                public void onEntryChosenForEviction(PreparedStatement statement) {
                    cleanupPreparedStatement(statement);
                }
            });


    /**
     * Defines multiple JDBC operations.
     */
    @FunctionalInterface
    public static interface Operations {
        /**
         * Apply a series of operations against the given JDBC statement.
         *
         * @param statement the JDBC statement to use to execute one or more operations
         * @throws SQLException if there is an error connecting to the database or executing the statements
         */
        void apply(Statement statement) throws SQLException;
    }

    /**
     * Extracts a data of resultset..
     */
    @FunctionalInterface
    public static interface ResultSetExtractor<T> {
        T apply(ResultSet rs) throws SQLException;
    }


    private volatile Connection conn;
    protected String username;
    protected String password;
    protected String url;
    protected String databaseName;

    public JdbcConnection(String username, String password, String url, String databaseName) {
        this.username = username;
        this.password = password;
        this.url = url;
        this.databaseName = databaseName;
    }

    public JdbcConnection setAutoCommit(boolean autoCommit) throws SQLException {
        connection().setAutoCommit(autoCommit);
        return this;
    }

    public JdbcConnection commit() throws SQLException {
        Connection conn = connection();
        if (!conn.getAutoCommit()) {
            conn.commit();
        }
        return this;
    }

    public synchronized JdbcConnection rollback() throws SQLException {
        if (!isConnected()) {
            return this;
        }
        Connection conn = connection();
        if (!conn.getAutoCommit()) {
            conn.rollback();
        }
        return this;
    }

    /**
     * Ensure a connection to the database is established.
     *
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database
     */
    public JdbcConnection connect() throws SQLException {
        connection();
        return this;
    }

    /**
     * Execute a series of SQL statements as a single transaction.
     *
     * @param sqlStatements the SQL statements that are to be performed as a single transaction
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection execute(String... sqlStatements) throws SQLException {
        return execute(statement -> {
            for (String sqlStatement : sqlStatements) {
                if (sqlStatement != null) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("executing '{}'", sqlStatement);
                    }
                    statement.execute(sqlStatement);
                }
            }
        });
    }

    /**
     * Execute a series of operations as a single transaction.
     *
     * @param operations the function that will be called with a newly-created {@link Statement}, and that performs
     *            one or more operations on that statement object
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     */
    public JdbcConnection execute(Operations operations) throws SQLException {
        Connection conn = connection();
        try (Statement statement = conn.createStatement();) {
            operations.apply(statement);
            commit();
        }
        return this;
    }

    public static interface ResultSetConsumer {
        void accept(ResultSet rs) throws SQLException;
    }

    public static interface ResultSetMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    public static interface BlockingResultSetConsumer {
        void accept(ResultSet rs) throws SQLException, InterruptedException;
    }

    public static interface ParameterResultSetConsumer {
        void accept(List<?> parameters, ResultSet rs) throws SQLException;
    }

    public static interface MultiResultSetConsumer {
        void accept(ResultSet[] rs) throws SQLException;
    }

    public static interface BlockingMultiResultSetConsumer {
        void accept(ResultSet[] rs) throws SQLException, InterruptedException;
    }

    public static interface StatementPreparer {
        void accept(PreparedStatement statement) throws SQLException;
    }

    @FunctionalInterface
    public static interface CallPreparer {
        void accept(CallableStatement statement) throws SQLException;
    }

    /**
     * Execute a SQL query.
     *
     * @param query the SQL query
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection query(String query, ResultSetConsumer resultConsumer) throws SQLException {
        return query(query, Connection::createStatement, resultConsumer);
    }

    /**
     * Execute a SQL query and map the result set into an expected type.
     * @param <T> type returned by the mapper
     *
     * @param query the SQL query
     * @param mapper the function processing the query results
     * @return the result of the mapper calculation
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public <T> T queryAndMap(String query, ResultSetMapper<T> mapper) throws SQLException {
        return queryAndMap(query, Connection::createStatement, mapper);
    }

    /**
     * Execute a stored procedure.
     *
     * @param sql the SQL query; may not be {@code null}
     * @param callPreparer a {@link CallPreparer} instance which can be used to set additional parameters; may be null
     * @param resultSetConsumer a {@link ResultSetConsumer} instance which can be used to process the results; may be null
     * @return this object for chaining methods together
     * @throws SQLException if anything unexpected fails
     */
    public JdbcConnection call(String sql, CallPreparer callPreparer, ResultSetConsumer resultSetConsumer) throws SQLException {
        Connection conn = connection();
        try (CallableStatement callableStatement = conn.prepareCall(sql)) {
            if (callPreparer != null) {
                callPreparer.accept(callableStatement);
            }
            try (ResultSet rs = callableStatement.executeQuery()) {
                if (resultSetConsumer != null) {
                    resultSetConsumer.accept(rs);
                }
            }
        }
        return this;
    }

    /**
     * Execute a SQL query.
     *
     * @param query the SQL query
     * @param statementFactory the function that should be used to create the statement from the connection; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection query(String query, StatementFactory statementFactory, ResultSetConsumer resultConsumer) throws SQLException {
        Connection conn = connection();
        try (Statement statement = statementFactory.createStatement(conn);) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("running '{}'", query);
            }
            try (ResultSet resultSet = statement.executeQuery(query);) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    /**
     * Execute multiple SQL prepared queries where each query is executed with the same set of parameters.
     *
     * @param multiQuery the array of prepared queries
     * @param preparer the function that supplies arguments to the prepared statement; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String[] multiQuery, StatementPreparer preparer, BlockingMultiResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        final StatementPreparer[] preparers = new StatementPreparer[multiQuery.length];
        Arrays.fill(preparers, preparer);
        return prepareQuery(multiQuery, preparers, resultConsumer);
    }

    /**
     * Execute multiple SQL prepared queries where each query is executed with the same set of parameters.
     *
     * @param multiQuery the array of prepared queries
     * @param preparers the array of functions that supply arguments to the prepared statements; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String[] multiQuery, StatementPreparer[] preparers, BlockingMultiResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        final ResultSet[] resultSets = new ResultSet[multiQuery.length];
        final PreparedStatement[] preparedStatements = new PreparedStatement[multiQuery.length];

        try {
            for (int i = 0; i < multiQuery.length; i++) {
                final String query = multiQuery[i];
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("running '{}'", query);
                }
                final PreparedStatement statement = createPreparedStatement(query);
                preparedStatements[i] = statement;
                preparers[i].accept(statement);
                resultSets[i] = statement.executeQuery();
            }
            if (resultConsumer != null) {
                resultConsumer.accept(resultSets);
            }
        }
        finally {
            for (ResultSet rs : resultSets) {
                if (rs != null) {
                    try {
                        rs.close();
                    }
                    catch (Exception ei) {
                    }
                }
            }
        }
        return this;
    }

    /**
     * Execute a SQL query and map the result set into an expected type.
     * @param <T> type returned by the mapper
     *
     * @param query the SQL query
     * @param statementFactory the function that should be used to create the statement from the connection; may not be null
     * @param mapper the function processing the query results
     * @return the result of the mapper calculation
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public <T> T queryAndMap(String query, StatementFactory statementFactory, ResultSetMapper<T> mapper) throws SQLException {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        Connection conn = connection();
        try (Statement statement = statementFactory.createStatement(conn);) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("running '{}'", query);
            }
            try (ResultSet resultSet = statement.executeQuery(query);) {
                return mapper.apply(resultSet);
            }
        }
    }

    public JdbcConnection queryWithBlockingConsumer(String query, StatementFactory statementFactory, BlockingResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        Connection conn = connection();
        try (Statement statement = statementFactory.createStatement(conn);) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("running '{}'", query);
            }
            try (ResultSet resultSet = statement.executeQuery(query);) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    /**
     * A function to create a statement from a connection.
     * @author Randall Hauch
     */
    @FunctionalInterface
    public interface StatementFactory {
        /**
         * Use the given connection to create a statement.
         * @param connection the JDBC connection; never null
         * @return the statement
         * @throws SQLException if there are problems creating a statement
         */
        Statement createStatement(Connection connection) throws SQLException;
    }

    /**
     * Execute a SQL prepared query.
     *
     * @param preparedQueryString the prepared query string
     * @param preparer the function that supplied arguments to the prepared statement; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQueryWithBlockingConsumer(String preparedQueryString, StatementPreparer preparer, BlockingResultSetConsumer resultConsumer)
            throws SQLException, InterruptedException {
        final PreparedStatement statement = createPreparedStatement(preparedQueryString);
        preparer.accept(statement);
        try (ResultSet resultSet = statement.executeQuery();) {
            if (resultConsumer != null) {
                resultConsumer.accept(resultSet);
            }
        }
        return this;
    }

    /**
     * Execute a SQL prepared query.
     *
     * @param preparedQueryString the prepared query string
     * @param preparer the function that supplied arguments to the prepared statement; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String preparedQueryString, StatementPreparer preparer, ResultSetConsumer resultConsumer)
            throws SQLException {
        final PreparedStatement statement = createPreparedStatement(preparedQueryString);
        preparer.accept(statement);
        try (ResultSet resultSet = statement.executeQuery();) {
            if (resultConsumer != null) {
                resultConsumer.accept(resultSet);
            }
        }
        return this;
    }

    /**
     * Execute a SQL prepared query and map the result set into an expected type..
     * @param <T> type returned by the mapper
     *
     * @param preparedQueryString the prepared query string
     * @param preparer the function that supplied arguments to the prepared statement; may not be null
     * @param mapper the function processing the query results
     * @return the result of the mapper calculation
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public <T> T prepareQueryAndMap(String preparedQueryString, StatementPreparer preparer, ResultSetMapper<T> mapper)
            throws SQLException {
        Objects.requireNonNull(mapper, "Mapper must be provided");
        final PreparedStatement statement = createPreparedStatement(preparedQueryString);
        preparer.accept(statement);
        try (ResultSet resultSet = statement.executeQuery();) {
            return mapper.apply(resultSet);
        }
    }

    /**
     * Execute a SQL update via a prepared statement.
     *
     * @param stmt the statement string
     * @param preparer the function that supplied arguments to the prepared stmt; may be null
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareUpdate(String stmt, StatementPreparer preparer) throws SQLException {
        final PreparedStatement statement = createPreparedStatement(stmt);
        if (preparer != null) {
            preparer.accept(statement);
        }
        statement.execute();
        return this;
    }

    /**
     * Execute a SQL prepared query.
     *
     * @param preparedQueryString the prepared query string
     * @param parameters the list of values for parameters in the query; may not be null
     * @param resultConsumer the consumer of the query results
     * @return this object for chaining methods together
     * @throws SQLException if there is an error connecting to the database or executing the statements
     * @see #execute(Operations)
     */
    public JdbcConnection prepareQuery(String preparedQueryString, List<?> parameters,
                                       ParameterResultSetConsumer resultConsumer)
            throws SQLException {
        final PreparedStatement statement = createPreparedStatement(preparedQueryString);
        int index = 1;
        for (final Object parameter : parameters) {
            statement.setObject(index++, parameter);
        }
        try (ResultSet resultSet = statement.executeQuery()) {
            if (resultConsumer != null) {
                resultConsumer.accept(parameters, resultSet);
            }
        }
        return this;
    }

    public void print(ResultSet resultSet) {
        // CHECKSTYLE:OFF
        print(resultSet, System.out::println);
        // CHECKSTYLE:ON
    }

    public void print(ResultSet resultSet, Consumer<String> lines) {
        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnCount = rsmd.getColumnCount();
            int[] columnSizes = findMaxLength(resultSet);
            lines.accept(delimiter(columnCount, columnSizes));
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    sb.append(" | ");
                }
                sb.append(StringUtils.leftPad(rsmd.getColumnLabel(i), columnSizes[i], ' '));
            }
            lines.accept(sb.toString());
            sb.setLength(0);
            lines.accept(delimiter(columnCount, columnSizes));
            while (resultSet.next()) {
                sb.setLength(0);
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        sb.append(" | ");
                    }
                    sb.append(StringUtils.leftPad(resultSet.getString(i), columnSizes[i], ' '));
                }
                lines.accept(sb.toString());
                sb.setLength(0);
            }
            lines.accept(delimiter(columnCount, columnSizes));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String delimiter(int columnCount, int[] columnSizes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                sb.append("---");
            }
            sb.append(createString('-', columnSizes[i]));
        }
        return sb.toString();
    }

    public static String createString(final char charToRepeat,
                                      int numberOfRepeats) {
        assert numberOfRepeats >= 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfRepeats; ++i) {
            sb.append(charToRepeat);
        }
        return sb.toString();
    }

    private int[] findMaxLength(ResultSet resultSet) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnCount = rsmd.getColumnCount();
        int[] columnSizes = new int[columnCount + 1];
        for (int i = 1; i <= columnCount; i++) {
            columnSizes[i] = Math.max(columnSizes[i], rsmd.getColumnLabel(i).length());
        }
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                String value = resultSet.getString(i);
                if (value != null) {
                    columnSizes[i] = Math.max(columnSizes[i], value.length());
                }
            }
        }
        resultSet.beforeFirst();
        return columnSizes;
    }

    public synchronized boolean isConnected() throws SQLException {
        if (conn == null) {
            return false;
        }
        return !conn.isClosed();
    }

    public synchronized Connection connection() throws SQLException {
        if (!isConnected()) {
            conn =  DbUtil.getConnection(url, username, password);
            if (!isConnected()) {
                throw new SQLException("Unable to obtain a JDBC connection");
            }
        }
        return conn;
    }

    protected List<String> parseSqlStatementString(final String statements) {
        final List<String> splitStatements = new ArrayList<>();
        final char[] statementsChars = statements.toCharArray();
        StringBuilder activeStatement = new StringBuilder();
        for (int i = 0; i < statementsChars.length; i++) {
            if (statementsChars[i] == STATEMENT_DELIMITER) {
                if (i == statementsChars.length - 1) {
                    // last character so it is the delimiter
                }
                else if (statementsChars[i + 1] == STATEMENT_DELIMITER) {
                    // two semicolons in a row - escaped semicolon
                    activeStatement.append(STATEMENT_DELIMITER);
                    i++;
                }
                else {
                    // semicolon as a delimiter
                    final String trimmedStatement = activeStatement.toString().trim();
                    if (!trimmedStatement.isEmpty()) {
                        splitStatements.add(trimmedStatement);
                    }
                    activeStatement = new StringBuilder();
                }
            }
            else {
                activeStatement.append(statementsChars[i]);
            }
        }
        final String trimmedStatement = activeStatement.toString().trim();
        if (!trimmedStatement.isEmpty()) {
            splitStatements.add(trimmedStatement);
        }
        return splitStatements;
    }

    /**
     * Close the connection and release any resources.
     */
    @Override
    public synchronized void close() throws SQLException {
        if (conn != null) {
            try {
                statementCache.values().forEach(this::cleanupPreparedStatement);
                statementCache.clear();
                LOGGER.trace("Closing database connection");
                conn.close();
            }
            finally {
                conn = null;
            }
        }
    }

    /**
     * Get the names of all of the catalogs.
     * @return the set of catalog names; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<String> readAllCatalogNames()
            throws SQLException {
        Set<String> catalogs = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getCatalogs()) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                catalogs.add(catalogName);
            }
        }
        return catalogs;
    }

    /**
     * Get the names of all of the schemas, optionally applying a filter.
     *
     * @param filter a {@link Predicate} to test each schema name; may be null in which case all schema names are returned
     * @return the set of catalog names; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<String> readAllSchemaNames(Predicate<String> filter)
            throws SQLException {
        Set<String> schemas = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getSchemas()) {
            while (rs.next()) {
                String schema = rs.getString(1);
                if (filter != null && filter.test(schema)) {
                    schemas.add(schema);
                }
            }
        }
        return schemas;
    }

    public String[] tableTypes() throws SQLException {
        List<String> types = new ArrayList<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getTableTypes()) {
            while (rs.next()) {
                String tableType = rs.getString(1);
                if (tableType != null) {
                    types.add(tableType);
                }
            }
        }
        return types.toArray(new String[types.size()]);
    }

    /**
     * Get the identifiers of all available tables.
     *
     * @param tableTypes the set of table types to include in the results, which may be null for all table types
     * @return the set of {@link TableId}s; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<TableId> readAllTableNames(String[] tableTypes) throws SQLException {
        return readTableNames(null, null, null, tableTypes);
    }

    /**
     * Get the identifiers of the tables.
     *
     * @param databaseCatalog the name of the catalog, which is typically the database name; may be an empty string for tables
     *            that have no catalog, or {@code null} if the catalog name should not be used to narrow the list of table
     *            identifiers
     * @param schemaNamePattern the pattern used to match database schema names, which may be "" to match only those tables with
     *            no schema or {@code null} if the schema name should not be used to narrow the list of table
     *            identifiers
     * @param tableNamePattern the pattern used to match database table names, which may be null to match all table names
     * @param tableTypes the set of table types to include in the results, which may be null for all table types
     * @return the set of {@link TableId}s; never null but possibly empty
     * @throws SQLException if an error occurs while accessing the database metadata
     */
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
                                       String[] tableTypes)
            throws SQLException {
        if (tableNamePattern == null) {
            tableNamePattern = "%";
        }
        Set<TableId> tableIds = new HashSet<>();
        DatabaseMetaData metadata = connection().getMetaData();
        try (ResultSet rs = metadata.getTables(databaseCatalog, schemaNamePattern, tableNamePattern, tableTypes)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                tableIds.add(tableId);
            }
        }
        return tableIds;
    }

    /**
     * Resolves the supplied metadata JDBC type to a final JDBC type.
     *
     * @param metadataJdbcType the JDBC type from the underlying driver's metadata lookup
     * @param nativeType the database native type or -1 for unknown
     * @return the resolved JDBC type
     */
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        return metadataJdbcType;
    }

    private void cleanupPreparedStatement(PreparedStatement statement) {
        LOGGER.trace("Closing prepared statement '{}' removed from cache", statement);
        try {
            statement.close();
        }
        catch (Exception e) {
            LOGGER.info("Exception while closing a prepared statement removed from cache", e);
        }
    }

    private PreparedStatement createPreparedStatement(String preparedQueryString) {
        return statementCache.computeIfAbsent(preparedQueryString, query -> {
            try {
                LOGGER.trace("Inserting prepared statement '{}' removed from the cache", query);
                return connection().prepareStatement(query);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Executes a series of statements without explicitly committing the connection.
     *
     * @param statements a series of statements to execute
     * @return this object so methods can be chained together; never null
     * @throws SQLException if anything fails
     */
    public JdbcConnection executeWithoutCommitting(String... statements) throws SQLException {
        Connection conn = connection();
        try (Statement statement = conn.createStatement()) {
            for (String stmt : statements) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Executing statement {}", stmt);
                }
                statement.execute(stmt);
            }
        }
        return this;
    }

    protected static boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }

    public <T> ResultSetMapper<T> singleResultMapper(ResultSetExtractor<T> extractor, String error) throws SQLException {
        return (rs) -> {
            if (rs.next()) {
                final T ret = extractor.apply(rs);
                if (!rs.next()) {
                    return ret;
                }
            }
            throw new IllegalStateException(error);
        };
    }

    public static <T> T querySingleValue(Connection connection, String queryString, StatementPreparer preparer, ResultSetExtractor<T> extractor) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(queryString);
        preparer.accept(preparedStatement);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                final T result = extractor.apply(resultSet);
                if (!resultSet.next()) {
                    return result;
                }
            }
            throw new IllegalStateException("Exactly one result expected.");
        }
    }
}
