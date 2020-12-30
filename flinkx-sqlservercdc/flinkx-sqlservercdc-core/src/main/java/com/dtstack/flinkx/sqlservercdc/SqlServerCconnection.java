/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.sqlservercdc;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlServerCconnection extends JdbcConnection {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerCconnection.class);

    private static final String CLOB_STRING = "CLOB";
    public static final String DRIVER = "net.sourceforge.jtds.jdbc.Driver";
//    public static Pattern p = Pattern.compile("\\[(.*?)]");
    private static final Pattern BRACKET_PATTERN = Pattern.compile("[\\[\\]]");

    private static final String SQL_SERVER_VERSION = "SELECT @@VERSION AS 'SQL Server Version'";
    private static final String LOCK_TABLE = "SELECT * FROM [#] WITH (TABLOCKX)";
    private static final String STATEMENTS_PLACEHOLDER = "#";
    private static final String CHECK_CDC_DATABASE = "select 1 from sys.databases where name='%s' AND is_cdc_enabled=1";
    private static final String CHECK_CDC_TABLE = "select sys.schemas.name+'.'+sys.tables.name from sys.tables, sys.schemas where sys.tables.is_tracked_by_cdc = 1 and sys.tables.schema_id = sys.schemas.schema_id;";
    private static final String GET_LIST_OF_CDC_ENABLED_TABLES = "EXEC sys.sp_cdc_help_change_data_capture";
    private static final String GET_MAX_LSN = "SELECT sys.fn_cdc_get_max_lsn()";
    private static final String GET_MIN_LSN = "SELECT sys.fn_cdc_get_min_lsn('#')";
    private static final String INCREMENT_LSN = "SELECT sys.fn_cdc_increment_lsn(?)";
    private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT * FROM cdc.[fn_cdc_get_all_changes_#](ISNULL(?,sys.fn_cdc_get_min_lsn('#')), ?, N'all update old')";
    //private static final String GET_ALL_CHANGES_FOR_TABLE = "SELECT * FROM cdc.[fn_cdc_get_all_changes_#](?, ?, N'all update old')";
    private static final String GET_LIST_OF_NEW_CDC_ENABLED_TABLES = "SELECT * FROM cdc.change_tables WHERE start_lsn BETWEEN ? AND ?";
    private static final String GET_LIST_OF_KEY_COLUMNS = "SELECT * FROM cdc.index_columns WHERE object_id=?";

    public SqlServerCconnection(String username, String password, String url, String databaseName) {
        super(username, password, url, databaseName);
    }

    public boolean checkEnabledCdcDatabase(String databaseName) throws SQLException {
        return queryAndMap(String.format(CHECK_CDC_DATABASE, databaseName), rs -> rs.next());
    }

    public Set<String> checkUnEnabledCdcTables(Collection<String> tableSet) throws SQLException {
        return queryAndMap(CHECK_CDC_TABLE, rs -> {
            CopyOnWriteArraySet<String> unEnabledCdcTables = new CopyOnWriteArraySet<>(tableSet);
            while(rs.next()) {
                String tableName = rs.getString(1);
                unEnabledCdcTables.remove(tableName);
            }
            return unEnabledCdcTables;
        });
    }

    public Set<SqlServerChangeTable> listOfChangeTables(String databaseName) throws SQLException {
        final String query = GET_LIST_OF_CDC_ENABLED_TABLES;

        return queryAndMap(query, rs -> {
            final Set<SqlServerChangeTable> changeTables = new HashSet<>();
            while (rs.next()) {
                changeTables.add(
                        new SqlServerChangeTable(
                                new TableId(databaseName, rs.getString(1), rs.getString(2)),
                                rs.getString(3),
                                rs.getInt(4),
                                Lsn.valueOf(rs.getBytes(6)),
                                Lsn.valueOf(rs.getBytes(7)),
                                Arrays.asList(BRACKET_PATTERN.matcher(Optional.ofNullable(rs.getString(15)).orElse(""))
                                        .replaceAll("").split(", "))));
            }
            return changeTables;
        });
    }

    public Lsn getMaxLsn() throws SQLException {
        return queryAndMap(GET_MAX_LSN, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOG.trace("Current maximum lsn is {}", ret);
            return ret;
        }, "Maximum LSN query must return exactly one value"));
    }

    public SqlServerChangeTable[] getCdcTablesToQuery(String databaseName, List<String> tableList) throws SQLException {
        Set<SqlServerChangeTable> cdcEnabledTableSet = listOfChangeTables(databaseName);

        if (cdcEnabledTableSet.isEmpty()) {
            LOG.error("No table has enabled CDC or security constraints prevents getting the list of change tables");
        }

        Map<TableId, List<SqlServerChangeTable>> whitelistedCdcEnabledTables = cdcEnabledTableSet.stream()
                .filter(changeTable -> {
                    String tableName = changeTable.getSourceTableId().schema() + "." + changeTable.getSourceTableId().table();
                    return tableList.contains(tableName);
                })
                .collect(Collectors.groupingBy(ChangeTable::getSourceTableId));

        List<ChangeTable> changeTableList = new ArrayList<>();
        for (List<SqlServerChangeTable> captures : whitelistedCdcEnabledTables.values()) {
            SqlServerChangeTable currentTable = captures.get(0);
            if (captures.size() > 1) {
                SqlServerChangeTable futureTable;
                if (captures.get(0).getStartLsn().compareTo(captures.get(1).getStartLsn()) < 0) {
                    futureTable = captures.get(1);
                } else {
                    currentTable = captures.get(1);
                    futureTable = captures.get(0);
                }
                currentTable.setStopLsn(futureTable.getStartLsn());
                changeTableList.add(futureTable);
                LOG.info("Multiple capture instances present for the same table: {} and {}", currentTable, futureTable);
            }
            changeTableList.add(currentTable);
        }

        return changeTableList.toArray(new SqlServerChangeTable[0]);
    }

    public Lsn incrementLsn(Lsn lsn) throws SQLException {
        final String query = INCREMENT_LSN;
        return prepareQueryAndMap(query, statement -> {
            statement.setBytes(1, lsn.getBinary());
        }, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOG.trace("Increasing lsn from {} to {}", lsn, ret);
            return ret;
        }, "Increment LSN query must return exactly one value"));
    }

    public void getChangesForTables(SqlServerChangeTable[] changeTables, Lsn intervalFromLsn, Lsn intervalToLsn, BlockingMultiResultSetConsumer consumer)
            throws SQLException, InterruptedException {
        final String[] queries = new String[changeTables.length];
        final StatementPreparer[] preparers = new StatementPreparer[changeTables.length];

        int idx = 0;
        for (SqlServerChangeTable changeTable : changeTables) {
            final String query = GET_ALL_CHANGES_FOR_TABLE.replace(STATEMENTS_PLACEHOLDER, changeTable.getCaptureInstance());
            queries[idx] = query;
            // If the table was added in the middle of queried buffer we need
            // to adjust from to the first LSN available
            final Lsn fromLsn = getFromLsn(changeTable, intervalFromLsn);
            LOG.trace("Getting changes for table {} in range[{}, {}]", changeTable, fromLsn, intervalToLsn);
            preparers[idx] = statement -> {
                statement.setBytes(1, fromLsn.getBinary());
                statement.setBytes(2, intervalToLsn.getBinary());
            };

            idx++;
        }
        prepareQuery(queries, preparers, consumer);
    }

    private Lsn getFromLsn(SqlServerChangeTable changeTable, Lsn intervalFromLsn) throws SQLException {
        Lsn fromLsn = changeTable.getStartLsn().compareTo(intervalFromLsn) > 0 ? changeTable.getStartLsn() : intervalFromLsn;
        return fromLsn.getBinary() != null ? fromLsn : getMinLsn(changeTable.getCaptureInstance());
    }

    /**
     * @return the smallest log sequence number of table
     */
    public Lsn getMinLsn(String changeTableName) throws SQLException {
        String query = GET_MIN_LSN.replace(STATEMENTS_PLACEHOLDER, changeTableName);
        return queryAndMap(query, singleResultMapper(rs -> {
            final Lsn ret = Lsn.valueOf(rs.getBytes(1));
            LOG.trace("Current minimum lsn is {}", ret);
            return ret;
        }, "Minimum LSN query must return exactly one value"));
    }

    /**
     * clob转string
     * @param obj   clob
     * @return
     */
    public static Object clobToString(Object obj) {
        if(obj instanceof Clob){
            Clob clob = (Clob)obj;
            return getAsString(clob);
        } else {
            return obj;
        }
    }

    private static String getAsString(Clob clob) {
        Reader reader = null;
        BufferedReader bufferedReader = null;
        try {
            reader = clob.getCharacterStream();
            bufferedReader = new BufferedReader(reader);
            return IOUtils.toString(bufferedReader);
        } catch (Exception e) {
            throw new RuntimeException("Error while reading String from CLOB", e);
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(bufferedReader);
        }
    }
}
