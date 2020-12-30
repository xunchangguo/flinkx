/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.flinkx.sqlservercdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SqlServerChangeTablePointer {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerChangeTablePointer.class);

    private static final int COL_COMMIT_LSN = 1;
    private static final int COL_ROW_LSN = 2;
    private static final int COL_OPERATION = 3;
    private static final int COL_DATA = 5;

    private final SqlServerChangeTable changeTable;
    private final ResultSet resultSet;
    private final int columnDataOffset;
    private boolean completed = false;
    private TxLogPosition currentChangePosition;


    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, ResultSet resultSet) {
        this(changeTable, resultSet, COL_DATA);
    }

    public SqlServerChangeTablePointer(SqlServerChangeTable changeTable, ResultSet resultSet, int columnDataOffset) {
        this.changeTable = changeTable;
        this.resultSet = resultSet;
        this.columnDataOffset = columnDataOffset;
    }


    public SqlServerChangeTable getChangeTable() {
        return changeTable;
    }

    public TxLogPosition getChangePosition() throws SQLException {
        return currentChangePosition;
    }

    public int getOperation() throws SQLException {
        return resultSet.getInt(COL_OPERATION);
    }

    public Object[] getData() throws SQLException {
        final int dataColumnCount = resultSet.getMetaData().getColumnCount() - (columnDataOffset - 1);
        final Object[] data = new Object[dataColumnCount];
        for (int i = 0; i < dataColumnCount; ++i) {
            data[i] = resultSet.getObject(columnDataOffset + i);
        }
        return data;
    }

    public boolean next() throws SQLException {
        completed = !resultSet.next();
        currentChangePosition = completed ? TxLogPosition.NULL : TxLogPosition.valueOf(Lsn.valueOf(resultSet.getBytes(COL_COMMIT_LSN)), Lsn.valueOf(resultSet.getBytes(COL_ROW_LSN)));
        if (completed) {
            LOG.debug("Closing result set of change tables for table {}", changeTable);
            resultSet.close();
        }
        return !completed;
    }

    public boolean isCompleted() {
        return completed;
    }

    public int compareTo(SqlServerChangeTablePointer o) throws SQLException {
        return getChangePosition().compareTo(o.getChangePosition());
    }

    @Override
    public String toString() {
        return "ChangeTablePointer [changeTable=" + changeTable + ", resultSet=" + resultSet + ", completed="
                + completed + ", currentChangePosition=" + currentChangePosition + "]";
    }
}
