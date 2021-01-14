/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dtstack.flinkx.sqlservercdc;

import java.util.List;
import java.util.Objects;

public class ChangeTable {

    private final String captureInstance;
    private final TableId sourceTableId;
    private final TableId changeTableId;
    private final int changeTableObjectId;

    /**
     * Creates an object that represents a source table's change table.
     *
     * @param captureInstance the logical name of the change capture process
     * @param sourceTableId the table from which the changes are captured
     * @param changeTableId the table that contains the changes for the source table
     * @param changeTableObjectId the numeric identifier for the change table in the source database
     */
    public ChangeTable(String captureInstance, TableId sourceTableId, TableId changeTableId, int changeTableObjectId) {
        this.captureInstance = captureInstance;
        this.sourceTableId = sourceTableId;
        this.changeTableId = changeTableId;
        this.changeTableObjectId = changeTableObjectId;
    }

    public String getCaptureInstance() {
        return captureInstance;
    }

    public TableId getSourceTableId() {
        return sourceTableId;
    }

    public TableId getChangeTableId() {
        return changeTableId;
    }

    public int getChangeTableObjectId() {
        return changeTableObjectId;
    }

    @Override
    public String toString() {
        return "ChangeTable{" +
                "captureInstance='" + captureInstance + '\'' +
                ", sourceTableId=" + sourceTableId +
                ", changeTableId=" + changeTableId +
                ", changeTableObjectId=" + changeTableObjectId +
                '}';
    }
}
