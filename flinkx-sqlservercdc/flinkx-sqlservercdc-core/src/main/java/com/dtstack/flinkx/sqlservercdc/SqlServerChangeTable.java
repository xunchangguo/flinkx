package com.dtstack.flinkx.sqlservercdc;

import java.util.Collections;
import java.util.List;

public class SqlServerChangeTable extends ChangeTable {

    private static final String CDC_SCHEMA = "cdc";

    /**
     * A LSN from which the data in the change table are relevant
     */
    private final Lsn startLsn;

    /**
     * A LSN to which the data in the change table are relevant
     */
    private Lsn stopLsn;

    /**
     * List of columns captured by the CDC table.
     */
    private final List<String> capturedColumns;

    public SqlServerChangeTable(TableId sourceTableId, String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn,
                                List<String> capturedColumns) {
        super(captureInstance, sourceTableId, resolveChangeTableId(sourceTableId, captureInstance), changeTableObjectId);
        this.startLsn = startLsn;
        this.stopLsn = stopLsn;
        this.capturedColumns = capturedColumns != null ? Collections.unmodifiableList(capturedColumns) : Collections.emptyList();
    }

    public SqlServerChangeTable(String captureInstance, int changeTableObjectId, Lsn startLsn, Lsn stopLsn) {
        this(null, captureInstance, changeTableObjectId, startLsn, stopLsn, null);
    }

    public Lsn getStartLsn() {
        return startLsn;
    }

    public Lsn getStopLsn() {
        return stopLsn;
    }

    public void setStopLsn(Lsn stopLsn) {
        this.stopLsn = stopLsn;
    }

    public List<String> getCapturedColumns() {
        return capturedColumns;
    }

    @Override
    public String toString() {
        return "Capture instance \"" + getCaptureInstance() + "\" [sourceTableId=" + getSourceTableId()
                + ", changeTableId=" + getChangeTableId() + ", startLsn=" + startLsn + ", changeTableObjectId="
                + getChangeTableObjectId() + ", stopLsn=" + stopLsn + "]";
    }

    private static TableId resolveChangeTableId(TableId sourceTableId, String captureInstance) {
        return sourceTableId != null ? new TableId(sourceTableId.catalog(), CDC_SCHEMA, captureInstance + "_CT") : null;
    }
}
