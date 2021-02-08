package com.dtstack.flinkx.postgresqlcdc.format;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RowDebeziumDeserializeSchema implements DebeziumDeserializationSchema<Row> {
    private static final long serialVersionUID = -4852684966051743776L;
    private static final String FIELD_OP = "op";
    private static final String FIELD_SOURCE_DB = "source_db";
    private static final String FIELD_STRUCT_DB = "db";
    private static final String FIELD_SOURCE_SCHEMA = "source_schema";
    private static final String FIELD_STRUCT_SCHEMA = "schema";
    private static final String FIELD_SOURCE_TABLE = "source_table";
    private static final String FIELD_STRUCT_TABLE = "table";
    private static final String FIELD_STRUCT_TIMESTAMP = "ts_ms";
    private static final String FIELD_STRUCT_DATA = "data";

    /**
     * Custom validator to validate the row value.
     */
    public interface ValueValidator extends Serializable {
        void validate(Row row, RowKind rowKind) throws Exception;
    }

    /** TypeInformation of the produced {@link Row}. **/
    private final TypeInformation<Row> resultTypeInfo;

    /**
     * Time zone of the database server.
     */
    private final ZoneId serverTimeZone;

    /**
     * Validator to validate the row value.
     */
    private final ValueValidator validator;

    public RowDebeziumDeserializeSchema(TypeInformation<Row> resultTypeInfo, ValueValidator validator, ZoneId serverTimeZone) {
        this.resultTypeInfo = resultTypeInfo;
        this.validator = validator;
        this.serverTimeZone = serverTimeZone;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Row> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            Row insert = extractAfterRow(value, valueSchema, op);
            validator.validate(insert, RowKind.INSERT);
//            insert.setRowKind(RowKind.INSERT);
            out.collect(insert);
        } else if (op == Envelope.Operation.DELETE) {
            //TODO
            Row delete = extractBeforeRow(value, valueSchema, op);
            validator.validate(delete, RowKind.DELETE);
//            delete.setRowKind(RowKind.DELETE);
            out.collect(delete);
        } else {
            //TODO
            //update
//            Row before = extractBeforeRow(value, valueSchema);
//            validator.validate(before, RowKind.UPDATE_BEFORE);
//            before.setRowKind(RowKind.UPDATE_BEFORE);
//            out.collect(before);

            Row after = extractAfterRow(value, valueSchema, op);
            validator.validate(after, RowKind.UPDATE_AFTER);
//            after.setRowKind(RowKind.UPDATE_AFTER);
            out.collect(after);
        }
    }

    private Row extractAfterRow(Struct value, Schema valueSchema, Envelope.Operation op) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct struct = value.getStruct(Envelope.FieldName.AFTER);
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        return createRow(op, afterSchema, struct, source);
    }

    private Row createRow(Envelope.Operation op, Schema schema, Struct struct, Struct source) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(FIELD_OP, op.code());
        map.put(FIELD_SOURCE_DB, source.get(FIELD_STRUCT_DB));
        map.put(FIELD_SOURCE_SCHEMA, source.get(FIELD_STRUCT_SCHEMA));
        map.put(FIELD_SOURCE_TABLE, source.get(FIELD_STRUCT_TABLE));
        map.put(FIELD_STRUCT_TIMESTAMP, source.get(FIELD_STRUCT_TIMESTAMP));
        Map<String, Object> valueMap = new LinkedHashMap<>();
        List<Field> fields = schema.fields();
        for(Field field : fields) {
            valueMap.put(field.name(), struct.get(field));
        }
        map.put(FIELD_STRUCT_DATA, valueMap);

        return Row.of(map);
    }

    private Row extractBeforeRow(Struct value, Schema valueSchema, Envelope.Operation op) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct struct = value.getStruct(Envelope.FieldName.BEFORE);
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        return createRow(op, afterSchema, struct, source);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return resultTypeInfo;
    }

}
