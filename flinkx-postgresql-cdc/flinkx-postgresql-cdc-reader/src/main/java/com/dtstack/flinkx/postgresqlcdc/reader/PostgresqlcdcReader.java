package com.dtstack.flinkx.postgresqlcdc.reader;

import com.alibaba.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.postgresqlcdc.format.RowDebeziumDeserializeSchema;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PostgresqlcdcReader extends BaseDataReader {
    private static final String PLUGIN_NAME_KEY = "pluginName";
    private static final String HOSTNAME_KEY = "hostname";
    private static final String PORT_KEY = "port";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String DATABASE_NAME_KEY = "database-name";
    private static final String TABLE_NAME_KEY = "table-names";
    private static final String SERVER_TIME_ZONE_KEY = "serverTimeZone";
    private static final String SCHEMA_NAME_KEY = "schemas";
    private static final String PROPERTIES_KEY = "properties";

    private String hostname;
    private int port = 5432;
    private String username;
    private String password;
    private String databaseName;
    private List<String> tableNames;
    private List<String> schemas;
    private String serverTimeZone = "UTC";
    private String pluginName = "decoderbufs";
    private Properties properties;


    public PostgresqlcdcReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        pluginName = readerConfig.getParameter().getStringVal(PLUGIN_NAME_KEY, "decoderbufs");
        hostname = readerConfig.getParameter().getStringVal(HOSTNAME_KEY);
        port = readerConfig.getParameter().getIntVal(PORT_KEY, 5432);
        username = readerConfig.getParameter().getStringVal(USERNAME_KEY);
        password = readerConfig.getParameter().getStringVal(PASSWORD_KEY);
        databaseName = readerConfig.getParameter().getStringVal(DATABASE_NAME_KEY);
        tableNames = (List<String>) readerConfig.getParameter().getVal(TABLE_NAME_KEY);
        schemas = (List<String>) readerConfig.getParameter().getVal(SCHEMA_NAME_KEY);
        serverTimeZone = readerConfig.getParameter().getStringVal(SERVER_TIME_ZONE_KEY, "UTC");

        properties = new Properties();
        Map<String, Object> mp = (Map<String, Object>) readerConfig.getParameter().getVal(PROPERTIES_KEY);
        if(mp != null && !mp.isEmpty()) {
            for(String key : mp.keySet()) {
                properties.put(key, mp.get(key));
            }
        }
    }

    @Override
    public DataStream<Row> readData() {
        TypeInformation<Row> typeInformation = TypeInformation.of(Row.class);
        RowDebeziumDeserializeSchema deserializer = new RowDebeziumDeserializeSchema(
                typeInformation,
                ((rowData, rowKind) -> {}),
                ZoneId.of(serverTimeZone));
        DebeziumSourceFunction<Row> cdcSource = PostgreSQLSource.<Row>builder()
                .decodingPluginName(pluginName)
                .hostname(hostname)
                .port(port)
                .username(username)
                .password(password)
                .database(databaseName)
                .tableList(tableNames.toArray(new String[tableNames.size()]))
                .schemaList(schemas.toArray(new String[schemas.size()]))
                .debeziumProperties(properties)
                .deserializer(deserializer)
                .build();
        DataStreamSource dataStreamSource = env.addSource(cdcSource, String.format("source-%s", databaseName));

        return dataStreamSource;
    }
}
