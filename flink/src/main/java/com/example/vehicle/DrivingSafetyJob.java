package com.example.vehicle;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DrivingSafetyJob {
    private static final Path SQL_FILE = Path.of("/opt/flink/sql/safety.sql");

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
                        .getExecutionEnvironment(),
                settings);

        // A Kafka partition that stops receiving data would otherwise hold the
        // global watermark back forever (watermark = min across partitions), so
        // the last windows of a replay never close. Mark quiet partitions idle.
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "10 s");

        String sqlScript = Files.readString(SQL_FILE);
        List<String> insertStatements = new ArrayList<>();

        for (String statement : sqlScript.split("(?m);\\s*$")) {
            String trimmed = stripLeadingComments(statement);
            if (trimmed.isEmpty()) continue;
            if (trimmed.toUpperCase().startsWith("INSERT")) {
                insertStatements.add(trimmed);
            } else {
                tableEnv.executeSql(trimmed);
            }
        }

        if (insertStatements.isEmpty()) {
            throw new IllegalStateException("No INSERT statements found in " + SQL_FILE);
        }

        StatementSet stmtSet = tableEnv.createStatementSet();
        for (String insert : insertStatements) {
            stmtSet.addInsertSql(insert);
        }
        stmtSet.execute().await();
    }

    /** Drops leading blank and "--" comment lines so a statement can be classified
     *  by its first keyword. Comments document why each threshold was chosen, so
     *  they must not stop an INSERT from being recognised. */
    private static String stripLeadingComments(String statement) {
        String[] lines = statement.split("\n");
        int i = 0;
        while (i < lines.length) {
            String line = lines[i].trim();
            if (line.isEmpty() || line.startsWith("--")) {
                i++;
            } else {
                break;
            }
        }
        return String.join("\n", Arrays.copyOfRange(lines, i, lines.length)).trim();
    }
}
