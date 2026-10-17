package com.example.vehicle;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DrivingSafetyJob {
    private static final Path SQL_FILE = Path.of("/opt/flink/sql/safety.sql");

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        // Without checkpointing Flink also switches restarts off, so losing a
        // TaskManager kills the job outright — and since offsets are only
        // committed when a checkpoint succeeds, a fresh start re-reads the whole
        // topic. Checkpointing is what makes operator state and read position
        // recoverable together.
        Configuration conf = new Configuration();
        conf.setString("execution.checkpointing.interval", "10 s");
        conf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        conf.setString("execution.checkpointing.min-pause", "5 s");
        conf.setString("execution.checkpointing.timeout", "60 s");
        // Keep the last checkpoint when the job is cancelled, so a restart can
        // resume from it rather than from the start of the topic.
        conf.setString("execution.checkpointing.externalized-checkpoint-retention",
                "RETAIN_ON_CANCELLATION");
        conf.setString("restart-strategy.type", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "3");
        conf.setString("restart-strategy.fixed-delay.delay", "10 s");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // A Kafka partition that stops receiving data would otherwise hold the
        // global watermark back forever (watermark = min across partitions), so
        // the last windows of a replay never close. Mark quiet partitions idle.
        // Overridable because it was the first suspect for the records dropped
        // as late after a recovery, and turning it off changed nothing (1,778
        // dropped against 1,853) — the cause was the watermark grace being
        // narrower than the rewind. The knob stays so that result can be
        // reproduced rather than taken on trust.
        tableEnv.getConfig().set("table.exec.source.idle-timeout",
                System.getenv().getOrDefault("IDLE_TIMEOUT", "10 s"));

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
