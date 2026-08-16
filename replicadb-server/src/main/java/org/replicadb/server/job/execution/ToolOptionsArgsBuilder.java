package org.replicadb.server.job.execution;

import org.replicadb.server.job.domain.JobDefinition;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

@Component
public class ToolOptionsArgsBuilder {

    public String[] build(JobDefinition definition, String previousWatermarkValue) {
        return build(definition, previousWatermarkValue, Function.identity());
    }

    String[] build(JobDefinition definition, String previousWatermarkValue,
                   Function<String, String> valueResolver) {
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(valueResolver, "valueResolver must not be null");

        List<String> arguments = new ArrayList<>();
        add(arguments, "--source-connect", valueResolver.apply(definition.sourceConnect()));
        addIfPresent(arguments, "--source-user", valueResolver.apply(definition.sourceUser()));
        addIfPresent(arguments, "--source-password", valueResolver.apply(definition.sourcePassword()));
        add(arguments, "--source-table", definition.sourceTable());
        addIfPresent(arguments, "--source-where", definition.sourceWhere());
        add(arguments, "--sink-connect", valueResolver.apply(definition.sinkConnect()));
        addIfPresent(arguments, "--sink-user", valueResolver.apply(definition.sinkUser()));
        addIfPresent(arguments, "--sink-password", valueResolver.apply(definition.sinkPassword()));
        add(arguments, "--sink-table", definition.sinkTable());
        add(arguments, "--mode", definition.mode().getModeText());
        add(arguments, "--jobs", Integer.toString(definition.jobs()));

        if (definition.incrementalWatermarkColumn() != null) {
            add(arguments, "--incremental-watermark-column", definition.incrementalWatermarkColumn());
            String watermarkValue = previousWatermarkValue != null
                    ? previousWatermarkValue
                    : definition.initialWatermarkValue();
            addIfPresent(arguments, "--incremental-watermark-value", watermarkValue);
        }

        return arguments.toArray(String[]::new);
    }

    private static void add(List<String> arguments, String option, String value) {
        arguments.add(option);
        arguments.add(value);
    }

    private static void addIfPresent(List<String> arguments, String option, String value) {
        if (value != null) {
            add(arguments, option, value);
        }
    }
}