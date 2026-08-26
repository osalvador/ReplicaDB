#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
state_directory=$(mktemp -d "${TMPDIR:-/tmp}/replicadb-cli-compatibility.XXXXXX")

cleanup() {
    rm -rf "$state_directory"
}
trap cleanup EXIT

if [[ -z "${JAVA_HOME:-}" && "$(uname)" = Darwin && -x /usr/libexec/java_home ]]; then
    JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || true)
    export JAVA_HOME
fi
if [[ -z "${JAVA_HOME:-}" || ! -x "$JAVA_HOME/bin/java" || ! -x "$JAVA_HOME/bin/jar" ]]; then
    printf 'A Java 17 JAVA_HOME is required for the CLI compatibility gate\n' >&2
    exit 2
fi

printf 'phase3 CLI compatibility: run root tests\n'
mvn -B -Dtest=NoSpringBootOnClasspathTest,CliOfflineExecutionTest,ReplicaDBRunCountersTest,ReplicaDBCancellationTest,ToolOptionsMultipleTablesTest,ToolOptionsIncrementalWatermarkTest \
    test -f "$repository_root/pom.xml" >/dev/null

printf 'phase3 CLI compatibility: package standalone artifact\n'
mvn -B -Ptest -DskipTests package -f "$repository_root/pom.xml" >/dev/null
cli_jar=$(find "$repository_root/target" -maxdepth 1 -type f -name '*-jar-with-dependencies.jar' -print -quit)
test -n "$cli_jar"

jar_contents=$($JAVA_HOME/bin/jar tf "$cli_jar")
grep -F 'org/replicadb/ReplicaDB.class' <<< "$jar_contents" >/dev/null
if grep -F 'org/springframework/boot/' <<< "$jar_contents" >/dev/null; then
    printf 'Spring Boot classes found in the standalone CLI artifact\n' >&2
    exit 1
fi

dependency_tree=$(mvn -q -DskipTests dependency:tree -f "$repository_root/pom.xml")
if grep -E 'org\.springframework|spring-boot|micrometer|quartz' <<< "$dependency_tree" >/dev/null; then
    printf 'server-only dependencies found in the root CLI dependency tree\n' >&2
    exit 1
fi
if grep -F '<groupId>org.springframework' "$repository_root/pom.xml" >/dev/null; then
    printf 'Spring dependency declared in the root CLI POM\n' >&2
    exit 1
fi

source_url="jdbc:sqlite:$state_directory/source.db"
sink_url="jdbc:sqlite:$state_directory/sink.db"
$JAVA_HOME/bin/jshell --class-path "$cli_jar" <<EOF >/dev/null
import java.sql.*;
Class.forName("org.sqlite.JDBC");
var source = DriverManager.getConnection("$source_url");
var sourceStatement = source.createStatement();
sourceStatement.execute("CREATE TABLE source_items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)");
sourceStatement.execute("INSERT INTO source_items (id, payload) VALUES (1, 'one'), (2, 'two')");
source.close();
var sink = DriverManager.getConnection("$sink_url");
var sinkStatement = sink.createStatement();
sinkStatement.execute("CREATE TABLE sink_items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)");
sink.close();
/exit
EOF

options_file="$state_directory/replicadb.properties"
printf '%s\n' \
    'mode=complete' \
    'jobs=1' \
    "source.connect=$source_url" \
    'source.table=source_items' \
    "sink.connect=$sink_url" \
    'sink.table=sink_items' > "$options_file"

printf 'phase3 CLI compatibility: execute packaged CLI offline\n'
env -u DB_URL -u DB_USERNAME -u DB_PASSWORD -u SPRING_PROFILES_ACTIVE \
    "$JAVA_HOME/bin/java" -jar "$cli_jar" --options-file "$options_file" > "$state_directory/success.log" 2>&1

row_count_output=$($JAVA_HOME/bin/jshell --class-path "$cli_jar" <<EOF
import java.sql.*;
Class.forName("org.sqlite.JDBC");
var connection = DriverManager.getConnection("$sink_url");
var result = connection.createStatement().executeQuery("SELECT COUNT(*) FROM sink_items");
result.next();
System.out.println("ROW_COUNT=" + result.getLong(1));
connection.close();
/exit
EOF
)
row_count=$(sed -nE 's/.*ROW_COUNT=([0-9]+).*/\1/p' <<< "$row_count_output" | tail -1)
test "$row_count" = 2

env -u DB_URL -u DB_USERNAME -u DB_PASSWORD -u SPRING_PROFILES_ACTIVE \
    "$JAVA_HOME/bin/java" -jar "$cli_jar" --help >/dev/null

set +e
env -u DB_URL -u DB_USERNAME -u DB_PASSWORD -u SPRING_PROFILES_ACTIVE \
    "$JAVA_HOME/bin/java" -jar "$cli_jar" --jobs not-a-number > "$state_directory/error.log" 2>&1
error_code=$?
set -e
test "$error_code" = 1

printf 'phase3 CLI compatibility passed artifact=%s rows=%s error_exit=%s\n' \
    "$(basename "$cli_jar")" "$row_count" "$error_code"
