const reference = '/reference/cli-options/';
const destinationOptions = reference + '#destination-preparation-and-staging';
const diagnostics = reference + '#parallelism-and-diagnostics';
const connectors = '/connectors/';
const configuration = '/cli/configuration/';
const modes = '/cli/replication-modes/';
const parallelism = '/cli/parallelism/';
const filtering = '/cli/filtering-and-queries/';
const watermark = '/cli/incremental-watermarks/';
const multiTable = '/cli/multi-table/';
const csv = '/connectors/csv/';
const s3 = '/connectors/amazon-s3/';
const kafka = '/connectors/kafka/#sink-connection-parameters';
const jdbc = '/connectors/generic-jdbc/#connection-parameters';
const entra = '/connectors/sql-server/#microsoft-entra-authentication';

const generalHelp = {
  'source-type': ['Select the system ReplicaDB will read from. Only connectors that can be sources appear here.', connectors],
  'sink-type': ['Select the system ReplicaDB will write to. This choice affects available modes and write options.', connectors],
  mode: ['Choose whether to replace the destination, stage a full replacement, or apply a selected batch of changes.', modes],
  scope: ['Read a single table or endpoint, use a custom query where supported, or replicate an ordered list of table pairs.', filtering],
  jobs: ['Sets the number of parallel tasks per table. Some connectors and interactive Entra authentication require one job.', parallelism],
  'fetch.size': ['Sets how many rows a reader requests at a time. Larger batches can increase throughput and memory use.', parallelism],
  verbose: ['Controls the CLI log level. DEBUG includes more detail than the default INFO level.', '/cli/troubleshooting/'],
  'quoted.identifiers': ['Quotes SQL identifiers when ReplicaDB generates statements. Use it for case-sensitive or reserved names where the connector supports quoting.', diagnostics],
  'sentry.dsn': ['Enables optional Sentry error reporting when a DSN is configured. Omit this setting to leave telemetry disabled.', diagnostics],
  'bandwidth.throttling': ['Caps transfer speed in KB/s per worker. Zero leaves it unlimited.', parallelism],
  'incremental.watermark.column': ['Select the source column used to find rows above the previous successful value.', watermark],
  'incremental.watermark.value': ['Set the last successful watermark. Leave this empty to read all matching rows on the first run.', watermark],
  'sink.disable.escape': ['Skips escaping backslashes and line breaks on PostgreSQL text COPY and MySQL/MariaDB text bulk loads.', destinationOptions],
  'sink.disable.index': ['The CLI accepts this setting. Current connectors do not change indexes based on its value.', destinationOptions],
  'sink.auto.create': ['Creates a missing SQL destination table from source metadata where the connector supports it.', destinationOptions],
  'sink.analyze': ['The CLI accepts this setting. Current connectors do not update table statistics based on its value.', destinationOptions],
  'sink.disable.truncate': ['Keeps existing rows when complete mode prepares a supported destination. Check keys and duplicates before writing into a populated table.', modes + '#complete'],
  'sink.staging.schema': ['Names the schema where ReplicaDB may create a staging table. The destination account needs permission to create it.', destinationOptions],
  'sink.staging.table': ['Uses a staging table or collection that already exists. Supply its qualified name if required by the connector.', destinationOptions],
  'sink.staging.table.alias': ['Uses this value as the prefix for the generated staging table name.', destinationOptions],
  'pair.source': ['Names the table or collection ReplicaDB reads for this pair.', multiTable],
  'pair.sink': ['Names the destination table or collection for this pair. Pairs run in the order shown.', multiTable]
};

const endpointHelp = {
  user: ['Sets the database account ReplicaDB uses for this connection.', configuration + '#connection-settings'],
  password: ['Supplies the connection password or service principal secret. Use an environment reference to keep the resolved value out of the file.', configuration + '#connection-settings'],
  'auth.mode': ['Selects the Microsoft Entra authentication flow for this SQL Server connection.', entra],
  'auth.principal.id': ['Sets the service principal ID or the managed identity client ID used by the selected authentication flow.', entra],
  'auth.login.hint': ['Suggests the account to use for interactive sign-in.', entra],
  'auth.client.certificate': ['Points to the client certificate used to authenticate a service principal.', entra],
  'auth.client.key': ['Supplies the private key separately when required by the client certificate format.', entra],
  table: ['Names the source or destination table. Qualify it with its schema if needed.', filtering],
  columns: ['Lists the selected source columns or destination column mapping in order.', filtering],
  where: ['Limits rows read from the source table. Enter the predicate without the WHERE keyword.', filtering],
  query: ['Supplies the SQL statement ReplicaDB uses to read rows. It defines the projection and joins.', filtering],
  'file.format': ['Selects the data format used to read or write the file. The supported formats are CSV and ORC.', csv + '#selecting-the-format']
};

const parameterHelp = {
  driver: ['Gives the fully qualified JDBC driver class. Install the driver in the CLI lib directory.', jdbc],
  user: endpointHelp.user,
  password: endpointHelp.password,
  topic: ['Selects the Kafka topic that receives the replicated records.', kafka],
  partition: ['Sends records to a fixed Kafka partition. Leave it empty to let the producer choose.', kafka],
  key: ['Sets a fixed Kafka message key for every record in the transfer.', kafka],
  acks: ['Controls how many broker acknowledgements the producer waits for.', kafka],
  accessKey: ['References the access key ID used to write to the S3 endpoint.', s3 + '#properties'],
  secretKey: ['References the secret access key used with the access key ID. Prefer an environment reference.', s3 + '#properties'],
  'secure-connection': ['Uses HTTPS for the S3 endpoint by default.', s3 + '#properties'],
  'row.isObject': ['Writes one object for each source row when enabled. Otherwise ReplicaDB writes file output.', s3 + '#object-per-row-mode'],
  'row.keyColumn': ['Names the source column that supplies each S3 object key.', s3 + '#object-per-row-mode'],
  'row.contentColumn': ['Names the source column whose value becomes the S3 object body.', s3 + '#object-per-row-mode'],
  keyFileName: ['Sets the S3 object key for file output.', s3 + '#file-mode'],
  'orc.compression': ['Selects the compression codec for ORC output.', csv + '#orc-limits'],
  'columns.types': ['Lists one JDBC type per CSV input column, in file order.', csv + '#source-column-types'],
  format: ['Selects a preset for the delimiter, quoting, and other CSV rules.', csv + '#predefined-csv-formats'],
  'format.delimiter': ['Sets the single character that separates CSV fields.', csv + '#csv-options'],
  'format.quote': ['Sets the single character used to wrap quoted CSV values.', csv + '#csv-options'],
  'format.escape': ['Sets the single character used to escape special values.', csv + '#csv-options'],
  'format.recordSeparator': ['Selects the line ending used to separate CSV records.', csv + '#csv-options'],
  'format.nullString': ['Sets the text that represents a null value in CSV.', csv + '#csv-options'],
  'format.quoteMode': ['Controls which CSV values are quoted.', csv + '#quote-modes'],
  'format.firstRecordAsHeader': ['Reads the first row as column names on a source or writes column names as the first row on a destination.', csv + '#csv-options'],
  'format.ignoreEmptyLines': ['Controls whether the CSV reader skips empty lines.', csv + '#csv-options'],
  'format.ignoreSurroundingSpaces': ['Controls whether the CSV reader ignores spaces around field values.', csv + '#csv-options'],
  'format.trim': ['Controls whether surrounding whitespace is trimmed from CSV values.', csv + '#csv-options']
};

const mongoHelp = {
  table: ['Sets the collection to read or write inside the database named in the URI.', '/connectors/mongodb/'],
  columns: ['Sets the source projection or destination field mapping for MongoDB documents.', '/connectors/mongodb/#field-mapping'],
  where: ['Filters MongoDB documents with a JSON query expression.', '/connectors/mongodb/#source-options'],
  query: ['Runs a MongoDB aggregation pipeline. Enter it as a JSON array.', '/connectors/mongodb/#source-options']
};

/** @param {string} key @param {{id: string, displayName: string, page: string}} [connector] */
export function fieldHelp(key, connector) {
  if (Object.hasOwn(generalHelp, key)) return generalHelp[key];
  const name = key.replace(/^(source|sink)\./, '');
  const connectorPage = connector?.page ?? configuration;
  if (name === 'connect') {
    return [`Sets the full connection URL for ${connector?.displayName ?? 'this endpoint'}. Start with the example and edit it for your connection.`, connectorPage];
  }
  if (name === 'extra') {
    return ['Adds connector or driver settings as name=value, one per line. ReplicaDB adds the endpoint prefix when generating the file.', connectorPage];
  }
  if (name.startsWith('connect.parameter.')) return parameterHelp[name.slice('connect.parameter.'.length)];
  if (name === 'columns' && (connector?.id === 'file' || connector?.id === 'amazon-s3')) {
    return [key.startsWith('source.')
      ? 'Sets the CSV source column names in file order, even when the input has a header.'
      : 'Overrides the output column names when you enable the CSV header.', csv + '#source-column-types'];
  }
  if (connector?.id.startsWith('mongodb') && Object.hasOwn(mongoHelp, name)) return mongoHelp[name];
  return endpointHelp[name];
}
