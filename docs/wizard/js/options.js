const completeOnly = ['complete'];
export const genericJdbc = {
  id: 'generic-jdbc',
  displayName: 'Other JDBC database',
  schemes: ['jdbc:'],
  roles: ['source', 'sink'],
  sourceModes: completeOnly,
  sinkModes: completeOnly,
  sourceQuery: true,
  singleJobOnly: true,
  bandwidthThrottling: false,
  staging: false,
  page: '/connectors/generic-jdbc/',
  caveats: ['Supply the JDBC driver class and install its JAR in the CLI lib directory.']
};

const connectionExamples = {
  mysql: 'jdbc:mysql://localhost:3306/orders',
  mariadb: 'jdbc:mariadb://localhost:3306/orders',
  postgresql: 'jdbc:postgresql://localhost:5432/orders',
  hsqldb: 'jdbc:hsqldb:file:/data/orders',
  oracle: 'jdbc:oracle:thin:@//localhost:1521/ORCLPDB1',
  'sql-server': 'jdbc:sqlserver://localhost:1433;databaseName=orders',
  cubrid: 'jdbc:cubrid:localhost:33000:orders:::',
  'jtds-sql-server': 'jdbc:jtds:sqlserver://localhost:1433/orders',
  db2: 'jdbc:db2://localhost:50000/orders',
  'db2-as400': 'jdbc:as400://localhost/orders',
  netezza: 'jdbc:netezza://localhost:5480/orders',
  denodo: 'jdbc:vdb://localhost:9999/orders',
  kafka: 'kafka://localhost:9092',
  'amazon-s3': 's3://s3.eu-west-1.amazonaws.com/my-bucket',
  mongodb: 'mongodb://localhost:27017/orders',
  'mongodb-srv': 'mongodb+srv://cluster.example.net/orders',
  'generic-jdbc': 'jdbc:vendor://localhost:1234/orders'
};

export const exampleTables = { source: 'orders', sink: 'orders_copy' };

function connectionExample(side, connector, values) {
  const table = exampleTables[side];
  if (connector.id === 'file') {
    const format = values[`${side}.file.format`] === 'orc' ? 'orc' : 'csv';
    return `file:///data/${table}.${format}`;
  }
  if (connector.id === 'sqlite') return `jdbc:sqlite:/data/${table}.db`;
  return connectionExamples[connector.id];
}

export const modes = ['complete', 'complete-atomic', 'incremental'];
const booleans = ['true', 'false'];
const fileFormats = ['csv', 'orc'];
const genericIds = ['hsqldb', 'cubrid', 'jtds-sql-server', 'netezza', 'generic-jdbc'];
const csvPresets = ['DEFAULT', 'EXCEL', 'INFORMIX_UNLOAD', 'INFORMIX_UNLOAD_CSV', 'MONGODB_CSV', 'MONGODB_TSV', 'MYSQL', 'ORACLE', 'POSTGRESQL_CSV', 'POSTGRESQL_TEXT', 'RFC4180', 'TDF'];
const authModes = ['Interactive', 'Default', 'ManagedIdentity', 'ServicePrincipal', 'ServicePrincipalCertificate', 'Integrated'].map(
  name => `ActiveDirectory${name}`
);

/**
 * @typedef {Object} FieldOptions
 * @property {string[]} [choices]
 * @property {Record<string, string>} [labels]
 * @property {string} [initial]
 * @property {string} [blank]
 * @property {string} [placeholder]
 * @property {string} [hint]
 * @property {number} [min]
 * @property {number} [length]
 * @property {boolean} [required]
 * @property {boolean} [locked]
 * @property {boolean} [multiline]
 * @property {boolean} [preserve]
 */

/** @param {string} key @param {string} label @param {FieldOptions} [options] */
function field(key, label, options = {}) {
  return { key, label, ...options };
}

/** @param {string} side @param {string} key @param {string} label @param {FieldOptions} [options] */
function parameter(side, key, label, options) {
  return field(`${side}.connect.parameter.${key}`, label, options);
}

export function isTableConnector(connector) {
  return !['file', 'amazon-s3', 'kafka'].includes(connector.id);
}

function isMongo(connector) {
  return connector.id === 'mongodb' || connector.id === 'mongodb-srv';
}

export function compatibility(source, sink, values) {
  const allowedModes = modes.filter(mode => source.sourceModes.includes(mode) && sink.sinkModes.includes(mode));
  const mode = allowedModes.includes(values.mode) ? values.mode : allowedModes[0];
  const interactive = [source, sink].some((connector, index) => {
    const side = index === 0 ? 'source' : 'sink';
    return connector.id === 'sql-server' && values[`${side}.auth.mode`] === 'ActiveDirectoryInteractive';
  });
  return {
    allowedModes,
    mode,
    interactive,
    singleJob: interactive || source.singleJobOnly || sink.singleJobOnly,
    catalog: isTableConnector(source) && isTableConnector(sink),
    throttle: sink.bandwidthThrottling,
    sourceQuery: source.sourceQuery
  };
}

function fileFields(side, values) {
  const format = field(`${side}.file.format`, 'File format', { choices: fileFormats, initial: 'csv' });
  if (values[format.key] === 'orc') {
    return side === 'source' ? [format] : [format, parameter(side, 'orc.compression', 'ORC compression', {
      choices: ['NONE', 'ZLIB', 'SNAPPY', 'LZO', 'LZ4', 'ZSTD', 'BROTLI']
    })];
  }

  return [
    format,
    ...(side === 'source' ? [
      parameter(side, 'columns.types', 'CSV column types', {
        required: true, initial: 'INTEGER,VARCHAR', hint: 'Enter one JDBC type per input column in file order.'
      }),
      field('source.columns', 'CSV column names', { initial: 'id,status' })
    ] : [field('sink.columns', 'CSV header names', { placeholder: 'id,status' })]),
    parameter(side, 'format', 'CSV preset', { choices: csvPresets }),
    parameter(side, 'format.delimiter', 'Delimiter', { length: 1 }),
    parameter(side, 'format.quote', 'Quote character', { length: 1 }),
    parameter(side, 'format.escape', 'Escape character', { length: 1 }),
    parameter(side, 'format.recordSeparator', 'Record separator', {
      choices: ['\n', '\r\n', '\r'], labels: { '\n': 'LF', '\r\n': 'CRLF', '\r': 'CR' }, preserve: true
    }),
    parameter(side, 'format.nullString', 'Null representation', { preserve: true }),
    parameter(side, 'format.quoteMode', 'Quote mode', { choices: ['ALL', 'ALL_NON_NULL', 'MINIMAL', 'NON_NUMERIC', 'NONE'] }),
    parameter(side, 'format.firstRecordAsHeader', 'First row is a header', { choices: booleans }),
    parameter(side, 'format.ignoreEmptyLines', 'Ignore empty lines', { choices: booleans }),
    parameter(side, 'format.ignoreSurroundingSpaces', 'Ignore surrounding spaces', { choices: booleans }),
    parameter(side, 'format.trim', 'Trim values', { choices: booleans })
  ];
}

function credentials(side, connector, values) {
  if (['file', 'sqlite', 'kafka', 'amazon-s3'].includes(connector.id)) return [];
  const prefix = side.toUpperCase();
  if (isMongo(connector)) return [];
  if (genericIds.includes(connector.id)) {
    return [
      parameter(side, 'driver', 'JDBC driver class', connector.id === 'generic-jdbc'
        ? { required: true, initial: 'com.example.jdbc.Driver' }
        : {}),
      parameter(side, 'user', 'Username'),
      parameter(side, 'password', 'Password', { placeholder: '${' + prefix + '_PASSWORD}' })
    ];
  }

  const auth = values[`${side}.auth.mode`];
  const servicePrincipal = auth === 'ActiveDirectoryServicePrincipal';
  const certificate = auth === 'ActiveDirectoryServicePrincipalCertificate';
  const authField = (name, label, options) => field(`${side}.auth.${name}`, label, options);
  const fields = [];
  if (connector.id === 'sql-server') fields.push(authField('mode', 'Microsoft Entra authentication', {
    choices: authModes, blank: 'Username and password'
  }));
  if (auth !== 'ActiveDirectoryIntegrated') fields.push(field(`${side}.user`, 'Username'));
  if (!auth || servicePrincipal || certificate) fields.push(field(`${side}.password`, 'Password', {
    required: servicePrincipal,
    initial: servicePrincipal ? '${' + prefix + '_PASSWORD}' : undefined,
    placeholder: '${' + prefix + '_PASSWORD}'
  }));
  if (servicePrincipal || certificate || ['ActiveDirectoryDefault', 'ActiveDirectoryManagedIdentity'].includes(auth)) {
    fields.push(authField('principal.id', 'Principal or client ID', {
      required: servicePrincipal || certificate,
      initial: servicePrincipal || certificate ? '${' + prefix + '_CLIENT_ID}' : undefined
    }));
  }
  if (auth === 'ActiveDirectoryInteractive') {
    fields.push(authField('login.hint', 'Login hint'));
  }
  if (certificate) {
    fields.push(authField('client.certificate', 'Client certificate', {
      required: true, initial: '${' + prefix + '_CLIENT_CERTIFICATE}'
    }),
      authField('client.key', 'Client private key'));
  }
  return fields;
}

function connectorFields(side, connector, values) {
  switch (connector.id) {
    case 'file':
      return fileFields(side, values);
    case 'kafka':
      return [
        parameter(side, 'topic', 'Topic', { required: true, initial: 'orders' }),
        parameter(side, 'partition', 'Partition', { min: 0 }),
        parameter(side, 'key', 'Producer key'),
        parameter(side, 'acks', 'Acknowledgements', { choices: ['all', '0', '1'] })
      ];
    case 'amazon-s3': {
      const rowKey = 'sink.connect.parameter.row.isObject';
      const fields = [
        parameter(side, 'accessKey', 'Access key ID', { required: true, initial: '${AWS_ACCESS_KEY_ID}' }),
        parameter(side, 'secretKey', 'Secret access key', { required: true, initial: '${AWS_SECRET_ACCESS_KEY}' }),
        parameter(side, 'secure-connection', 'Use HTTPS', { choices: booleans }),
        parameter(side, 'row.isObject', 'One object per row', { choices: booleans, initial: 'false' })
      ];
      if (values[rowKey] === 'true') {
        fields.push(parameter(side, 'row.keyColumn', 'Object key column', { required: true, initial: 'id' }),
          parameter(side, 'row.contentColumn', 'Object content column', { required: true, initial: 'payload' }));
      } else {
        fields.push(parameter(side, 'keyFileName', 'Object key'), ...fileFields(side, values));
      }
      return fields;
    }
    default:
      return credentials(side, connector, values);
  }
}

export function endpointFields(side, connector, source, sink, values) {
  const rules = compatibility(source, sink, values);
  const catalog = rules.catalog && values.scope === 'catalog';
  const query = rules.sourceQuery && values.scope === 'query';
  const fields = [field(`${side}.connect`, 'Connection URL', {
    required: true, initial: connectionExample(side, connector, values),
    hint: isMongo(connector) ? 'The URI must include the database. Use an environment variable for a URI containing credentials.' : ''
  }), ...connectorFields(side, connector, values)];

  if (isTableConnector(connector)) {
    if (!catalog && !(side === 'source' && query && !isMongo(connector))) {
      fields.push(field(`${side}.table`, isMongo(connector) ? 'Collection' : 'Table', {
        required: true, initial: exampleTables[side]
      }));
    }
    if (side === 'source' && query) {
      fields.push(field('source.query', isMongo(connector) ? 'Aggregation pipeline (JSON array)' : 'SQL query', {
        required: true, multiline: true,
        initial: isMongo(connector) ? '[{"$match":{}}]' : `SELECT * FROM ${exampleTables.source}`
      }));
    } else {
      fields.push(field(`${side}.columns`, side === 'source' && isMongo(connector) ? 'Projection (JSON)' : 'Columns'));
      if (side === 'source') fields.push(field('source.where', isMongo(connector) ? 'Filter (JSON)' : 'Filter predicate'));
    }
  }
  return fields;
}

export function advancedFields(source, sink, values) {
  const rules = compatibility(source, sink, values);
  const catalog = rules.catalog && values.scope === 'catalog';
  const fields = [
    field('jobs', 'Parallel jobs', { min: 1, initial: rules.singleJob ? '1' : '4', locked: rules.singleJob }),
    field('fetch.size', 'Fetch size', { min: 1, initial: '100' }),
    field('verbose', 'Log level', { choices: ['DEBUG', 'INFO', 'WARN', 'ERROR'], initial: 'INFO' }),
    field('quoted.identifiers', 'Quote SQL identifiers', { choices: booleans }),
    field('sentry.dsn', 'Sentry DSN', { placeholder: '${SENTRY_DSN}' })
  ];
  if (rules.throttle) fields.push(field('bandwidth.throttling', 'Bandwidth cap per worker (KB/s)', { min: 0, placeholder: '0' }));
  if (rules.mode === 'incremental' && values.scope !== 'query' && !catalog &&
      source.schemes.some(scheme => scheme.startsWith('jdbc:'))) {
    fields.push(field('incremental.watermark.column', 'Watermark column'),
      field('incremental.watermark.value', 'Previous watermark'));
  }
  if (isTableConnector(sink)) {
    fields.push(field('sink.disable.escape', 'Disable sink escaping', { choices: booleans }));
    if (!isMongo(sink)) {
      fields.push(field('sink.disable.index', 'Disable sink index handling', { choices: booleans }),
        field('sink.auto.create', 'Create the sink table if missing', { choices: booleans }),
        field('sink.analyze', 'Analyze the sink table after load', { choices: booleans }));
    }
    if (rules.mode === 'complete') fields.push(field('sink.disable.truncate', 'Skip clearing the sink', { choices: booleans }));
    if (rules.mode !== 'complete' && sink.staging) {
      if (!isMongo(sink)) fields.push(field('sink.staging.schema', 'Schema for generated staging'));
      if (!catalog) {
        fields.push(field('sink.staging.table', 'Existing staging table or collection'));
        fields.push(field('sink.staging.table.alias', 'Generated staging name prefix'));
      }
    }
  }
  return fields;
}

function escapeProperty(value) {
  return value.replace(/\\/g, '\\\\').replace(/\r/g, '\\r').replace(/\n/g, '\\n')
    .replace(/\t/g, '\\t').replace(/\f/g, '\\f').replace(/^ +/, spaces => '\\ '.repeat(spaces.length));
}

function collectField(properties, errors, definition, values) {
  const { key, label, required, min, length, choices, initial, locked } = definition;
  const raw = String((locked ? initial : values[key] ?? initial) ?? '');
  const value = definition.preserve || length === 1 ? raw : raw.trim();

  if (!value) {
    if (required) errors.push(`Enter ${label.toLowerCase()} (${key}).`);
    return;
  }
  if (min !== undefined && (!/^\d+$/.test(value) || Number(value) < min || Number(value) > 2147483647)) {
    errors.push(`${label} must be an integer between ${min} and 2147483647.`);
  }
  if (choices && !choices.includes(value)) errors.push(`Choose a valid value for ${label.toLowerCase()}.`);
  if (length && value.length !== length) errors.push(`${label} must contain exactly ${length} character.`);
  properties.set(key, value);
}

function collectExtraParameters(properties, errors, side, input, reservedParameters) {
  for (const line of (input ?? '').split(/\r?\n/)) {
    if (!line.trim() || /^\s*[#!]/.test(line)) continue;
    const entry = /^\s*([\w.-]+)\s*=(.*)$/.exec(line);
    if (!entry) {
      errors.push(`${side}: enter additional connection parameters as name=value, one per line.`);
      continue;
    }
    const key = `${side}.connect.parameter.${entry[1]}`;
    if (reservedParameters.has(key) || properties.has(key)) {
      errors.push(`Use the dedicated field or a single entry for ${key}.`);
    } else {
      properties.set(key, entry[2].trim());
    }
  }
}

export function generateConfiguration(source, sink, values, pairs = []) {
  const rules = compatibility(source, sink, values);
  const errors = [];
  const properties = new Map();
  if (!source.roles.includes('source') || !sink.roles.includes('sink') || !rules.allowedModes.includes(values.mode)) {
    return { errors: ['Choose compatible source and sink connectors and a supported mode.'], text: '' };
  }
  if (values.scope === 'catalog' && !rules.catalog) errors.push('A table catalog requires table or collection connectors on both sides.');
  if (values.scope === 'query' && !rules.sourceQuery) errors.push('This source does not support a custom query.');

  properties.set('mode', rules.mode);
  const advanced = advancedFields(source, sink, values);
  for (const definition of advanced) collectField(properties, errors, definition, values);
  for (const [side, connector] of [['source', source], ['sink', sink]]) {
    const definitions = endpointFields(side, connector, source, sink, values);
    for (const definition of definitions) collectField(properties, errors, definition, values);
    const url = properties.get(`${side}.connect`);
    if (url && !/^\$\{[A-Za-z_][A-Za-z0-9_]*\}$/.test(url) &&
        !connector.schemes.some(scheme => url.toLowerCase().startsWith(scheme.toLowerCase()))) {
      errors.push(`Enter a ${connector.displayName} URL for the ${side}.`);
    }
    if (connector.id === 'file' && url?.startsWith('file:') && !/^file:\/\/(?:\/|\$\{[A-Za-z_][A-Za-z0-9_]*\})/.test(url)) {
      errors.push(`Use an absolute file URI (file:///) for the ${side}.`);
    }
    const reservedParameters = new Set(definitions.map(definition => definition.key));
    for (const format of fileFormats) {
      for (const definition of fileFields(side, { [`${side}.file.format`]: format })) {
        if (connector.id === 'file' || connector.id === 'amazon-s3') reservedParameters.add(definition.key);
      }
    }
    if (connector.id === 'amazon-s3') {
      for (const definition of connectorFields(side, connector, { 'sink.connect.parameter.row.isObject': 'true' })) reservedParameters.add(definition.key);
      reservedParameters.add('sink.connect.parameter.keyFileName');
    }
    collectExtraParameters(properties, errors, side, values[`${side}.extra`], reservedParameters);
  }

  if (values.scope === 'catalog') {
    if (!pairs.length) errors.push('Add at least one table pair.');
    pairs.forEach((pair, index) => {
      for (const side of ['source', 'sink']) {
        collectField(properties, errors, field(`replication.table.${index + 1}.${side}`, `${side} table for pair ${index + 1}`, { required: true }),
          { [`replication.table.${index + 1}.${side}`]: pair[side] });
      }
    });
  }
  if (properties.has('incremental.watermark.value') && !properties.has('incremental.watermark.column')) {
    errors.push('Set the watermark column before entering its previous value.');
  }
  if (properties.has('sink.staging.schema') && properties.has('sink.staging.table')) {
    errors.push('Choose a staging schema or an existing staging table.');
  }
  if (properties.has('sink.staging.table.alias') && properties.has('sink.staging.table')) {
    errors.push('A generated staging name prefix applies only when ReplicaDB creates the staging table.');
  }

  const text = errors.length ? '' : [...properties].map(([key, value]) => `${key}=${escapeProperty(value)}`).join('\n') + '\n';
  return { errors, text };
}
