export type DbType =
  | 'oracle'
  | 'mysql'
  | 'mariadb'
  | 'postgres'
  | 'db2'
  | 'db2i'
  | 'sqlite'
  | 'sqlserver'
  | 'denodo'
  | 'file'
  | 'kafka'
  | 'custom';

export type OracleFormat = 'service' | 'sid';

export type ConnectionFields = {
  host?: string;
  port?: string | number;
  database?: string;
  sqliteFilePath?: string;
  filePath?: string;
  kafkaBootstrapServers?: string;
  oracleFormat?: OracleFormat;
  raw?: string;
};

export type ParsedConnection = ConnectionFields & {
  type: DbType;
};

function required(fields: ConnectionFields, name: keyof ConnectionFields): string {
  const value = fields[name];
  if (value === undefined || value === null || String(value).trim() === '') {
    throw new Error(`${name} is required`);
  }
  return String(value);
}

export function buildConnectString(type: DbType, fields: ConnectionFields): string {
  const host = () => required(fields, 'host');
  const port = () => required(fields, 'port');
  const database = () => required(fields, 'database');

  switch (type) {
    case 'oracle':
      return fields.oracleFormat === 'sid'
        ? `jdbc:oracle:thin:@${host()}:${port()}:${database()}`
        : `jdbc:oracle:thin:@//${host()}:${port()}/${database()}`;
    case 'mysql':
      return `jdbc:mysql://${host()}:${port()}/${database()}`;
    case 'mariadb':
      return `jdbc:mariadb://${host()}:${port()}/${database()}`;
    case 'postgres':
      return `jdbc:postgresql://${host()}${fields.port === undefined ? '' : `:${port()}`}/${database()}`;
    case 'db2':
      return `jdbc:db2://${host()}:${port()}/${database()}`;
    case 'db2i':
      return `jdbc:as400://${host()}:${port()}/${database()}`;
    case 'sqlite':
      return `jdbc:sqlite:/${required(fields, 'sqliteFilePath')}`;
    case 'sqlserver':
      return `jdbc:sqlserver://${host()}:${port()};database=${database()}`;
    case 'denodo':
      return `jdbc:vdb://${host()}:${port()}/${database()}`;
    case 'file':
      return `file://${required(fields, 'filePath')}`;
    case 'kafka':
      return `kafka://${required(fields, 'kafkaBootstrapServers')}`;
    case 'custom':
      return required(fields, 'raw');
  }
}

function parseAuthority(authority: string): Pick<ParsedConnection, 'host' | 'port'> {
  if (authority.startsWith('[')) {
    const closingBracket = authority.indexOf(']');
    if (closingBracket > 0) {
      const host = authority.slice(0, closingBracket + 1);
      const port = authority.slice(closingBracket + 1).replace(/^:/, '') || undefined;
      return { host, port };
    }
  }

  const separator = authority.lastIndexOf(':');
  if (separator > 0 && /^\d+$/.test(authority.slice(separator + 1))) {
    return { host: authority.slice(0, separator), port: authority.slice(separator + 1) };
  }
  return { host: authority, port: undefined };
}

function parseDatabaseConnection(type: DbType, authority: string, database: string): ParsedConnection {
  return { type, database, ...parseAuthority(authority) };
}

export function parseConnectString(connect: string): ParsedConnection {
  const custom = (): ParsedConnection => ({ type: 'custom', raw: connect });

  if (!connect) {
    return custom();
  }

  const oracleService = connect.match(/^jdbc:oracle:thin:@\/\/(.+)\/(.+)$/);
  if (oracleService) {
    return { type: 'oracle', oracleFormat: 'service', database: oracleService[2], ...parseAuthority(oracleService[1]) };
  }

  const oracleSid = connect.match(/^jdbc:oracle:thin:@(.+):([^:]+)$/);
  if (oracleSid) {
    const sidParts = oracleSid[1].match(/^(.+?)(?::(\d+))?$/);
    if (sidParts) {
      return {
        type: 'oracle',
        oracleFormat: 'sid',
        database: oracleSid[2],
        host: sidParts[1],
        port: sidParts[2]
      };
    }
  }

  const sqlServer = connect.match(/^jdbc:sqlserver:\/\/(.+?);database=(.+)$/);
  if (sqlServer) {
    return { type: 'sqlserver', database: sqlServer[2], ...parseAuthority(sqlServer[1]) };
  }

  const jdbc = connect.match(/^jdbc:(mysql|mariadb|postgresql|db2|as400|vdb):\/\/(.+)\/(.+)$/);
  if (jdbc) {
    const type = jdbc[1] === 'postgresql' ? 'postgres'
      : jdbc[1] === 'as400' ? 'db2i'
        : jdbc[1] === 'vdb' ? 'denodo' : jdbc[1];
    return parseDatabaseConnection(type as DbType, jdbc[2], jdbc[3]);
  }

  const sqlite = connect.match(/^jdbc:sqlite:\/(.*)$/);
  if (sqlite) {
    return { type: 'sqlite', sqliteFilePath: sqlite[1] };
  }

  const file = connect.match(/^file:\/\/(.*)$/);
  if (file) {
    return { type: 'file', filePath: file[1] };
  }

  const kafka = connect.match(/^kafka:\/\/(.*)$/);
  if (kafka) {
    return { type: 'kafka', kafkaBootstrapServers: kafka[1] };
  }

  return custom();
}
