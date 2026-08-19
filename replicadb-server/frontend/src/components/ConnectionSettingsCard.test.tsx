import { fireEvent, render, screen } from '@testing-library/react';
import { useState } from 'react';
import { describe, expect, it } from 'vitest';
import ConnectionSettingsCard, {
  type ConnectionDraft,
  type EndpointValues
} from './ConnectionSettingsCard';

const endpointValues: EndpointValues = {
  connect: '',
  user: '',
  password: '',
  authMode: '',
  authPrincipalId: '',
  authLoginHint: '',
  authClientCertificate: '',
  authClientKey: ''
};

function Harness({ side = 'source' }: { side?: 'source' | 'sink' }) {
  const [draft, setDraft] = useState<ConnectionDraft>({
    type: 'custom',
    fields: { raw: '' },
    extraParams: ''
  });

  return (
    <ConnectionSettingsCard
      side={side}
      draft={draft}
      values={endpointValues}
      onDraftChange={setDraft}
      onValueChange={() => undefined}
    />
  );
}

function selectType(label: string, option: string) {
  fireEvent.mouseDown(screen.getByRole('combobox', { name: label }));
  fireEvent.click(screen.getByRole('option', { name: option }));
}

describe('ConnectionSettingsCard', () => {
  it('reveals host and database fields for a database type and previews the URL', () => {
    render(<Harness />);
    selectType('Source data source type', 'PostgreSQL');

    fireEvent.change(screen.getByLabelText('Host'), { target: { value: 'localhost' } });
    fireEvent.change(screen.getByLabelText('Port'), { target: { value: '5432' } });
    fireEvent.change(screen.getByLabelText('Database / SID or Service Name'), { target: { value: 'replica' } });

    const preview = screen.getByLabelText('Source connection');
    expect(preview).toHaveValue('jdbc:postgresql://localhost:5432/replica');
    expect(preview).toHaveAttribute('readonly');
    expect(preview).toHaveAttribute('aria-readonly', 'true');
    expect(screen.queryByLabelText('File path')).not.toBeInTheDocument();
  });

  it('keeps a custom connection string editable and preserves its field order', () => {
    render(<Harness />);
    const type = screen.getByRole('combobox', { name: 'Source data source type' });
    const connection = screen.getByLabelText(/^Source connection/);

    expect(connection).not.toHaveAttribute('readonly');
    fireEvent.change(connection, { target: { value: 'jdbc:custom://example/database' } });
    expect(connection).toHaveValue('jdbc:custom://example/database');
    expect(Boolean(type.compareDocumentPosition(connection) & Node.DOCUMENT_POSITION_FOLLOWING)).toBe(true);
  });

  it('switches Oracle between service name and SID formats', () => {
    render(<Harness />);
    selectType('Source data source type', 'Oracle');
    fireEvent.change(screen.getByLabelText('Host'), { target: { value: 'oracle' } });
    fireEvent.change(screen.getByLabelText('Port'), { target: { value: '1521' } });
    fireEvent.change(screen.getByLabelText('Database / SID or Service Name'), { target: { value: 'ORCL' } });
    fireEvent.click(screen.getByLabelText('SID'));

    expect(screen.getByLabelText('Source connection')).toHaveValue('jdbc:oracle:thin:@oracle:1521:ORCL');
  });

  it('shows Entra authentication only for SQL Server', () => {
    render(<Harness />);
    expect(screen.queryByText('Microsoft Entra Authentication')).not.toBeInTheDocument();

    selectType('Source data source type', 'SQL Server');
    const disclosure = screen.getByRole('button', { name: 'Microsoft Entra Authentication' });
    expect(disclosure).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(disclosure);
    expect(disclosure).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByRole('region', { name: 'Microsoft Entra Authentication' })).toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Authentication mode' })).toBeInTheDocument();
  });

  it('reveals Kafka fields only for a sink', () => {
    render(<Harness side="sink" />);
    selectType('Sink data source type', 'Apache Kafka');

    expect(screen.getByLabelText('Bootstrap servers')).toBeInTheDocument();
    expect(screen.getByLabelText('Topic name')).toBeInTheDocument();
    expect(screen.getByLabelText('Topic partition')).toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'ACKs' })).toBeInTheDocument();
    expect(screen.getByLabelText('Extra Kafka producer properties')).toBeInTheDocument();
    expect(screen.queryByLabelText('Host')).not.toBeInTheDocument();
  });

  it('allows extra parameters to be edited as key-value lines', () => {
    render(<Harness />);
    const field = screen.getByLabelText('Extra JDBC parameters');
    fireEvent.change(field, { target: { value: 'ApplicationName=ReplicaDB\nsslmode=require' } });

    expect(field).toHaveValue('ApplicationName=ReplicaDB\nsslmode=require');
  });
});
