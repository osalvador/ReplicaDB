import { fireEvent, render, screen } from '@testing-library/react';
import { useState } from 'react';
import { describe, expect, it } from 'vitest';
import DataFilteringTabs, { type DataFilteringValues } from './DataFilteringTabs';

function Harness({ sourceType = 'postgres', tableError }: { sourceType?: 'postgres' | 'file'; tableError?: string }) {
  const [values, setValues] = useState<DataFilteringValues>({
    table: 'orders',
    columns: 'id, payload',
    where: 'id > 10',
    query: ''
  });
  const [fileParams, setFileParams] = useState<Record<string, string>>({});

  return (
    <DataFilteringTabs
      values={values}
      onChange={(field, value) => setValues(current => ({ ...current, [field]: value }))}
      sourceType={sourceType}
      fileParams={fileParams}
      onFileParamChange={(key, value) => setFileParams(current => ({ ...current, [key]: value }))}
      tableError={tableError}
    />
  );
}

describe('DataFilteringTabs', () => {
  it('clears table options when switching to query mode and back', () => {
    render(<Harness />);
    expect(screen.getByRole('tab', { name: 'Options' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('tabpanel')).toHaveAttribute('aria-labelledby', expect.stringContaining('options-tab'));
    fireEvent.click(screen.getByRole('tab', { name: 'Query' }));

    expect(screen.getByRole('tab', { name: 'Query' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('tabpanel')).toHaveAttribute('aria-labelledby', expect.stringContaining('query-tab'));
    expect(screen.queryByLabelText('Table')).not.toBeInTheDocument();
    fireEvent.change(screen.getByRole('textbox', { name: 'Query' }), { target: { value: 'select * from orders' } });
    fireEvent.click(screen.getByRole('tab', { name: 'Options' }));

    expect(screen.getByLabelText('Table')).toHaveValue('');
    expect(screen.getByLabelText('Columns')).toHaveValue('');
    expect(screen.getByLabelText('Where')).toHaveValue('');
  });

  it('associates table validation text with the table field', () => {
    render(<Harness tableError="Source table or query is required." />);

    expect(screen.getByLabelText('Table')).toHaveAccessibleDescription('Source table or query is required.');
    expect(screen.getByText('Source table or query is required.')).toBeInTheDocument();
  });

  it('shows file parsing settings only for file sources', () => {
    const { rerender } = render(<Harness />);
    expect(screen.queryByText('Parsing and formatting file data')).not.toBeInTheDocument();

    rerender(<Harness sourceType="file" />);
    expect(screen.getByText('Parsing and formatting file data')).toBeInTheDocument();
    expect(screen.getByRole('group', { name: 'File format settings' })).toBeInTheDocument();
    fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Format' }));
    fireEvent.click(screen.getByRole('option', { name: 'RFC4180' }));
    expect(screen.getByRole('combobox', { name: 'Format' })).toHaveTextContent('RFC4180');
  });
});
