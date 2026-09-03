import { fireEvent, render, screen } from '@testing-library/react';
import { useState } from 'react';
import { describe, expect, it } from 'vitest';
import StagingOptionsTabs from './StagingOptionsTabs';

function Harness() {
  const [values, setValues] = useState({ schema: 'staging', table: '' });
  return (
    <StagingOptionsTabs
      {...values}
      onChange={(field, value) => setValues(current => ({ ...current, [field]: value }))}
    />
  );
}

describe('StagingOptionsTabs', () => {
  it('preserves schema and table values when switching modes', () => {
    render(<Harness />);
    expect(screen.getByRole('tab', { name: 'Create in schema' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('tabpanel')).toHaveAttribute('aria-labelledby', expect.stringContaining('schema-tab'));
    fireEvent.click(screen.getByRole('tab', { name: 'Use existing table' }));

    expect(screen.getByRole('tab', { name: 'Use existing table' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('tabpanel')).toHaveAttribute('aria-labelledby', expect.stringContaining('table-tab'));
    expect(screen.getByLabelText('Existing staging table')).toHaveValue('');
    fireEvent.change(screen.getByLabelText('Existing staging table'), { target: { value: 'staging.orders' } });
    fireEvent.click(screen.getByRole('tab', { name: 'Create in schema' }));

    expect(screen.getByLabelText('Staging schema')).toHaveValue('staging');
    fireEvent.click(screen.getByRole('tab', { name: 'Use existing table' }));
    expect(screen.getByLabelText('Existing staging table')).toHaveValue('staging.orders');
  });

  it('explains the two staging target choices', () => {
    render(<Harness />);

    expect(screen.getByText(/Choose one staging target/)).toBeInTheDocument();
    expect(screen.getByText('ReplicaDB creates a staging table in this schema.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab', { name: 'Use existing table' }));
    expect(screen.getByText('Use a qualified staging table that already exists.')).toBeInTheDocument();
  });
});
