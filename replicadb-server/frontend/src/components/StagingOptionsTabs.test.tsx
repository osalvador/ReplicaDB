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
    expect(screen.getByRole('tab', { name: 'Schema' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('tabpanel')).toHaveAttribute('aria-labelledby', expect.stringContaining('schema-tab'));
    fireEvent.click(screen.getByRole('tab', { name: 'Table' }));

    expect(screen.getByRole('tab', { name: 'Table' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByRole('tabpanel')).toHaveAttribute('aria-labelledby', expect.stringContaining('table-tab'));
    expect(screen.getByLabelText('Staging table')).toHaveValue('');
    fireEvent.change(screen.getByLabelText('Staging table'), { target: { value: 'staging.orders' } });
    fireEvent.click(screen.getByRole('tab', { name: 'Schema' }));

    expect(screen.getByLabelText('Staging schema')).toHaveValue('staging');
    fireEvent.click(screen.getByRole('tab', { name: 'Table' }));
    expect(screen.getByLabelText('Staging table')).toHaveValue('staging.orders');
  });
});
