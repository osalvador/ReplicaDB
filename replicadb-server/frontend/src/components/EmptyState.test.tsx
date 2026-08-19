import { Button } from '@mui/material';
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import EmptyState from './EmptyState';

describe('EmptyState', () => {
  it('renders a named empty status with optional description and action', () => {
    render(
      <EmptyState
        title="No jobs available."
        description="Create a job to begin replicating data."
        action={<Button>Create job</Button>}
      />
    );

    expect(screen.getByRole('status', { name: 'No jobs available.' })).toBeInTheDocument();
    expect(screen.getByText('Create a job to begin replicating data.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Create job' })).toBeInTheDocument();
  });
});
