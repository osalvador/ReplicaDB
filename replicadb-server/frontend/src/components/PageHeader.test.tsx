import { ThemeProvider, Button } from '@mui/material';
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { theme } from '../theme/theme';
import PageHeader from './PageHeader';

function renderHeader(ui: React.ReactNode) {
  return render(<ThemeProvider theme={theme}>{ui}</ThemeProvider>);
}

describe('PageHeader', () => {
  it('renders the heading, supporting text, back link, and responsive action slot', () => {
    renderHeader(
      <PageHeader
        title="Dashboard"
        description="Jobs available to your account"
        backLink={<a href="/">Back to home</a>}
        actions={<Button>New job</Button>}
      />
    );

    expect(screen.getByRole('heading', { level: 1, name: 'Dashboard' })).toBeInTheDocument();
    expect(screen.getByText('Jobs available to your account')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Back to home' })).toHaveAttribute('href', '/');
    expect(screen.getByRole('button', { name: 'New job' })).toBeInTheDocument();
  });

  it('uses the requested semantic heading level and omits absent description content', () => {
    renderHeader(<PageHeader title="Details" headingLevel={2} />);

    expect(screen.getByRole('heading', { level: 2, name: 'Details' })).toBeInTheDocument();
    expect(screen.queryByText('Jobs available to your account')).not.toBeInTheDocument();
  });
});
