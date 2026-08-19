import { ThemeProvider } from '@mui/material';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it } from 'vitest';
import NotAuthorizedPage from './NotAuthorizedPage';
import { theme } from '../theme/theme';

describe('NotAuthorizedPage', () => {
  it('renders an explanation and a link back to the dashboard', () => {
    render(
      <ThemeProvider theme={theme}>
        <MemoryRouter>
          <NotAuthorizedPage />
        </MemoryRouter>
      </ThemeProvider>
    );

    expect(screen.getByRole('heading', { name: 'Not authorized' })).toBeInTheDocument();
    expect(screen.getByText('You do not have permission to view this page.')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Back to dashboard' })).toHaveAttribute('href', '/');
  });
});
