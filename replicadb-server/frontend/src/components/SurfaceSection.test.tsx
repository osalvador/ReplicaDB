import { ThemeProvider, Button } from '@mui/material';
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { theme } from '../theme/theme';
import SurfaceSection from './SurfaceSection';

function renderSection(ui: React.ReactNode) {
  return render(<ThemeProvider theme={theme}>{ui}</ThemeProvider>);
}

describe('SurfaceSection', () => {
  it('renders a framed section with title, description, action, and content', () => {
    renderSection(
      <SurfaceSection
        title="Source"
        description="Configure the source connection."
        actions={<Button>Test connection</Button>}
      >
        <div>Source fields</div>
      </SurfaceSection>
    );

    expect(screen.getByRole('region')).toBeInTheDocument();
    expect(screen.getByRole('heading', { level: 2, name: 'Source' })).toBeInTheDocument();
    expect(screen.getByText('Configure the source connection.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Test connection' })).toBeInTheDocument();
    expect(screen.getByText('Source fields')).toBeInTheDocument();
  });

  it('uses the requested semantic heading level and omits absent description content', () => {
    renderSection(<SurfaceSection title="Execution" headingLevel={3} />);

    expect(screen.getByRole('heading', { level: 3, name: 'Execution' })).toBeInTheDocument();
    expect(screen.queryByText('Configure the source connection.')).not.toBeInTheDocument();
  });
});
