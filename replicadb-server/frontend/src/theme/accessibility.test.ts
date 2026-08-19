import { ThemeProvider } from '@mui/material';
import { render, screen } from '@testing-library/react';
import { createElement } from 'react';
import { describe, expect, it } from 'vitest';
import PageHeader from '../components/PageHeader';
import StatusChip from '../components/StatusChip';
import { theme, visualTokens } from './theme';

function luminance(hex: string): number {
  const channels = hex.slice(1).match(/../g)?.map(value => parseInt(value, 16) / 255);
  if (!channels || channels.length !== 3) {
    throw new Error(`Invalid color: ${hex}`);
  }
  const linear = channels.map(channel => channel <= 0.03928
    ? channel / 12.92
    : ((channel + 0.055) / 1.055) ** 2.4);
  return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
}

function contrastRatio(first: string, second: string): number {
  const firstLuminance = luminance(first);
  const secondLuminance = luminance(second);
  const lighter = Math.max(firstLuminance, secondLuminance);
  const darker = Math.min(firstLuminance, secondLuminance);
  return (lighter + 0.05) / (darker + 0.05);
}

describe('accessibility visual contract', () => {
  it('keeps normal text and action colors above the WCAG AA threshold', () => {
    expect(contrastRatio(visualTokens.text.primary, visualTokens.surface.paper)).toBeGreaterThanOrEqual(4.5);
    expect(contrastRatio(visualTokens.text.secondary, visualTokens.surface.page)).toBeGreaterThanOrEqual(4.5);
    expect(contrastRatio(visualTokens.brand.primary, visualTokens.surface.paper)).toBeGreaterThanOrEqual(4.5);
    expect(contrastRatio(visualTokens.brand.secondary, visualTokens.surface.paper)).toBeGreaterThanOrEqual(4.5);
    for (const color of Object.values(visualTokens.semantic)) {
      expect(contrastRatio(color, visualTokens.surface.paper)).toBeGreaterThanOrEqual(4.5);
    }
  });

  it('defines a visible focus ring and semantic status markup', () => {
    expect(theme.components?.MuiCssBaseline?.styleOverrides).toMatchObject({
      '*:focus-visible': {
        outline: '3px solid #0B6E69',
        outlineOffset: '2px'
      }
    });

    render(createElement(
      ThemeProvider,
      { theme },
      createElement(PageHeader, {
        title: 'Dashboard',
        actions: createElement('button', { type: 'button' }, 'New job')
      }),
      createElement(StatusChip, { status: 'FAILED' })
    ));

    expect(screen.getByRole('heading', { level: 1, name: 'Dashboard' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'New job' })).toBeInTheDocument();
    expect(screen.getByRole('status', { name: 'Run status: FAILED' })).toBeInTheDocument();
  });
});
