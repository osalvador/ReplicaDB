import { describe, expect, it } from 'vitest';
import { theme, visualTokens } from './theme';

describe('theme visual token contract', () => {
  it('exposes the ReplicaDB brand and surface tokens', () => {
    expect(theme.tokens).toEqual(visualTokens);
    expect(theme.tokens.brand.primary).toBe('#0B6E69');
    expect(theme.tokens.brand.secondary).toBe('#B15C38');
    expect(theme.tokens.surface.page).toBe('#F3F6F4');
    expect(theme.tokens.surface.paper).toBe('#FFFFFF');
    expect(theme.tokens.surface.subtle).toBe('#E8F0ED');
    expect(theme.tokens.text.primary).toBe('#1B2926');
    expect(theme.tokens.text.secondary).toBe('#50625D');
  });

  it('exposes semantic, focus, control, section, and spacing tokens', () => {
    expect(Object.values(theme.tokens.semantic)).toHaveLength(4);
    expect(new Set(Object.values(theme.tokens.semantic)).size).toBe(4);
    expect(theme.tokens.focus.ring).toBe('#0B6E69');
    expect(theme.tokens.control.height).toBe(40);
    expect(theme.tokens.section.radius).toBe(8);
    expect(theme.tokens.spacing.unit).toBe(8);
  });

  it('keeps the primary identity out of the violet palette and uses white action text', () => {
    expect(theme.tokens.brand.primary).not.toMatch(/purple|violet/i);
    expect(theme.palette.primary.contrastText).toBe('#FFFFFF');
    expect(theme.palette.secondary.contrastText).toBe('#FFFFFF');
  });

  it('defines the desktop-first breakpoint contract', () => {
    expect(theme.breakpoints.values).toMatchObject({
      xs: 0,
      sm: 600,
      md: 900
    });
  });

  it('configures the Material 3 adapted foundation', () => {
    expect(theme.spacing(1)).toBe('8px');
    expect(theme.shape.borderRadius).toBe(8);
    expect(theme.shadows[0]).toBe('none');
    expect(theme.shadows[1]).toContain('rgba(27, 41, 38');
    expect(theme.typography.button?.textTransform).toBe('none');
    expect(theme.typography.h2?.fontFamily).toContain('Georgia');
    expect(theme.palette.success.main).toBe('#216E4A');
    expect(theme.palette.info.main).toBe('#1769AA');
    expect(theme.palette.warning.main).toBe('#8A4B08');
    expect(theme.palette.error.main).toBe('#B3261E');
  });

  it('defines representative component state overrides', () => {
    expect(theme.components?.MuiButton?.styleOverrides?.root).toMatchObject({
      minHeight: 40,
      borderRadius: 8
    });
    expect(theme.components?.MuiOutlinedInput?.styleOverrides?.root).toMatchObject({
      minHeight: 40,
      borderRadius: 6
    });
    expect(theme.components?.MuiOutlinedInput?.styleOverrides?.root).toHaveProperty(
      '&.Mui-focused .MuiOutlinedInput-notchedOutline'
    );
    expect(theme.components?.MuiAlert?.styleOverrides?.root).toMatchObject({
      borderRadius: 8,
      padding: '8px 12px'
    });
    expect(theme.components?.MuiChip?.styleOverrides?.root).toMatchObject({
      minHeight: 28,
      borderRadius: 6
    });
    expect(theme.components?.MuiTable?.styleOverrides?.root).toHaveProperty(
      '& .MuiTableHead-root'
    );
    expect(theme.components?.MuiMenu?.styleOverrides?.paper).toMatchObject({
      borderRadius: 8,
      backgroundColor: visualTokens.surface.paper,
      boxShadow: theme.shadows[2]
    });
    expect(theme.components?.MuiMenuItem?.styleOverrides?.root).toHaveProperty('&&.Mui-selected');
    expect(theme.components?.MuiMenuItem?.styleOverrides?.root).toHaveProperty(
      '& .MuiListItemText-root'
    );
    expect(theme.components?.MuiAutocomplete?.styleOverrides?.paper).toMatchObject({
      borderRadius: 8,
      backgroundColor: visualTokens.surface.paper,
      boxShadow: theme.shadows[2]
    });
    expect(theme.components?.MuiAutocomplete?.styleOverrides?.listbox).toHaveProperty(
      '& .MuiAutocomplete-option'
    );
  });
});
