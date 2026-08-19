import { Box, Stack, Typography } from '@mui/material';
import type { ReactNode } from 'react';

export type HeadingLevel = 1 | 2 | 3 | 4 | 5 | 6;

export interface PageHeaderProps {
  title: ReactNode;
  description?: ReactNode;
  backLink?: ReactNode;
  actions?: ReactNode;
  headingLevel?: HeadingLevel;
}

export default function PageHeader({
  title,
  description,
  backLink,
  actions,
  headingLevel = 1
}: PageHeaderProps) {
  return (
    <Box component="header" sx={{ mb: 3 }}>
      <Stack
        direction={{ xs: 'column', sm: 'row' }}
        spacing={{ xs: 2, sm: 3 }}
        justifyContent="space-between"
        alignItems={{ xs: 'stretch', sm: 'flex-start' }}
      >
        <Box sx={{ minWidth: 0, flex: 1 }}>
          {backLink !== undefined && <Box sx={{ mb: 1 }}>{backLink}</Box>}
          <Typography component={`h${headingLevel}`} variant="h3">
            {title}
          </Typography>
          {description !== undefined && (
            <Typography color="text.secondary" sx={{ mt: 1 }}>
              {description}
            </Typography>
          )}
        </Box>
        {actions !== undefined && (
          <Stack
            direction={{ xs: 'column', sm: 'row' }}
            spacing={1}
            alignItems={{ xs: 'stretch', sm: 'center' }}
            sx={{ flexShrink: 0, '& > *': { width: { xs: '100%', sm: 'auto' } } }}
          >
            {actions}
          </Stack>
        )}
      </Stack>
    </Box>
  );
}
