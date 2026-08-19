import { Box, Stack, Typography, type BoxProps } from '@mui/material';
import type { SxProps, Theme } from '@mui/material/styles';
import { useId, type ReactNode } from 'react';
import type { HeadingLevel } from './PageHeader';

export interface SurfaceSectionProps extends Omit<BoxProps, 'title'> {
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  headingLevel?: HeadingLevel;
}

export default function SurfaceSection({
  title,
  description,
  actions,
  headingLevel = 2,
  children,
  ...boxProps
}: SurfaceSectionProps) {
  const titleId = useId();
  const sectionSx: SxProps<Theme> = theme => ({
    border: '1px solid rgba(80, 98, 93, 0.2)',
    borderRadius: `${theme.tokens.section.radius}px`,
    backgroundColor: theme.tokens.surface.paper,
    padding: { xs: 2, md: 3 }
  });
  const combinedSx: SxProps<Theme> = boxProps.sx
    ? [sectionSx, ...(Array.isArray(boxProps.sx) ? boxProps.sx : [boxProps.sx])]
    : sectionSx;

  return (
    <Box
      component="section"
      aria-labelledby={boxProps['aria-label'] === undefined ? titleId : undefined}
      {...boxProps}
      sx={combinedSx}
    >
      <Stack
        direction={{ xs: 'column', sm: 'row' }}
        spacing={{ xs: 1, sm: 2 }}
        justifyContent="space-between"
        alignItems={{ xs: 'stretch', sm: 'flex-start' }}
      >
        <Box sx={{ minWidth: 0, flex: 1 }}>
          <Typography id={titleId} component={`h${headingLevel}`} variant="h5">
            {title}
          </Typography>
          {description !== undefined && (
            <Typography color="text.secondary" variant="body2" sx={{ mt: 0.5 }}>
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
      {children !== undefined && <Box sx={{ mt: 2 }}>{children}</Box>}
    </Box>
  );
}
