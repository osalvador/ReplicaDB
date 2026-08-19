import { AppBar, Box, Button, Stack, Toolbar, Typography } from '@mui/material';
import { Link as RouterLink, Outlet } from 'react-router-dom';
import { useAuth } from '../auth/useAuth';

export default function AppLayout() {
  const { user, logout } = useAuth();

  return (
    <>
      <AppBar position="static" color="inherit" elevation={0}>
        <Toolbar
          sx={theme => ({
            gap: { xs: 1, sm: 2 },
            px: { xs: 2, md: 4 },
            py: 1,
            flexWrap: 'wrap',
            borderBottom: 0,
            '& .MuiTypography-root': {
              minWidth: 0
            },
            '& .ReplicaDB-brand': {
              borderRadius: `${theme.tokens.section.radius}px`,
              padding: '4px 8px',
              textDecoration: 'none',
              transition: 'background-color 150ms ease, color 150ms ease',
              '&:hover': {
                backgroundColor: 'rgba(11, 110, 105, 0.08)'
              },
              '&:focus-visible': {
                outline: `3px solid ${theme.tokens.focus.ring}`,
                outlineOffset: '2px'
              }
            }
          })}
        >
          <Typography
            component={RouterLink}
            to="/"
            variant="h6"
            color="primary"
            fontWeight={700}
            className="ReplicaDB-brand"
          >
            ReplicaDB
          </Typography>
          <Stack
            role="group"
            aria-label="Signed-in identity"
            direction="row"
            spacing={1.5}
            alignItems="center"
            justifyContent={{ xs: 'flex-end', sm: 'initial' }}
            sx={{
              ml: { xs: 0, sm: 'auto' },
              width: { xs: '100%', sm: 'auto' },
              minWidth: 0,
              flexWrap: 'wrap'
            }}
          >
            <Typography component="span" variant="body2" fontWeight={700} noWrap>
              {user?.username}
            </Typography>
            <Typography component="span" variant="body2" color="text.secondary" noWrap>
              {user?.role}
            </Typography>
            <Button color="primary" variant="outlined" size="small" onClick={logout}>
              Logout
            </Button>
          </Stack>
        </Toolbar>
      </AppBar>
      <Box
        component="main"
        sx={{
          width: '100%',
          maxWidth: 1600,
          mx: 'auto',
          px: { xs: 2, sm: 3, md: 4 },
          py: { xs: 2, md: 4 }
        }}
      >
        <Outlet />
      </Box>
    </>
  );
}
