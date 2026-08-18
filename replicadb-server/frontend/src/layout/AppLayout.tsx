import { AppBar, Box, Button, Toolbar, Typography } from '@mui/material';
import { Outlet } from 'react-router-dom';
import { useAuth } from '../auth/useAuth';

export default function AppLayout() {
  const { user, logout } = useAuth();

  return (
    <>
      <AppBar position="static" color="inherit" elevation={0}>
        <Toolbar sx={{ borderBottom: 1, borderColor: 'divider' }}>
          <Typography component="div" variant="h6" color="primary" fontWeight={700}>
            ReplicaDB
          </Typography>
          <Box sx={{ flexGrow: 1 }} />
          <Typography component="span" sx={{ mr: 2 }}>
            {user?.username}
          </Typography>
          <Typography component="span" color="text.secondary" sx={{ mr: 2 }}>
            {user?.role}
          </Typography>
          <Button color="primary" onClick={logout}>
            Logout
          </Button>
        </Toolbar>
      </AppBar>
      <Box component="main" sx={{ p: { xs: 2, md: 4 } }}>
        <Outlet />
      </Box>
    </>
  );
}
