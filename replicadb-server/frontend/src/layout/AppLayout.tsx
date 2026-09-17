import DashboardOutlinedIcon from '@mui/icons-material/DashboardOutlined';
import FactCheckOutlinedIcon from '@mui/icons-material/FactCheckOutlined';
import ManageAccountsOutlinedIcon from '@mui/icons-material/ManageAccountsOutlined';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import LogoutOutlinedIcon from '@mui/icons-material/LogoutOutlined';
import MenuIcon from '@mui/icons-material/Menu';
import PeopleOutlineIcon from '@mui/icons-material/PeopleOutline';
import StorageOutlinedIcon from '@mui/icons-material/StorageOutlined';
import WorkOutlineIcon from '@mui/icons-material/WorkOutline';
import {
  AppBar,
  Box,
  Divider,
  Drawer,
  IconButton,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Stack,
  Toolbar,
  Tooltip,
  Typography
} from '@mui/material';
import { useState } from 'react';
import { Link as RouterLink, Outlet, useLocation } from 'react-router-dom';
import { useAuth } from '../auth/useAuth';

const drawerWidth = 248;
const collapsedDrawerWidth = 72;
const navigationCollapsedStorageKey = 'replicadb.navigation.collapsed';

function readNavigationCollapsed() {
  try {
    return window.localStorage.getItem(navigationCollapsedStorageKey) === 'true';
  } catch {
    return false;
  }
}

function writeNavigationCollapsed(collapsed: boolean) {
  try {
    window.localStorage.setItem(navigationCollapsedStorageKey, String(collapsed));
  } catch {}
}

type NavigationProps = {
  collapsed?: boolean;
  onNavigate?: () => void;
  onToggleCollapse?: () => void;
  onLogout?: () => void;
};

function Navigation({ collapsed = false, onNavigate, onToggleCollapse, onLogout }: NavigationProps) {
  const location = useLocation();
  const { user } = useAuth();
  const items = [
    { label: 'Dashboard', to: '/', icon: <DashboardOutlinedIcon /> },
    { label: 'Jobs', to: '/jobs', icon: <WorkOutlineIcon /> },
    { label: 'Datasources', to: '/datasources', icon: <StorageOutlinedIcon /> },
    ...(user?.role === 'ADMIN' ? [{ label: 'Audit', to: '/audit', icon: <FactCheckOutlinedIcon /> }] : []),
    ...(user?.role === 'ADMIN' ? [{ label: 'Users', to: '/users', icon: <PeopleOutlineIcon /> }] : [])
  ];

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ px: collapsed ? 1 : 2.5, py: 2.25, display: 'flex', alignItems: 'center', justifyContent: collapsed ? 'center' : 'space-between', gap: 1 }}>
        {!collapsed && (
          <Box>
            <Typography component={RouterLink} to="/" variant="h5" color="primary" fontWeight={700} sx={{ textDecoration: 'none' }}>
              ReplicaDB
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5 }}>
              Control plane
            </Typography>
          </Box>
        )}
        {onToggleCollapse && (
          <Tooltip title={collapsed ? 'Expand navigation' : 'Collapse navigation'} placement="right">
            <IconButton
              color="primary"
              size="small"
              aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'}
              aria-expanded={!collapsed}
              onClick={onToggleCollapse}
            >
              {collapsed ? <ChevronRightIcon /> : <ChevronLeftIcon />}
            </IconButton>
          </Tooltip>
        )}
      </Box>
      <Divider />
      <List component="nav" aria-label="Primary navigation" sx={{ px: collapsed ? 1 : 1.5, py: 2 }}>
        {items.map(item => {
          const selected = item.to === '/' ? location.pathname === '/' : location.pathname.startsWith(item.to);
          const navigationItem = (
            <ListItemButton
              key={item.to}
              component={RouterLink}
              to={item.to}
              selected={selected}
              aria-label={item.label}
              aria-current={selected ? 'page' : undefined}
              onClick={onNavigate}
              sx={{ borderRadius: 1, mb: 0.5, minHeight: 44, justifyContent: collapsed ? 'center' : 'initial', px: collapsed ? 1.5 : 1.5 }}
            >
              <ListItemIcon sx={{ minWidth: collapsed ? 0 : 36, justifyContent: 'center', color: selected ? 'primary.main' : 'text.secondary' }}>
                {item.icon}
              </ListItemIcon>
              {!collapsed && <ListItemText primary={item.label} primaryTypographyProps={{ fontWeight: selected ? 700 : 500 }} />}
            </ListItemButton>
          );

          return collapsed ? <Tooltip key={item.to} title={item.label} placement="right">{navigationItem}</Tooltip> : navigationItem;
        })}
      </List>
      <Box sx={{ mt: 'auto', px: collapsed ? 1 : 1.5, pb: 2 }}>
        <Divider sx={{ mb: 1.5 }} />
        {collapsed ? (
          <Stack spacing={0.5} alignItems="center">
            <Tooltip title="My profile" placement="right">
              <IconButton
                component={RouterLink}
                to="/profile"
                color={location.pathname === '/profile' ? 'primary' : 'default'}
                aria-label="My profile"
                onClick={onNavigate}
              >
                <ManageAccountsOutlinedIcon />
              </IconButton>
            </Tooltip>
            <Tooltip title="Logout" placement="right">
              <IconButton color="default" aria-label="Logout" onClick={() => void onLogout?.()}>
                <LogoutOutlinedIcon />
              </IconButton>
            </Tooltip>
          </Stack>
        ) : (
          <Stack spacing={1}>
            <Stack direction="row" spacing={1.25} alignItems="center" sx={{ px: 1 }}>
              <ManageAccountsOutlinedIcon color="primary" fontSize="small" />
              <Box sx={{ minWidth: 0 }}>
                <Typography variant="body2" fontWeight={700} noWrap>
                  {user?.username}
                </Typography>
                <Typography variant="caption" color="text.secondary" noWrap>
                  {user?.role}
                </Typography>
              </Box>
            </Stack>
            <List component="nav" aria-label="Account navigation" disablePadding>
              <ListItemButton
                component={RouterLink}
                to="/profile"
                selected={location.pathname === '/profile'}
                aria-label="My profile"
                onClick={onNavigate}
                sx={{ borderRadius: 1, minHeight: 40, px: 1.5 }}
              >
                <ListItemIcon sx={{ minWidth: 36, justifyContent: 'center', color: 'text.secondary' }}>
                  <ManageAccountsOutlinedIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="My profile" primaryTypographyProps={{ fontWeight: 600 }} />
              </ListItemButton>
              <ListItemButton
                component="button"
                aria-label="Logout"
                onClick={() => void onLogout?.()}
                sx={{ borderRadius: 1, minHeight: 40, width: '100%', px: 1.5 }}
              >
                <ListItemIcon sx={{ minWidth: 36, justifyContent: 'center', color: 'text.secondary' }}>
                  <LogoutOutlinedIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Logout" primaryTypographyProps={{ fontWeight: 600 }} />
              </ListItemButton>
            </List>
            <Typography variant="caption" color="text.secondary" sx={{ px: 1 }}>
              Durable replication, visible operations.
            </Typography>
          </Stack>
        )}
      </Box>
    </Box>
  );
}

export default function AppLayout() {
  const { user, logout } = useAuth();
  const [mobileOpen, setMobileOpen] = useState(false);
  const [navigationCollapsed, setNavigationCollapsed] = useState(readNavigationCollapsed);

  const toggleNavigationCollapsed = () => {
    setNavigationCollapsed(value => {
      const nextValue = !value;
      writeNavigationCollapsed(nextValue);
      return nextValue;
    });
  };

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      <Drawer
        variant="permanent"
        sx={{
          display: { xs: 'none', md: 'block' },
          width: navigationCollapsed ? collapsedDrawerWidth : drawerWidth,
          flexShrink: 0,
          '& .MuiDrawer-paper': {
            width: navigationCollapsed ? collapsedDrawerWidth : drawerWidth,
            boxSizing: 'border-box',
            transition: theme => theme.transitions.create('width', { duration: theme.transitions.duration.standard })
          }
        }}
      >
        <Navigation
          collapsed={navigationCollapsed}
          onToggleCollapse={toggleNavigationCollapsed}
          onLogout={() => void logout()}
        />
      </Drawer>
      <Drawer
        variant="temporary"
        open={mobileOpen}
        onClose={() => setMobileOpen(false)}
        ModalProps={{ keepMounted: true }}
        sx={{ display: { xs: 'block', md: 'none' }, '& .MuiDrawer-paper': { width: drawerWidth, boxSizing: 'border-box' } }}
      >
        <Navigation onNavigate={() => setMobileOpen(false)} onLogout={() => void logout()} />
      </Drawer>
      <Box sx={{ flexGrow: 1, minWidth: 0 }}>
        <AppBar position="static" color="inherit" elevation={0}>
          <Toolbar sx={{ gap: 1, px: { xs: 2, md: 4 }, py: 1, minHeight: 56 }}>
            <IconButton color="primary" aria-label="Open navigation" onClick={() => setMobileOpen(true)} sx={{ display: { xs: 'inline-flex', md: 'none' } }}>
              <MenuIcon />
            </IconButton>
            <Typography component={RouterLink} to="/" variant="h6" color="primary" fontWeight={700} sx={{ display: { xs: 'block', md: 'none' }, textDecoration: 'none' }}>
              ReplicaDB
            </Typography>
            <Stack role="group" aria-label="Signed-in identity" direction="row" spacing={1.5} alignItems="center" sx={{ ml: 'auto', minWidth: 0, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
              <Typography component="span" variant="body2" fontWeight={700} noWrap>{user?.username}</Typography>
              <Typography component="span" variant="body2" color="text.secondary" noWrap>{user?.role}</Typography>
            </Stack>
          </Toolbar>
        </AppBar>
        <Box component="main" sx={{ width: '100%', maxWidth: 1600, mx: 'auto', px: { xs: 2, sm: 3, md: 4 }, py: { xs: 2, md: 4 } }}>
          <Outlet />
        </Box>
      </Box>
    </Box>
  );
}
