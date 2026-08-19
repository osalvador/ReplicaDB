import { Alert, Box, Button, Stack, TextField, Typography } from '@mui/material';
import { FormEvent, useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { useNavigate } from 'react-router-dom';
import { ApiError } from '../api/client';
import { useAuth } from '../auth/useAuth';
import SurfaceSection from '../components/SurfaceSection';

export default function LoginPage() {
  const navigate = useNavigate();
  const { login } = useAuth();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [errorMessage, setErrorMessage] = useState<string>();
  const [submitting, setSubmitting] = useState(false);

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setErrorMessage(undefined);
    setSubmitting(true);

    try {
      await login(username, password);
      navigate('/');
    } catch (error) {
      setErrorMessage(error instanceof ApiError ? error.detail : 'Unable to sign in.');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Box
      sx={{
        minHeight: '100vh',
        display: 'grid',
        placeItems: 'center',
        backgroundColor: 'background.default',
        p: { xs: 2, sm: 3 }
      }}
    >
      <Box sx={{ width: 'min(100%, 440px)' }}>
        <Typography
          component={RouterLink}
          to="/"
          variant="h5"
          color="primary"
          fontWeight={700}
          sx={{
            display: 'inline-block',
            borderRadius: 1,
            px: 1,
            py: 0.5,
            textDecoration: 'none',
            '&:hover': { backgroundColor: 'rgba(11, 110, 105, 0.08)' }
          }}
        >
          ReplicaDB
        </Typography>
        <Box component="form" aria-label="Sign-in form" onSubmit={submit} sx={{ mt: 2 }}>
          <SurfaceSection
            title="Sign in"
            description="Access your database replication control plane."
            headingLevel={1}
          >
            <Stack spacing={2.5}>
              {errorMessage && <Alert severity="error">{errorMessage}</Alert>}
              <TextField
                label="Username"
                value={username}
                onChange={event => setUsername(event.target.value)}
                autoComplete="username"
                fullWidth
                autoFocus
              />
              <TextField
                label="Password"
                type="password"
                value={password}
                onChange={event => setPassword(event.target.value)}
                autoComplete="current-password"
                fullWidth
              />
              <Button
                type="submit"
                variant="contained"
                size="large"
                fullWidth
                disabled={submitting || !username.trim() || !password}
              >
                {submitting ? 'Signing in...' : 'Sign in'}
              </Button>
            </Stack>
          </SurfaceSection>
        </Box>
      </Box>
    </Box>
  );
}
