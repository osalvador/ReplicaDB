import { Alert, Box, Button, Stack, TextField, Typography } from '@mui/material';
import { FormEvent, useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { useNavigate } from 'react-router-dom';
import { ApiError, isNetworkError } from '../api/client';
import { useAuth } from '../auth/useAuth';
import SurfaceSection from '../components/SurfaceSection';

type LoginErrorKind = 'credentials' | 'throttled' | 'network' | 'unavailable' | 'unknown';

type LoginError = {
  kind: LoginErrorKind;
  message: string;
};

function loginError(error: unknown): LoginError {
  if (isNetworkError(error)) {
    return {
      kind: 'network',
      message: 'Unable to reach ReplicaDB. Check that the server is running or your connection is available, then try again.'
    };
  }

  if (error instanceof ApiError) {
    if (error.status === 401) {
      return {
        kind: 'credentials',
        message: 'Sign-in was not accepted. Check your username and password, then try again. If you still need access, contact your administrator.'
      };
    }

    if (error.status === 429) {
      return {
        kind: 'throttled',
        message: 'Too many failed sign-in attempts. Wait a few minutes before trying again.'
      };
    }

    if (error.status === 408 || error.status >= 500) {
      return {
        kind: 'unavailable',
        message: 'ReplicaDB is temporarily unavailable. Check the server and try again.'
      };
    }
  }

  return {
    kind: 'unknown',
    message: 'Unable to sign in right now. Try again, or contact your administrator if the problem continues.'
  };
}

export default function LoginPage() {
  const navigate = useNavigate();
  const { login } = useAuth();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loginErrorState, setLoginErrorState] = useState<LoginError>();
  const [submitting, setSubmitting] = useState(false);

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setLoginErrorState(undefined);
    setSubmitting(true);

    try {
      await login(username, password);
      navigate('/');
    } catch (error) {
      setLoginErrorState(loginError(error));
    } finally {
      setSubmitting(false);
    }
  };

  const canRetry = loginErrorState?.kind === 'network'
    || loginErrorState?.kind === 'unavailable'
    || loginErrorState?.kind === 'unknown';

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
              {loginErrorState && (
                <Alert
                  severity="error"
                  action={canRetry ? (
                    <Button type="submit" color="inherit" size="small">
                      Try again
                    </Button>
                  ) : undefined}
                >
                  {loginErrorState.message}
                </Alert>
              )}
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
              <Typography color="text.secondary" variant="body2">
                Need access or help signing in? Contact your ReplicaDB administrator.
              </Typography>
            </Stack>
          </SurfaceSection>
        </Box>
      </Box>
    </Box>
  );
}
