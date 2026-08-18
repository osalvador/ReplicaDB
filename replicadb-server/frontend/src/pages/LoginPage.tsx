import { Alert, Box, Button, Paper, Stack, TextField, Typography } from '@mui/material';
import { FormEvent, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ApiError } from '../api/client';
import { useAuth } from '../auth/useAuth';

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
    <Box sx={{ minHeight: '100vh', display: 'grid', placeItems: 'center', p: 3 }}>
      <Paper component="form" onSubmit={submit} elevation={2} sx={{ width: 'min(100%, 420px)', p: 4 }}>
        <Stack spacing={3}>
          <Box>
            <Typography component="p" color="primary" fontWeight={700} letterSpacing="0.08em" textTransform="uppercase">
              ReplicaDB
            </Typography>
            <Typography component="h1" variant="h4" sx={{ mt: 1 }}>
              Sign in
            </Typography>
          </Box>
          {errorMessage && <Alert severity="error">{errorMessage}</Alert>}
          <TextField
            label="Username"
            value={username}
            onChange={event => setUsername(event.target.value)}
            autoComplete="username"
            fullWidth
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
            disabled={submitting || !username.trim() || !password}
          >
            {submitting ? 'Signing in...' : 'Sign in'}
          </Button>
        </Stack>
      </Paper>
    </Box>
  );
}
