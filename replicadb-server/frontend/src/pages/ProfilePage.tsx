import { Alert, TextField } from '@mui/material';
import { useAuth } from '../auth/useAuth';
import PageHeader from '../components/PageHeader';
import SurfaceSection from '../components/SurfaceSection';

export default function ProfilePage() {
  const { user } = useAuth();

  return (
    <>
      <PageHeader
        title="My profile"
        description="Review your account identity and account settings."
      />
      <SurfaceSection title="Account identity" description="These details are managed by the control plane administrator.">
        <TextField label="Username" value={user?.username ?? ''} fullWidth InputProps={{ readOnly: true }} />
        <TextField label="Role" value={user?.role ?? ''} fullWidth InputProps={{ readOnly: true }} sx={{ mt: 2 }} />
      </SurfaceSection>
      <SurfaceSection
        title="Password"
        description="Password changes will be available when self-service account management is enabled."
        sx={{ mt: 3 }}
      >
        <Alert severity="info">
          Contact an administrator to change your password for now.
        </Alert>
        <TextField label="Current password" type="password" disabled fullWidth sx={{ mt: 2 }} />
        <TextField label="New password" type="password" disabled fullWidth sx={{ mt: 2 }} />
      </SurfaceSection>
    </>
  );
}