import { Button } from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import PageHeader from '../components/PageHeader';

export default function NotAuthorizedPage() {
  return (
    <PageHeader
      title="Not authorized"
      description="You do not have permission to view this page."
      actions={
        <Button component={RouterLink} to="/" variant="outlined">
          Back to dashboard
        </Button>
      }
    />
  );
}
