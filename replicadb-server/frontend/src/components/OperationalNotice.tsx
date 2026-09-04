import { Alert, type AlertColor, type AlertProps } from '@mui/material';
import { alpha } from '@mui/material/styles';

type OperationalNoticeProps = Pick<AlertProps, 'children' | 'severity'> & {
  severity: AlertColor;
};

export default function OperationalNotice({ children, severity }: OperationalNoticeProps) {
  return (
    <Alert
      severity={severity}
      variant="standard"
      sx={theme => ({
        alignItems: 'center',
        border: '1px solid',
        borderColor: alpha(theme.palette[severity].main, 0.22),
        borderRadius: `${theme.tokens.section.radius}px`,
        padding: '8px 12px',
        '& .MuiAlert-icon': {
          alignItems: 'center',
          fontSize: 20,
          marginRight: 1,
          padding: 0
        },
        '& .MuiAlert-message': {
          fontSize: theme.typography.body2.fontSize,
          lineHeight: theme.typography.body2.lineHeight,
          padding: 0
        }
      })}
    >
      {children}
    </Alert>
  );
}
