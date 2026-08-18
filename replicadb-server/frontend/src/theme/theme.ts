import { createTheme } from '@mui/material/styles';

export const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#0b6e69',
      contrastText: '#ffffff'
    },
    secondary: {
      main: '#b15c38',
      contrastText: '#ffffff'
    },
    background: {
      default: '#f3f6f4',
      paper: '#ffffff'
    },
    text: {
      primary: '#1b2926',
      secondary: '#50625d'
    }
  },
  typography: {
    fontFamily: '"Avenir Next", "Helvetica Neue", sans-serif',
    h1: {
      fontFamily: 'Georgia, "Times New Roman", serif',
      fontWeight: 600
    },
    h2: {
      fontFamily: 'Georgia, "Times New Roman", serif',
      fontWeight: 600
    },
    h3: {
      fontFamily: 'Georgia, "Times New Roman", serif',
      fontWeight: 600
    }
  },
  shape: {
    borderRadius: 8
  }
});
