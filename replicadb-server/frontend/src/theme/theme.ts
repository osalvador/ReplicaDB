import { createTheme, type Shadows } from '@mui/material/styles';

export interface ReplicaDbVisualTokens {
  brand: {
    primary: string;
    secondary: string;
  };
  surface: {
    page: string;
    paper: string;
    subtle: string;
  };
  text: {
    primary: string;
    secondary: string;
  };
  semantic: {
    success: string;
    info: string;
    warning: string;
    error: string;
  };
  focus: {
    ring: string;
  };
  control: {
    height: number;
  };
  section: {
    radius: number;
  };
  spacing: {
    unit: number;
  };
}

export const visualTokens: ReplicaDbVisualTokens = {
  brand: {
    primary: '#0B6E69',
    secondary: '#B15C38'
  },
  surface: {
    page: '#F3F6F4',
    paper: '#FFFFFF',
    subtle: '#E8F0ED'
  },
  text: {
    primary: '#1B2926',
    secondary: '#50625D'
  },
  semantic: {
    success: '#216E4A',
    info: '#1769AA',
    warning: '#8A4B08',
    error: '#B3261E'
  },
  focus: {
    ring: '#0B6E69'
  },
  control: {
    height: 40
  },
  section: {
    radius: 8
  },
  spacing: {
    unit: 8
  }
};

const elevationShadows: Shadows = [
  'none',
  '0 1px 2px 0 rgba(27, 41, 38, 0.08)',
  '0 1px 3px 0 rgba(27, 41, 38, 0.1)',
  '0 2px 6px 0 rgba(27, 41, 38, 0.1)',
  '0 2px 8px 0 rgba(27, 41, 38, 0.12)',
  '0 3px 10px 0 rgba(27, 41, 38, 0.12)',
  '0 3px 12px 0 rgba(27, 41, 38, 0.13)',
  '0 4px 14px 0 rgba(27, 41, 38, 0.13)',
  '0 4px 16px 0 rgba(27, 41, 38, 0.14)',
  '0 5px 18px 0 rgba(27, 41, 38, 0.14)',
  '0 5px 20px 0 rgba(27, 41, 38, 0.15)',
  '0 6px 22px 0 rgba(27, 41, 38, 0.15)',
  '0 6px 24px 0 rgba(27, 41, 38, 0.16)',
  '0 7px 26px 0 rgba(27, 41, 38, 0.16)',
  '0 7px 28px 0 rgba(27, 41, 38, 0.17)',
  '0 8px 30px 0 rgba(27, 41, 38, 0.17)',
  '0 8px 32px 0 rgba(27, 41, 38, 0.18)',
  '0 9px 34px 0 rgba(27, 41, 38, 0.18)',
  '0 9px 36px 0 rgba(27, 41, 38, 0.19)',
  '0 10px 38px 0 rgba(27, 41, 38, 0.19)',
  '0 10px 40px 0 rgba(27, 41, 38, 0.2)',
  '0 11px 42px 0 rgba(27, 41, 38, 0.2)',
  '0 11px 44px 0 rgba(27, 41, 38, 0.21)',
  '0 12px 46px 0 rgba(27, 41, 38, 0.21)',
  '0 12px 48px 0 rgba(27, 41, 38, 0.22)'
];

declare module '@mui/material/styles' {
  interface Theme {
    tokens: ReplicaDbVisualTokens;
  }

  interface ThemeOptions {
    tokens?: ReplicaDbVisualTokens;
  }
}

export const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: visualTokens.brand.primary,
      contrastText: '#FFFFFF'
    },
    secondary: {
      main: visualTokens.brand.secondary,
      contrastText: '#FFFFFF'
    },
    success: {
      main: visualTokens.semantic.success,
      contrastText: '#FFFFFF'
    },
    info: {
      main: visualTokens.semantic.info,
      contrastText: '#FFFFFF'
    },
    warning: {
      main: visualTokens.semantic.warning,
      contrastText: '#FFFFFF'
    },
    error: {
      main: visualTokens.semantic.error,
      contrastText: '#FFFFFF'
    },
    background: {
      default: visualTokens.surface.page,
      paper: visualTokens.surface.paper
    },
    text: {
      primary: visualTokens.text.primary,
      secondary: visualTokens.text.secondary
    }
  },
  tokens: visualTokens,
  spacing: visualTokens.spacing.unit,
  shadows: elevationShadows,
  typography: {
    fontFamily: '"Avenir Next", "Helvetica Neue", sans-serif',
    h1: {
      fontFamily: 'Georgia, "Times New Roman", serif',
      fontSize: '2.25rem',
      lineHeight: 1.2,
      fontWeight: 600
    },
    h2: {
      fontFamily: 'Georgia, "Times New Roman", serif',
      fontSize: '1.75rem',
      lineHeight: 1.25,
      fontWeight: 600
    },
    h3: {
      fontFamily: 'Georgia, "Times New Roman", serif',
      fontSize: '1.5rem',
      lineHeight: 1.3,
      fontWeight: 600
    },
    h4: {
      fontFamily: 'Georgia, "Times New Roman", serif',
      fontSize: '1.25rem',
      lineHeight: 1.35,
      fontWeight: 600
    },
    h5: {
      fontSize: '1.125rem',
      lineHeight: 1.4,
      fontWeight: 700
    },
    h6: {
      fontSize: '1rem',
      lineHeight: 1.5,
      fontWeight: 700
    },
    subtitle1: {
      fontSize: '1rem',
      lineHeight: 1.5,
      fontWeight: 600
    },
    body1: {
      fontSize: '1rem',
      lineHeight: 1.5
    },
    body2: {
      fontSize: '0.875rem',
      lineHeight: 1.45
    },
    button: {
      fontSize: '0.875rem',
      lineHeight: 1.25,
      fontWeight: 700,
      textTransform: 'none'
    }
  },
  shape: {
    borderRadius: visualTokens.section.radius
  },
  breakpoints: {
    values: {
      xs: 0,
      sm: 600,
      md: 900,
      lg: 1200,
      xl: 1536
    }
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        ':root': {
          colorScheme: 'light'
        },
        '*:focus-visible': {
          outline: `3px solid ${visualTokens.focus.ring}`,
          outlineOffset: '2px'
        }
      }
    },
    MuiAppBar: {
      defaultProps: {
        elevation: 0
      },
      styleOverrides: {
        root: {
          backgroundColor: visualTokens.surface.paper,
          color: visualTokens.text.primary,
          borderBottom: '1px solid rgba(80, 98, 93, 0.18)',
          boxShadow: 'none',
          '& .MuiToolbar-root': {
            minHeight: visualTokens.control.height,
            paddingBlock: 8
          }
        }
      }
    },
    MuiButton: {
      defaultProps: {
        disableElevation: true
      },
      styleOverrides: {
        root: {
          minHeight: visualTokens.control.height,
          borderRadius: 8,
          paddingInline: 16,
          fontWeight: 700,
          textTransform: 'none',
          boxShadow: 'none',
          '&:hover': {
            boxShadow: 'none'
          },
          '&.Mui-focusVisible': {
            outline: `3px solid ${visualTokens.focus.ring}`,
            outlineOffset: '2px'
          },
          '&.Mui-disabled': {
            opacity: 0.58
          }
        },
        contained: {
          '&:hover': {
            backgroundColor: 'rgba(11, 110, 105, 0.9)'
          }
        },
        outlined: {
          borderColor: 'rgba(11, 110, 105, 0.55)',
          '&:hover': {
            borderColor: visualTokens.brand.primary,
            backgroundColor: 'rgba(11, 110, 105, 0.08)'
          }
        },
        text: {
          '&:hover': {
            backgroundColor: 'rgba(11, 110, 105, 0.08)'
          }
        },
        sizeSmall: {
          minHeight: 32,
          paddingInline: 12
        },
        sizeLarge: {
          minHeight: 48,
          paddingInline: 20
        }
      }
    },
    MuiIconButton: {
      styleOverrides: {
        root: {
          width: visualTokens.control.height,
          height: visualTokens.control.height,
          borderRadius: 8,
          '&:hover': {
            backgroundColor: 'rgba(11, 110, 105, 0.08)'
          },
          '&.Mui-focusVisible': {
            outline: `3px solid ${visualTokens.focus.ring}`,
            outlineOffset: '2px'
          },
          '&.Mui-disabled': {
            opacity: 0.58
          }
        }
      }
    },
    MuiTextField: {
      defaultProps: {
        variant: 'outlined',
        size: 'small'
      }
    },
    MuiOutlinedInput: {
      styleOverrides: {
        root: {
          minHeight: visualTokens.control.height,
          borderRadius: 6,
          backgroundColor: visualTokens.surface.paper,
          '&:hover .MuiOutlinedInput-notchedOutline': {
            borderColor: visualTokens.brand.primary
          },
          '&.Mui-focused .MuiOutlinedInput-notchedOutline': {
            borderColor: visualTokens.brand.primary,
            borderWidth: 2,
            boxShadow: '0 0 0 3px rgba(11, 110, 105, 0.16)'
          },
          '&.Mui-error .MuiOutlinedInput-notchedOutline': {
            borderColor: visualTokens.semantic.error
          },
          '&.Mui-disabled': {
            backgroundColor: visualTokens.surface.subtle
          }
        },
        notchedOutline: {
          borderColor: 'rgba(80, 98, 93, 0.42)'
        }
      }
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          backgroundImage: 'none',
          backgroundColor: visualTokens.surface.paper
        }
      }
    },
    MuiCard: {
      defaultProps: {
        variant: 'outlined'
      },
      styleOverrides: {
        root: {
          borderColor: 'rgba(80, 98, 93, 0.2)',
          borderRadius: visualTokens.section.radius,
          boxShadow: 'none'
        }
      }
    },
    MuiAlert: {
      styleOverrides: {
        root: {
          borderRadius: visualTokens.section.radius,
          padding: '8px 12px',
          alignItems: 'flex-start'
        },
        icon: {
          paddingBlock: 3
        },
        message: {
          paddingBlock: 3
        }
      }
    },
    MuiChip: {
      styleOverrides: {
        root: {
          minHeight: 28,
          height: 28,
          borderRadius: 6,
          fontWeight: 700
        },
        label: {
          paddingInline: 10
        },
        deleteIcon: {
          marginRight: 6
        }
      }
    },
    MuiTable: {
      styleOverrides: {
        root: {
          backgroundColor: visualTokens.surface.paper,
          '& .MuiTableHead-root': {
            backgroundColor: visualTokens.surface.subtle
          },
          '& .MuiTableCell-root': {
            padding: '12px 16px',
            borderBottom: '1px solid rgba(80, 98, 93, 0.16)'
          },
          '& .MuiTableCell-head': {
            color: visualTokens.text.secondary,
            fontSize: '0.75rem',
            fontWeight: 700,
            letterSpacing: '0.04em',
            textTransform: 'uppercase'
          },
          '& .MuiTableBody-root .MuiTableRow-root:hover': {
            backgroundColor: 'rgba(11, 110, 105, 0.05)'
          }
        }
      }
    },
    MuiDialog: {
      styleOverrides: {
        paper: {
          borderRadius: visualTokens.section.radius,
          border: '1px solid rgba(80, 98, 93, 0.2)',
          boxShadow: elevationShadows[4]
        },
        paperFullScreen: {
          border: 0,
          borderRadius: 0
        }
      }
    },
    MuiTabs: {
      styleOverrides: {
        root: {
          minHeight: visualTokens.control.height,
          borderBottom: '1px solid rgba(80, 98, 93, 0.18)'
        },
        indicator: {
          height: 3,
          borderRadius: '3px 3px 0 0',
          backgroundColor: visualTokens.brand.primary
        }
      }
    },
    MuiTablePagination: {
      styleOverrides: {
        root: {
          borderTop: '1px solid rgba(80, 98, 93, 0.16)'
        },
        toolbar: {
          minHeight: 48,
          paddingInline: 8
        },
        select: {
          borderRadius: 4,
          '&:focus': {
            borderRadius: 4,
            backgroundColor: 'rgba(11, 110, 105, 0.08)'
          }
        },
        actions: {
          '& .MuiIconButton-root': {
            width: 32,
            height: 32
          }
        }
      }
    }
  }
});
