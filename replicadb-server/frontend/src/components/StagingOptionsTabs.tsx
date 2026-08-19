import { Stack, Tab, Tabs, TextField, Typography } from '@mui/material';
import { useId, useState } from 'react';

type StagingOptionsTabsProps = {
  schema: string;
  table: string;
  onChange: (field: 'schema' | 'table', value: string) => void;
};

export default function StagingOptionsTabs({ schema, table, onChange }: StagingOptionsTabsProps) {
  const [tab, setTab] = useState<'schema' | 'table'>(table ? 'table' : 'schema');
  const tabId = useId();

  const changeTab = (nextTab: 'schema' | 'table') => {
    setTab(nextTab);
  };

  return (
    <Stack spacing={2}>
      <Typography component="h3" variant="subtitle1" fontWeight={700}>
        Staging options
      </Typography>
      <Tabs
        value={tab}
        onChange={(_, value: 'schema' | 'table') => changeTab(value)}
        aria-label="Staging target mode"
      >
        <Tab
          id={`${tabId}-schema-tab`}
          aria-controls={`${tabId}-schema-panel`}
          label="Schema"
          value="schema"
        />
        <Tab
          id={`${tabId}-table-tab`}
          aria-controls={`${tabId}-table-panel`}
          label="Table"
          value="table"
        />
      </Tabs>
      {tab === 'schema' ? (
        <div role="tabpanel" id={`${tabId}-schema-panel`} aria-labelledby={`${tabId}-schema-tab`}>
          <TextField
            label="Staging schema"
            value={schema}
            onChange={event => onChange('schema', event.target.value)}
            helperText="Schema with permissions to create staging tables"
            fullWidth
          />
        </div>
      ) : (
        <div role="tabpanel" id={`${tabId}-table-panel`} aria-labelledby={`${tabId}-table-tab`}>
          <TextField
            label="Staging table"
            value={table}
            onChange={event => onChange('table', event.target.value)}
            helperText="Qualified staging table name; the table must exist"
            fullWidth
          />
        </div>
      )}
    </Stack>
  );
}
