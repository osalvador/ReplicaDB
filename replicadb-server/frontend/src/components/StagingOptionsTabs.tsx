import { Stack, Tab, Tabs, TextField, Typography } from '@mui/material';
import { useId, useState } from 'react';

export type StagingTarget = 'schema' | 'table';

type StagingOptionsTabsProps = {
  schema: string;
  table: string;
  onChange: (field: 'schema' | 'table', value: string) => void;
  target?: StagingTarget;
  onTargetChange?: (target: StagingTarget) => void;
};

export default function StagingOptionsTabs({
  schema,
  table,
  onChange,
  target,
  onTargetChange
}: StagingOptionsTabsProps) {
  const [uncontrolledTarget, setUncontrolledTarget] = useState<StagingTarget>(table ? 'table' : 'schema');
  const selectedTarget = target ?? uncontrolledTarget;
  const tabId = useId();

  const changeTarget = (nextTarget: StagingTarget) => {
    onTargetChange?.(nextTarget);
    if (!onTargetChange) {
      setUncontrolledTarget(nextTarget);
    }
  };

  return (
    <Stack spacing={2}>
      <Typography component="h3" variant="subtitle1" fontWeight={700}>
        Staging target
      </Typography>
      <Typography color="text.secondary" variant="body2">
        Choose one staging target: let ReplicaDB create a staging table in a schema, or use an existing qualified staging table.
      </Typography>
      <Tabs
        value={selectedTarget}
        onChange={(_, value: StagingTarget) => changeTarget(value)}
        aria-label="Staging target"
      >
        <Tab
          id={`${tabId}-schema-tab`}
          aria-controls={`${tabId}-schema-panel`}
          label="Create in schema"
          value="schema"
        />
        <Tab
          id={`${tabId}-table-tab`}
          aria-controls={`${tabId}-table-panel`}
          label="Use existing table"
          value="table"
        />
      </Tabs>
      {selectedTarget === 'schema' ? (
        <div role="tabpanel" id={`${tabId}-schema-panel`} aria-labelledby={`${tabId}-schema-tab`}>
          <TextField
            label="Staging schema"
            value={schema}
            onChange={event => onChange('schema', event.target.value)}
            helperText="ReplicaDB creates a staging table in this schema."
            fullWidth
          />
        </div>
      ) : (
        <div role="tabpanel" id={`${tabId}-table-panel`} aria-labelledby={`${tabId}-table-tab`}>
          <TextField
            label="Existing staging table"
            value={table}
            onChange={event => onChange('table', event.target.value)}
            helperText="Use a qualified staging table that already exists."
            fullWidth
          />
        </div>
      )}
    </Stack>
  );
}
