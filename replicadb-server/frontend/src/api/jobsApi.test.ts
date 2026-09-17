import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiError, apiClient } from './client';
import { createJob, deleteJob, toJobDefinitionRequest, updateJob, type JobDefinitionFormInput } from './jobsApi';

describe('jobsApi mutations', () => {
  let mock: AxiosMockAdapter;

  beforeEach(() => {
    mock = new AxiosMockAdapter(apiClient);
  });

  afterEach(() => {
    mock.restore();
  });

  it('normalizes blank optional strings', () => {
    const request = toJobDefinitionRequest(formInput('complete'));

    expect(request.sourceWhere).toBeUndefined();
    expect(request.sourceColumns).toBeUndefined();
    expect(request.sinkStagingTable).toBeUndefined();
    expect(request.sourceDatasourceId).toBe('source-1');
    expect(request.sinkDatasourceId).toBe('sink-1');
    expect(request).not.toHaveProperty('sourceConnect');
    expect(request).not.toHaveProperty('sinkConnect');
    expect(request).not.toHaveProperty('sourceUser');
    expect(request).not.toHaveProperty('sinkUser');
    expect(request.maxAttempts).toBe(3);
    expect(request.retryBackoffSeconds).toBe(60);
    expect(request.automaticRetryEnabled).toBe(false);
  });

  it('normalizes advanced fields and keeps query instead of table', () => {
    const request = toJobDefinitionRequest({
      ...formInput('complete'),
      sourceTable: 'source_table',
      sourceQuery: 'select * from source_table',
      sourceColumns: 'id, name',
      sinkColumns: 'name, id',
      sinkStagingSchema: 'staging',
      sinkStagingTable: 'sink_stage',
      fetchSize: 250,
      bandwidthThrottling: 512,
      verbose: true
    });

    expect(request).not.toHaveProperty('sourceTable');
    expect(request.sourceQuery).toBe('select * from source_table');
    expect(request).not.toHaveProperty('sourceConnectionParams');
    expect(request).not.toHaveProperty('sinkConnectionParams');
    expect(request).not.toHaveProperty('sourceConnect');
    expect(request).not.toHaveProperty('sinkConnect');
    expect(request.fetchSize).toBe(250);
    expect(request.bandwidthThrottling).toBe(512);
    expect(request.verbose).toBe(true);
  });

  it('omits watermark fields for non-incremental modes', () => {
    const request = toJobDefinitionRequest({
      ...formInput('complete'),
      incrementalWatermarkColumn: 'updated_at',
      initialWatermarkValue: '0'
    });

    expect(request).not.toHaveProperty('incrementalWatermarkColumn');
    expect(request).not.toHaveProperty('initialWatermarkValue');
  });

  it('keeps watermark fields for incremental mode', () => {
    const request = toJobDefinitionRequest(formInput('incremental'));

    expect(request.incrementalWatermarkColumn).toBe('updated_at');
    expect(request.initialWatermarkValue).toBe('0');
  });

  it('keeps explicit retry policy fields in the normalized payload', () => {
    const request = toJobDefinitionRequest({
      ...formInput('complete'),
      maxAttempts: 5,
      retryBackoffSeconds: 90,
      automaticRetryEnabled: true
    });

    expect(request.maxAttempts).toBe(5);
    expect(request.retryBackoffSeconds).toBe(90);
    expect(request.automaticRetryEnabled).toBe(true);
    expect(request).not.toHaveProperty('leaseToken');
  });

  it('posts the normalized payload when creating a job', async () => {
    const input = formInput('complete');
    const response = { id: 'job-1', name: input.name };
    mock.onPost('/jobs').reply(201, response);

    await expect(createJob(input)).resolves.toEqual(response);

    expect(JSON.parse(mock.history.post[0].data)).toEqual(
      JSON.parse(JSON.stringify(toJobDefinitionRequest(input)))
    );
  });

  it('puts the normalized payload when updating a job', async () => {
    const input = formInput('incremental');
    const response = { id: 'job-1', name: input.name };
    mock.onPut('/jobs/job-1').reply(200, response);

    await expect(updateJob('job-1', input)).resolves.toEqual(response);

    expect(JSON.parse(mock.history.put[0].data)).toEqual(
      JSON.parse(JSON.stringify(toJobDefinitionRequest(input)))
    );
  });

  it('deletes a job without sending a request body', async () => {
    mock.onDelete('/jobs/job-1').reply(204);

    await expect(deleteJob('job-1')).resolves.toBeUndefined();

    expect(mock.history.delete[0].url).toBe('/jobs/job-1');
    expect(mock.history.delete[0].data).toBeUndefined();
  });

  it.each([403, 404, 409])('surfaces job deletion problem responses with status %s', async status => {
    const problem = { title: 'Cannot delete job', detail: 'The job has an active run.' };
    mock.onDelete('/jobs/job-1').reply(status, problem, { 'content-type': 'application/problem+json' });

    await expect(deleteJob('job-1')).rejects.toMatchObject({ status, detail: problem.detail });
  });

  it('surfaces RFC 7807 create and update failures as ApiError', async () => {
    const problem = { title: 'Invalid job', detail: 'The source table is required.' };
    mock.onPost('/jobs').reply(400, problem, { 'content-type': 'application/problem+json' });
    mock.onPut('/jobs/job-1').reply(400, problem, { 'content-type': 'application/problem+json' });

    await expect(createJob(formInput('complete'))).rejects.toMatchObject({
      status: 400,
      detail: problem.detail
    });
    await expect(updateJob('job-1', formInput('complete'))).rejects.toMatchObject({
      status: 400,
      detail: problem.detail
    });
  });
});

function formInput(mode: JobDefinitionFormInput['mode']): JobDefinitionFormInput {
  return {
    name: 'job-1',
    sourceDatasourceId: 'source-1',
    sourceDatasourceUseEnabled: true,
    sinkDatasourceId: 'sink-1',
    sinkDatasourceUseEnabled: true,
    sourceTable: 'source_table',
    sourceWhere: '',
    sinkTable: 'sink_table',
    sourceColumns: '',
    sourceQuery: '',
    sinkColumns: '',
    sinkStagingSchema: '',
    sinkStagingTable: '',
    sinkDisableEscape: false,
    sinkDisableTruncate: false,
    mode,
    jobs: 2,
    incrementalWatermarkColumn: 'updated_at',
    initialWatermarkValue: '0',
    fetchSize: 100,
    bandwidthThrottling: 0,
    verbose: false,
    maxAttempts: 3,
    retryBackoffSeconds: 60,
    automaticRetryEnabled: mode !== 'complete'
  };
}
