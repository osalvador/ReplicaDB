import AxiosMockAdapter from 'axios-mock-adapter';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApiError, apiClient } from './client';
import { createJob, toJobDefinitionRequest, updateJob, type JobDefinitionFormInput } from './jobsApi';

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

    expect(request.sourceUser).toBeUndefined();
    expect(request.sourcePassword).toBeUndefined();
    expect(request.sourceWhere).toBeUndefined();
    expect(request.sinkUser).toBeUndefined();
    expect(request.sinkPassword).toBeUndefined();
    expect(request.sourceColumns).toBeUndefined();
    expect(request.sinkStagingTable).toBeUndefined();
    expect(request.sourceConnectionParams).toBeUndefined();
  });

  it('normalizes advanced fields and keeps query instead of table', () => {
    const request = toJobDefinitionRequest({
      ...formInput('complete'),
      sourceTable: 'source_table',
      sourceQuery: 'select * from source_table',
      sourceColumns: 'id, name',
      sourceConnectionParams: { format: 'RFC4180' },
      sinkColumns: 'name, id',
      sinkStagingSchema: 'staging',
      sinkStagingTable: 'sink_stage',
      sinkConnectionParams: { topic: 'orders' },
      fetchSize: 250,
      bandwidthThrottling: 512,
      verbose: true
    });

    expect(request).not.toHaveProperty('sourceTable');
    expect(request.sourceQuery).toBe('select * from source_table');
    expect(request.sourceConnectionParams).toEqual({ format: 'RFC4180' });
    expect(request.sinkConnectionParams).toEqual({ topic: 'orders' });
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
    sourceConnect: 'jdbc:source',
    sourceUser: '',
    sourcePassword: '',
    sourceTable: 'source_table',
    sourceWhere: '',
    sinkConnect: 'jdbc:sink',
    sinkUser: '',
    sinkPassword: '',
    sinkTable: 'sink_table',
    sourceAuthMode: '',
    sourceAuthPrincipalId: '',
    sourceAuthLoginHint: '',
    sourceAuthClientCertificate: '',
    sourceAuthClientKey: '',
    sourceConnectionParams: {},
    sourceColumns: '',
    sourceQuery: '',
    sinkAuthMode: '',
    sinkAuthPrincipalId: '',
    sinkAuthLoginHint: '',
    sinkAuthClientCertificate: '',
    sinkAuthClientKey: '',
    sinkConnectionParams: {},
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
    verbose: false
  };
}
