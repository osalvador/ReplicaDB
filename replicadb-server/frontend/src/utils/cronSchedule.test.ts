import { describe, expect, it } from 'vitest';
import {
  buildCronExpression,
  createDefaultCronBuilder,
  getTimeZoneOption,
  parseCronExpression,
  timeZoneLabel,
  validateQuartzExpression
} from './cronSchedule';

describe('cron schedule builder', () => {
  it('builds valid Quartz expressions for each guided frequency', () => {
    const builder = createDefaultCronBuilder();

    expect(buildCronExpression({ ...builder, mode: 'everyMinutes', everyMinutes: '15' }).expression)
      .toBe('0 0/15 * * * ?');
    expect(buildCronExpression({ ...builder, mode: 'hourly', minute: '10' }).expression)
      .toBe('0 10 * * * ?');
    expect(buildCronExpression({ ...builder, mode: 'daily', hour: '2', minute: '15' }).expression)
      .toBe('0 15 2 * * ?');
    expect(buildCronExpression({ ...builder, mode: 'weekly', hour: '2', minute: '15', dayOfWeek: 'MON' }).expression)
      .toBe('0 15 2 ? * MON');
    expect(buildCronExpression({ ...builder, mode: 'monthly', hour: '2', minute: '15', dayOfMonth: '10' }).expression)
      .toBe('0 15 2 10 * ?');
  });

  it('rejects values outside the Quartz field ranges', () => {
    const builder = createDefaultCronBuilder();

    expect(buildCronExpression({ ...builder, mode: 'everyMinutes', everyMinutes: '0' }).error)
      .toContain('between 1 and 59');
    expect(buildCronExpression({ ...builder, mode: 'daily', hour: '24' }).error)
      .toContain('between 0 and 23');
    expect(buildCronExpression({ ...builder, mode: 'monthly', dayOfMonth: '32' }).error)
      .toContain('between 1 and 31');
    expect(validateQuartzExpression('0 60 * * * ?')).toContain('minutes');
    expect(validateQuartzExpression('0 0 * * * *')).toContain('either');
  });

  it('preserves supported expressions when opening an editor', () => {
    expect(parseCronExpression('0 15 2 ? * MON')).toMatchObject({
      mode: 'weekly',
      minute: '15',
      hour: '2',
      dayOfWeek: 'MON'
    });
    expect(parseCronExpression('0 0 1 1 1 ?')).toMatchObject({
      mode: 'custom',
      customExpression: '0 0 1 1 1 ?'
    });
  });

  it('labels time zones with their standard UTC offset', () => {
    const madrid = getTimeZoneOption('Europe/Madrid');

    expect(madrid.offset).toBe('UTC+01:00');
    expect(timeZoneLabel(madrid)).toBe('UTC+01:00 · Europe/Madrid');
    expect(getTimeZoneOption('UTC').offset).toBe('UTC+00:00');
    expect(getTimeZoneOption('Australia/Sydney').offset).toBe('UTC+10:00');
  });
});
