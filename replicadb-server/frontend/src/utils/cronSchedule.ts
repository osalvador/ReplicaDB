export type CronMode = 'everyMinutes' | 'hourly' | 'daily' | 'weekly' | 'monthly' | 'custom';

export type CronBuilder = {
  mode: CronMode;
  everyMinutes: string;
  minute: string;
  hour: string;
  dayOfWeek: string;
  dayOfMonth: string;
  customExpression: string;
};

export type CronExpressionResult = {
  expression?: string;
  error?: string;
};

export type TimeZoneOption = {
  value: string;
  offset: string;
};

export const cronModeOptions: Array<{ value: CronMode; label: string }> = [
  { value: 'everyMinutes', label: 'Every few minutes' },
  { value: 'hourly', label: 'Every hour' },
  { value: 'daily', label: 'Every day' },
  { value: 'weekly', label: 'Every week' },
  { value: 'monthly', label: 'Every month' },
  { value: 'custom', label: 'Advanced CRON expression' }
];

export const dayOfWeekOptions = [
  { value: 'SUN', label: 'Sunday' },
  { value: 'MON', label: 'Monday' },
  { value: 'TUE', label: 'Tuesday' },
  { value: 'WED', label: 'Wednesday' },
  { value: 'THU', label: 'Thursday' },
  { value: 'FRI', label: 'Friday' },
  { value: 'SAT', label: 'Saturday' }
];

const fallbackTimeZones = [
  'UTC',
  'Africa/Cairo',
  'America/Los_Angeles',
  'America/Mexico_City',
  'America/New_York',
  'America/Sao_Paulo',
  'Asia/Kolkata',
  'Asia/Shanghai',
  'Asia/Tokyo',
  'Australia/Sydney',
  'Europe/Madrid',
  'Pacific/Auckland'
];

const quartzFieldNames = ['seconds', 'minutes', 'hours', 'day of month', 'month', 'day of week', 'year'];
const quartzFieldRanges: Array<[number, number]> = [
  [0, 59],
  [0, 59],
  [0, 23],
  [1, 31],
  [1, 12],
  [1, 7],
  [1970, 2199]
];
const monthAliases = new Set(['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']);
const dayAliases = new Set(dayOfWeekOptions.map(option => option.value));

export function createDefaultCronBuilder(): CronBuilder {
  return {
    mode: 'hourly',
    everyMinutes: '15',
    minute: '0',
    hour: '9',
    dayOfWeek: 'MON',
    dayOfMonth: '1',
    customExpression: ''
  };
}

function integerValue(value: string, fieldName: string, minimum: number, maximum: number): string | undefined {
  const normalizedValue = value.trim();
  if (!/^\d+$/.test(normalizedValue)) {
    return `${fieldName} must be a whole number between ${minimum} and ${maximum}.`;
  }

  const numericValue = Number(normalizedValue);
  if (!Number.isSafeInteger(numericValue) || numericValue < minimum || numericValue > maximum) {
    return `${fieldName} must be a whole number between ${minimum} and ${maximum}.`;
  }

  return undefined;
}

function buildFixedTimeExpression(builder: CronBuilder, dayOfMonth: string, dayOfWeek: string): CronExpressionResult {
  const minuteError = integerValue(builder.minute, 'Minute', 0, 59);
  if (minuteError) {
    return { error: minuteError };
  }

  const hourError = integerValue(builder.hour, 'Hour', 0, 23);
  if (hourError) {
    return { error: hourError };
  }

  return { expression: `0 ${builder.minute} ${builder.hour} ${dayOfMonth} * ${dayOfWeek}` };
}

export function validateQuartzExpression(expression: string): string | undefined {
  const normalizedExpression = expression.trim();
  const fields = normalizedExpression ? normalizedExpression.split(/\s+/) : [];
  if (fields.length !== 6 && fields.length !== 7) {
    return 'Use a Quartz CRON expression with 6 or 7 fields: seconds, minutes, hours, day, month, weekday, and optionally year.';
  }

  for (const [index, field] of fields.entries()) {
    const fieldName = quartzFieldNames[index];
    const [minimum, maximum] = quartzFieldRanges[index];
    if (!/^[\dA-Za-z*?,/#LW-]+$/i.test(field) || (field.includes('?') && field !== '?')) {
      return `The ${fieldName} field is not valid.`;
    }

    const aliases = index === 4 ? monthAliases : index === 5 ? dayAliases : new Set<string>();
    const invalidAliases = (field.match(/[A-Za-z]+/g) ?? [])
      .map(alias => alias.toUpperCase())
      .filter(alias => alias !== 'L' && alias !== 'W' && !aliases.has(alias));
    if (invalidAliases.length > 0) {
      return `The ${fieldName} field contains an unknown value.`;
    }

    const numericValues = field.match(/\d+/g) ?? [];
    if (numericValues.some(value => {
      const numericValue = Number(value);
      return !Number.isSafeInteger(numericValue) || numericValue < minimum || numericValue > maximum;
    })) {
      return `The ${fieldName} field must use values from ${minimum} to ${maximum}.`;
    }

    const steps = field.match(/\/(\d+)/g) ?? [];
    if (steps.some(step => Number(step.slice(1)) < 1)) {
      return `The ${fieldName} field must use a step greater than zero.`;
    }
  }

  const dayOfMonthIsUnspecified = fields[3] === '?';
  const dayOfWeekIsUnspecified = fields[5] === '?';
  if (dayOfMonthIsUnspecified === dayOfWeekIsUnspecified) {
    return 'Use ? in either the day-of-month or day-of-week field.';
  }

  return undefined;
}

export function buildCronExpression(builder: CronBuilder): CronExpressionResult {
  switch (builder.mode) {
    case 'everyMinutes': {
      const intervalError = integerValue(builder.everyMinutes, 'Interval', 1, 59);
      if (intervalError) {
        return { error: intervalError };
      }
      return { expression: `0 0/${builder.everyMinutes} * * * ?` };
    }
    case 'hourly': {
      const minuteError = integerValue(builder.minute, 'Minute', 0, 59);
      return minuteError ? { error: minuteError } : { expression: `0 ${builder.minute} * * * ?` };
    }
    case 'daily':
      return buildFixedTimeExpression(builder, '*', '?');
    case 'weekly':
      if (!dayOfWeekOptions.some(option => option.value === builder.dayOfWeek)) {
        return { error: 'Choose a valid day of the week.' };
      }
      return buildFixedTimeExpression(builder, '?', builder.dayOfWeek);
    case 'monthly': {
      const dayError = integerValue(builder.dayOfMonth, 'Day of month', 1, 31);
      if (dayError) {
        return { error: dayError };
      }
      return buildFixedTimeExpression(builder, builder.dayOfMonth, '?');
    }
    case 'custom': {
      const expression = builder.customExpression.trim();
      const error = validateQuartzExpression(expression);
      return error ? { error } : { expression };
    }
  }
}

function numericField(value: string): boolean {
  return /^\d+$/.test(value);
}

function parseDayOfWeek(value: string): string {
  if (dayOfWeekOptions.some(option => option.value === value)) {
    return value;
  }
  const numericValue = Number(value);
  return numericValue >= 1 && numericValue <= 7 ? dayOfWeekOptions[numericValue - 1].value : 'MON';
}

export function parseCronExpression(expression: string): CronBuilder {
  const builder = createDefaultCronBuilder();
  const normalizedExpression = expression.trim();
  const fields = normalizedExpression.split(/\s+/);
  if (fields.length !== 6 || fields[0] !== '0') {
    return { ...builder, mode: 'custom', customExpression: normalizedExpression };
  }

  const [seconds, minute, hour, dayOfMonth, month, dayOfWeek] = fields;
  const everyMinutesMatch = minute.match(/^0\/([1-9]\d*)$/);
  if (seconds === '0' && everyMinutesMatch && hour === '*' && dayOfMonth === '*' && month === '*' && dayOfWeek === '?') {
    return { ...builder, mode: 'everyMinutes', everyMinutes: everyMinutesMatch[1] };
  }
  if (seconds === '0' && numericField(minute) && hour === '*' && dayOfMonth === '*' && month === '*' && dayOfWeek === '?') {
    return { ...builder, mode: 'hourly', minute };
  }
  if (seconds === '0' && numericField(minute) && numericField(hour) && dayOfMonth === '*' && month === '*' && dayOfWeek === '?') {
    return { ...builder, mode: 'daily', minute, hour };
  }
  if (seconds === '0' && numericField(minute) && numericField(hour) && dayOfMonth === '?' && month === '*' && dayOfWeek !== '?') {
    return { ...builder, mode: 'weekly', minute, hour, dayOfWeek: parseDayOfWeek(dayOfWeek) };
  }
  if (seconds === '0' && numericField(minute) && numericField(hour) && numericField(dayOfMonth) && month === '*' && dayOfWeek === '?') {
    return { ...builder, mode: 'monthly', minute, hour, dayOfMonth };
  }

  return { ...builder, mode: 'custom', customExpression: normalizedExpression };
}

type IntlWithTimeZoneValues = typeof Intl & {
  supportedValuesOf?: (key: 'timeZone') => string[];
};

function standardUtcOffset(timeZone: string): string {
  try {
    const candidateDates = [
      new Date(Date.UTC(2026, 0, 15, 12)),
      new Date(Date.UTC(2026, 6, 15, 12))
    ];
    for (const date of candidateDates) {
      const parts = new Intl.DateTimeFormat('en-US', {
        timeZone,
        timeZoneName: 'long'
      }).formatToParts(date);
      const timeZoneName = parts.find(part => part.type === 'timeZoneName')?.value ?? '';
      if (/\bstandard\b/i.test(timeZoneName)) {
        return utcOffsetForDate(timeZone, date);
      }
    }
    return utcOffsetForDate(timeZone, candidateDates[0]);
  } catch {
    return 'UTC+00:00';
  }
}

function utcOffsetForDate(timeZone: string, date: Date): string {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    timeZoneName: 'longOffset'
  }).formatToParts(date);
  const timeZoneName = parts.find(part => part.type === 'timeZoneName')?.value ?? 'GMT';
  const offset = timeZoneName.match(/^GMT([+-]\d{2}:\d{2})$/)?.[1] ?? '+00:00';
  return `UTC${offset}`;
}

function offsetMinutes(offset: string): number {
  const sign = offset[3] === '-' ? -1 : 1;
  const [hours, minutes] = offset.slice(4).split(':').map(Number);
  return sign * (hours * 60 + minutes);
}

function createTimeZoneOptions(): TimeZoneOption[] {
  const intlWithTimeZoneValues = Intl as IntlWithTimeZoneValues;
  const supportedTimeZones = intlWithTimeZoneValues.supportedValuesOf?.('timeZone') ?? fallbackTimeZones;
  const values = Array.from(new Set(['UTC', ...supportedTimeZones]));
  return values
    .map(value => ({ value, offset: standardUtcOffset(value) }))
    .sort((left, right) => {
      if (left.value === 'UTC') return -1;
      if (right.value === 'UTC') return 1;
      return offsetMinutes(left.offset) - offsetMinutes(right.offset) || left.value.localeCompare(right.value);
    });
}

export const timeZoneOptions = createTimeZoneOptions();

export function getTimeZoneOption(value: string | null | undefined): TimeZoneOption {
  const normalizedValue = value?.trim() || 'UTC';
  return timeZoneOptions.find(option => option.value === normalizedValue) ?? {
    value: normalizedValue,
    offset: standardUtcOffset(normalizedValue)
  };
}

export function timeZoneLabel(option: TimeZoneOption): string {
  return `${option.offset} · ${option.value}`;
}
