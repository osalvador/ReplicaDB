import { readFile } from 'node:fs/promises';
import { expect, test } from '@playwright/test';

test('builds and downloads a compatible SQLite to CSV options file', async ({ page }) => {
  await page.goto('./wizard/index.html');
  const output = page.getByLabel('replicadb.conf');
  await expect(output).toHaveValue(/source\.connect=jdbc:postgresql:\/\/localhost:5432\/orders/);
  const sink = page.getByRole('region', { name: 'Destination', exact: true });
  await sink.getByRole('textbox', { name: /^Connection URL(?: \*)?$/ }).fill('jdbc:postgresql://localhost/reporting');
  await page.getByRole('combobox', { name: 'Source connector', exact: true }).selectOption('sqlite');
  await expect(output).toHaveValue(/sink\.connect=jdbc:postgresql:\/\/localhost\/reporting/);
  await page.getByRole('combobox', { name: 'Destination connector', exact: true }).selectOption('file');
  await expect(page.locator('#mode option[value="complete-atomic"]')).toHaveJSProperty('disabled', true);
  await expect(page.locator('#scope option[value="catalog"]')).toHaveJSProperty('disabled', true);

  const source = page.getByRole('region', { name: 'Source', exact: true });
  await expect(source.getByRole('textbox', { name: /^Connection URL(?: \*)?$/ })).toHaveValue('jdbc:sqlite:/data/orders.db');
  await expect(sink.getByRole('textbox', { name: /^Connection URL(?: \*)?$/ })).toHaveValue('file:///data/orders_copy.csv');
  await expect(output).toHaveValue(/source\.table=orders/);
  await source.getByRole('textbox', { name: /^Connection URL(?: \*)?$/ }).fill('jdbc:sqlite:/tmp/orders.db');
  await source.getByRole('textbox', { name: /^Table(?: \*)?$/ }).fill('orders');
  await sink.getByRole('textbox', { name: /^Connection URL(?: \*)?$/ }).fill('file:///tmp/orders.csv');
  await sink.getByRole('combobox', { name: 'First row is a header', exact: true }).selectOption('true');
  await sink.getByRole('combobox', { name: 'Record separator', exact: true }).selectOption({ label: 'LF' });

  await expect(output).toHaveValue(/jobs=1\n[\s\S]*sink\.connect\.parameter\.format\.firstRecordAsHeader=true/);
  const downloadEvent = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Download .conf' }).click();
  const download = await downloadEvent;
  expect(download.suggestedFilename()).toBe('replicadb.conf');
  expect(await readFile(await download.path(), 'utf8')).toContain('sink.connect.parameter.format.recordSeparator=\\n');

  const width = page.viewportSize()!.width;
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
});

test('switches advanced fields for Azure, a table catalog, and an S3 destination', async ({ page }) => {
  await page.goto('./wizard/index.html');
  await page.getByRole('combobox', { name: 'Source connector', exact: true }).selectOption('sql-server');
  await page.getByRole('combobox', { name: 'Microsoft Entra authentication', exact: true }).selectOption('ActiveDirectoryInteractive');
  await page.getByText('Advanced replication options', { exact: true }).click();
  await expect(page.getByRole('spinbutton', { name: 'Parallel jobs', exact: true })).toBeDisabled();
  await expect(page.getByRole('spinbutton', { name: 'Parallel jobs', exact: true })).toHaveValue('1');
  await expect(page.getByLabel('Password', { exact: true })).toHaveCount(1);

  await page.getByRole('combobox', { name: 'Replication mode', exact: true }).selectOption('incremental');
  await expect(page.getByRole('textbox', { name: /^Watermark column(?: \*)?$/ })).toBeVisible();
  await page.getByRole('combobox', { name: 'Object selection', exact: true }).selectOption('catalog');
  await expect(page.getByRole('textbox', { name: /^Watermark column(?: \*)?$/ })).toHaveCount(0);
  await expect(page.getByRole('textbox', { name: /^Existing staging table or collection(?: \*)?$/ })).toHaveCount(0);
  await page.getByRole('button', { name: 'Add table pair' }).click();
  await expect(page.getByRole('textbox', { name: /^Destination table 2(?: \*)?$/ })).toBeVisible();

  await page.getByRole('combobox', { name: 'Destination connector', exact: true }).selectOption('amazon-s3');
  await expect(page.getByRole('combobox', { name: 'Replication mode', exact: true })).toHaveValue('complete');
  await expect(page.getByRole('combobox', { name: 'Object selection', exact: true })).toHaveValue('table');
  await page.getByRole('combobox', { name: 'One object per row', exact: true }).selectOption('true');
  const sink = page.getByRole('region', { name: 'Destination', exact: true });
  await expect(sink.getByRole('textbox', { name: /^Object key column(?: \*)?$/ })).toHaveValue('id');
  await expect(sink.getByRole('combobox', { name: 'File format', exact: true })).toHaveCount(0);
  await expect(page.getByLabel('replicadb.conf')).toHaveValue(/sink\.connect\.parameter\.secretKey=\$\{AWS_SECRET_ACCESS_KEY\}/);
});

test('opens field-specific help with a documentation link', async ({ page }) => {
  await page.goto('./wizard/index.html');
  await page.getByText('Advanced replication options', { exact: true }).click();
  const help = page.getByRole('button', { name: 'Help for Parallel jobs' });
  const helpPanel = page.locator('#' + await help.getAttribute('aria-controls'));
  await expect(helpPanel).toBeHidden();
  await help.press('Enter');
  await expect(help).toHaveAttribute('aria-expanded', 'true');
  await expect(helpPanel).toContainText('parallel tasks per table');
  await expect(helpPanel.getByRole('link')).toHaveAttribute('href', '../cli/parallelism/');
  await help.click();
  await expect(helpPanel).toBeHidden();

  await page.getByRole('combobox', { name: 'Destination connector', exact: true }).selectOption('kafka');
  const topicHelp = page.getByRole('button', { name: 'Help for Topic' });
  await topicHelp.click();
  const topicPanel = page.locator('#' + await topicHelp.getAttribute('aria-controls'));
  await expect(topicPanel).toContainText('receives the replicated records');
  await expect(topicPanel.getByRole('link')).toHaveAttribute('href', '../connectors/kafka/#sink-connection-parameters');
});
