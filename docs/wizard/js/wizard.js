import { advancedFields, compatibility, endpointFields, exampleTables, generateConfiguration, genericJdbc } from './options.js';
import { fieldHelp } from './help.js';

const byId = id => document.getElementById(id);
const form = byId('wizard');
const output = byId('output');
const feedback = byId('feedback');
const pairs = [];
const values = { mode: 'complete', scope: 'table' };
const sides = ['source', 'sink'];
const formatControllers = new Set(['mode', 'scope', 'source.file.format', 'sink.file.format',
  'source.auth.mode', 'sink.auth.mode', 'sink.connect.parameter.row.isObject']);
let nextFieldId = 0;

function addHelp(wrapper, label, key, connector) {
  const [description, route] = fieldHelp(key, connector);
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'help-toggle';
  button.textContent = '?';
  button.setAttribute('aria-label', `Help for ${label.textContent.replace(/ \*$/, '')}`);
  button.setAttribute('aria-expanded', 'false');

  const panel = document.createElement('div');
  panel.id = `${label.htmlFor}-help`;
  panel.className = 'field-help';
  panel.hidden = true;
  button.setAttribute('aria-controls', panel.id);
  const link = document.createElement('a');
  link.href = `..${route}`;
  link.target = '_blank';
  link.rel = 'noopener';
  link.textContent = 'Read the documentation';
  panel.append(description, ' ', link);
  label.after(button);
  wrapper.append(panel);

  button.addEventListener('click', () => {
    panel.hidden = !panel.hidden;
    button.setAttribute('aria-expanded', String(!panel.hidden));
  });
}

function control(definition, value, connector, helpKey = definition.key) {
  const { key, label, choices, initial, min, length, multiline, required, placeholder, hint, locked } = definition;
  const wrapper = document.createElement('div');
  wrapper.className = 'control';
  const heading = document.createElement('div');
  heading.className = 'field-heading';
  const labelNode = document.createElement('label');
  labelNode.textContent = label + (required ? ' *' : '');
  const input = document.createElement(choices ? 'select' : multiline ? 'textarea' : 'input');
  input.id = `config-field-${++nextFieldId}`;
  labelNode.htmlFor = input.id;
  input.name = key;
  input.required = Boolean(required);
  input.spellcheck = false;
  input.autocomplete = 'off';
  if (choices) {
    input.add(new Option(definition.blank ?? 'Use default', ''));
    for (const choice of choices) input.add(new Option(definition.labels?.[choice] ?? choice, choice));
  } else if (multiline) {
    input.rows = 3;
  } else if (min !== undefined) {
    input.type = 'number';
    input.min = min;
    input.step = 1;
  }
  if (placeholder) input.placeholder = placeholder;
  if (length) input.maxLength = length;
  if (locked) input.disabled = true;
  input.value = (locked ? initial : value ?? initial) ?? '';
  heading.append(labelNode);
  wrapper.append(heading, input);
  if (hint) {
    const hintNode = document.createElement('small');
    hintNode.textContent = hint;
    hintNode.id = `${input.id}-hint`;
    input.setAttribute('aria-describedby', hintNode.id);
    wrapper.append(hintNode);
  }
  addHelp(wrapper, labelNode, helpKey, connector);
  return wrapper;
}

function renderPairs(updateOutput) {
  const container = byId('pairs');
  container.replaceChildren();
  pairs.forEach((pair, index) => {
    const row = document.createElement('div');
    row.className = 'pair';
    for (const side of sides) {
      const label = side === 'source' ? 'Source' : 'Destination';
      const wrapper = control({ key: '', label: `${label} table ${index + 1}`, required: true }, pair[side], undefined, `pair.${side}`);
      wrapper.querySelector('input').addEventListener('input', event => {
        pair[side] = event.target.value;
        updateOutput();
      });
      row.append(wrapper);
    }
    const remove = document.createElement('button');
    remove.type = 'button';
    remove.textContent = 'Remove';
    remove.setAttribute('aria-label', `Remove table pair ${index + 1}`);
    remove.addEventListener('click', () => {
      pairs.splice(index, 1);
      renderPairs(updateOutput);
      byId('add-pair').focus();
      updateOutput();
    });
    row.append(remove);
    container.append(row);
  });
}

function renderEndpoint(side, connector, source, sink) {
  const target = byId(`${side}-fields`);
  const extrasOpen = target.querySelector('details')?.open;
  const guide = document.createElement('p');
  guide.className = 'endpoint-guide';
  const link = document.createElement('a');
  link.href = `..${connector.page}`;
  link.textContent = `${connector.displayName} setup`;
  guide.append(link);
  if (connector.caveats.length) guide.append(` · ${connector.caveats.join(' ')}`);

  const sections = document.createElement('div');
  sections.className = 'endpoint-options';
  const inputs = document.createElement('div');
  const definitions = endpointFields(side, connector, source, sink, values);
  inputs.append(...definitions.map(definition => control(definition, values[definition.key], connector)));

  const extras = document.createElement('details');
  extras.open = Boolean(extrasOpen);
  const summary = document.createElement('summary');
  summary.textContent = 'Additional connection parameters';
  extras.append(summary, control({
    key: `${side}.extra`, label: 'name=value, one per line', multiline: true,
    placeholder: 'connectTimeout=10', hint: 'Names are prefixed with ' + side + '.connect.parameter.'
  }, values[`${side}.extra`], connector));
  sections.append(inputs, extras);
  target.replaceChildren(guide, sections);
}

function start(connectors) {
  const catalog = [...connectors].sort((a, b) => a.displayName.localeCompare(b.displayName)).concat(genericJdbc);
  const byConnectorId = new Map(catalog.map(connector => [connector.id, connector]));

  for (const side of sides) {
    const selectable = catalog.filter(connector => connector.roles.includes(side));
    const select = byId(`${side}-type`);
    for (const connector of selectable) select.add(new Option(connector.displayName, connector.id));
    select.value = 'postgresql';
  }
  for (const label of form.querySelectorAll('[data-help]')) {
    addHelp(label.closest('.control'), label, label.dataset.help);
  }
  for (const mode of ['complete', 'complete-atomic', 'incremental']) byId('mode').add(new Option(mode, mode));

  const selection = () => sides.map(side => byConnectorId.get(byId(`${side}-type`).value));
  const updateOutput = () => {
    const [source, sink] = selection();
    const result = generateConfiguration(source, sink, values, pairs);
    output.value = result.text;
    byId('errors').replaceChildren(...result.errors.map(error => {
      const item = document.createElement('li');
      item.textContent = error;
      return item;
    }));
    byId('download').disabled = !result.text;
    byId('copy').disabled = !result.text;
    feedback.textContent = '';
  };

  function updateFields(focusName) {
    const [source, sink] = selection();
    const rules = compatibility(source, sink, values);
    values.mode = rules.mode;
    byId('mode').value = values.mode;
    for (const option of byId('mode').options) option.disabled = !rules.allowedModes.includes(option.value);
    const scope = byId('scope');
    scope.options[1].disabled = !rules.sourceQuery;
    scope.options[2].disabled = !rules.catalog;
    if (scope.selectedOptions[0].disabled) scope.value = 'table';
    values.scope = scope.value;
    const jobs = rules.singleJob ? ' This configuration uses one job.' : '';
    byId('compatibility').textContent = `Supported modes: ${rules.allowedModes.join(', ')}.${jobs}`;

    for (const [side, connector] of [['source', source], ['sink', sink]]) renderEndpoint(side, connector, source, sink);
    byId('catalog').hidden = values.scope !== 'catalog';
    if (!byId('catalog').hidden && !pairs.length) {
      pairs.push({ ...exampleTables });
      renderPairs(updateOutput);
    }
    byId('advanced-fields').replaceChildren(...advancedFields(source, sink, values)
      .map(definition => control(definition, values[definition.key])));
    if (focusName && document.activeElement === document.body) form.elements.namedItem(focusName)?.focus();
    updateOutput();
  }

  for (const side of sides) {
    byId(`${side}-type`).addEventListener('change', () => {
      for (const key of Object.keys(values)) {
        if (key.startsWith(`${side}.`) || (side === 'source' && key.startsWith('incremental.'))) delete values[key];
      }
      updateFields();
    });
  }
  form.addEventListener('input', event => {
    const { name, value } = event.target;
    if (!name) return;
    values[name] = value;
    if (formatControllers.has(name)) updateFields(name);
    else updateOutput();
  });
  form.addEventListener('submit', event => event.preventDefault());
  byId('add-pair').addEventListener('click', () => {
    pairs.push({ source: '', sink: '' });
    renderPairs(updateOutput);
    byId('pairs').lastElementChild.querySelector('input').focus();
    updateOutput();
  });
  byId('copy').addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(output.value);
      feedback.textContent = 'Options copied.';
    } catch {
      output.focus();
      output.select();
      feedback.textContent = 'Options selected. Use your browser’s copy shortcut.';
    }
  });
  byId('download').addEventListener('click', () => {
    const link = document.createElement('a');
    link.href = URL.createObjectURL(new Blob([output.value], { type: 'text/plain;charset=utf-8' }));
    link.download = 'replicadb.conf';
    link.click();
    setTimeout(() => URL.revokeObjectURL(link.href), 1000);
  });
  updateFields();
  form.hidden = false;
}

try {
  const response = await fetch(new URL('../connector-capabilities.json', import.meta.url));
  if (!response.ok) throw new Error('Connector catalog unavailable');
  start(await response.json());
} catch (error) {
  console.error(error);
  const message = byId('load-error');
  message.textContent = 'The connector catalog could not load. Reload the page to try again.';
  message.hidden = false;
}
