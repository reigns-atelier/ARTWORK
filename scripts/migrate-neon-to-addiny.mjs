import pg from 'pg';

const SOURCE_URL = process.env.NEON_RESTORE_DATABASE_URL;
const DESTINATION_URL = process.env.DATABASE_URL;
const CONFIRMATION = 'replace-addiny-with-neon';

if (!SOURCE_URL || !DESTINATION_URL) {
  throw new Error('NEON_RESTORE_DATABASE_URL and DATABASE_URL are required.');
}
if (SOURCE_URL === DESTINATION_URL) {
  throw new Error('Source and destination databases must be different.');
}
if (process.env.MIGRATION_CONFIRM !== CONFIRMATION) {
  throw new Error(`Set MIGRATION_CONFIRM=${CONFIRMATION} to authorize the transactional replacement.`);
}

const quoteIdentifier = value => `"${String(value).replaceAll('"', '""')}"`;
const ssl = enabled => enabled ? { rejectUnauthorized: false } : false;
const source = new pg.Client({ connectionString: SOURCE_URL, ssl: ssl(true) });
const destination = new pg.Client({
  connectionString: DESTINATION_URL,
  ssl: ssl(process.env.DATABASE_SSL !== 'false'),
});

const tableSql = `
  SELECT tablename
  FROM pg_tables
  WHERE schemaname = 'public'
  ORDER BY tablename
`;

async function tableNames(client) {
  return (await client.query(tableSql)).rows.map(row => row.tablename);
}

async function rowCounts(client, tables) {
  const counts = {};
  for (const table of tables) {
    const result = await client.query(`SELECT COUNT(*)::bigint AS count FROM public.${quoteIdentifier(table)}`);
    counts[table] = Number(result.rows[0].count);
  }
  return counts;
}

async function copyTable(table) {
  const columns = (await source.query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1
        AND is_generated = 'NEVER'
      ORDER BY ordinal_position`,
    [table],
  )).rows.map(row => row.column_name);

  if (!columns.length) throw new Error(`No writable columns found for ${table}.`);
  const rows = (await source.query(
    `SELECT ${columns.map(quoteIdentifier).join(', ')} FROM public.${quoteIdentifier(table)}`,
  )).rows;
  const chunkSize = 200;
  for (let offset = 0; offset < rows.length; offset += chunkSize) {
    const chunk = rows.slice(offset, offset + chunkSize);
    const values = [];
    const tuples = chunk.map(row => {
      const placeholders = columns.map(column => {
        values.push(row[column]);
        return `$${values.length}`;
      });
      return `(${placeholders.join(', ')})`;
    });
    await destination.query(
      `INSERT INTO public.${quoteIdentifier(table)} (${columns.map(quoteIdentifier).join(', ')}) VALUES ${tuples.join(', ')}`,
      values,
    );
  }
}

async function resetSequences() {
  const sequences = (await destination.query(`
    SELECT table_name, column_name,
           pg_get_serial_sequence(format('%I.%I', table_schema, table_name), column_name) AS sequence_name
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND (column_default LIKE 'nextval(%' OR is_identity = 'YES')
  `)).rows.filter(row => row.sequence_name);

  for (const { table_name: table, column_name: column, sequence_name: sequence } of sequences) {
    const maximum = await destination.query(
      `SELECT MAX(${quoteIdentifier(column)}) AS value FROM public.${quoteIdentifier(table)}`,
    );
    const value = maximum.rows[0].value;
    await destination.query('SELECT setval($1::regclass, $2, $3)', [sequence, value ?? 1, value != null]);
  }
}

try {
  await Promise.all([source.connect(), destination.connect()]);
  const [sourceTables, destinationTables] = await Promise.all([
    tableNames(source),
    tableNames(destination),
  ]);
  const missing = sourceTables.filter(table => !destinationTables.includes(table));
  const extra = destinationTables.filter(table => !sourceTables.includes(table));
  if (sourceTables.length !== 43 || missing.length || extra.length) {
    throw new Error(JSON.stringify({
      message: 'Schema safety check failed.',
      expectedSourceTables: 43,
      sourceTables: sourceTables.length,
      destinationTables: destinationTables.length,
      missing,
      extra,
    }));
  }

  const sourceCounts = await rowCounts(source, sourceTables);
  await destination.query('BEGIN');
  const lock = await destination.query(
    "SELECT pg_try_advisory_xact_lock(hashtext('reigns_atelier_state_write')) AS locked",
  );
  if (!lock.rows[0]?.locked) {
    throw new Error('Another database write or migration is still running. Retry after it finishes.');
  }
  await destination.query(
    `TRUNCATE ${sourceTables.map(table => `public.${quoteIdentifier(table)}`).join(', ')} RESTART IDENTITY CASCADE`,
  );
  for (const table of sourceTables) await copyTable(table);
  await resetSequences();

  const destinationCounts = await rowCounts(destination, sourceTables);
  const mismatches = sourceTables.filter(table => sourceCounts[table] !== destinationCounts[table]);
  if (mismatches.length) {
    throw new Error(`Row-count verification failed for: ${mismatches.join(', ')}`);
  }
  await destination.query('COMMIT');
  process.stdout.write(`${JSON.stringify({
    success: true,
    tables: sourceTables.length,
    sourceCounts,
    destinationCounts,
  }, null, 2)}\n`);
} catch (error) {
  await destination.query('ROLLBACK').catch(() => {});
  throw error;
} finally {
  await Promise.allSettled([source.end(), destination.end()]);
}
