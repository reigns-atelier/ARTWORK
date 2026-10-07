if (process.env.MIGRATION_ON_START === 'replace-addiny-with-neon-20261007') {
  await import('./migrate-neon-to-addiny.mjs');
}

await import('../server/index.js');
