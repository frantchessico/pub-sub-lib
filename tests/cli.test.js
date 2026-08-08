const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const test = require('node:test');

test('CLI prints gateway usage', () => {
  const result = spawnSync(process.execPath, ['dist/cli.js'], {
    cwd: process.cwd(),
    encoding: 'utf8',
  });

  assert.equal(result.status, 0);
  assert.match(result.stdout, /zero-pub-sub gateway/);
});
