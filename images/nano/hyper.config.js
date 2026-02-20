import { app, fs, hooks as _hooks, minisearch, lmdb, queue, sqlite } from './deps.js'

import { DIR } from './dir.js'

export default {
  app,
  adapters: [
    {
      port: 'data',
      plugins: [lmdb({ dir: DIR })],
    },
    {
      port: 'cache',
      plugins: [sqlite({ dir: DIR })],
    },
    {
      port: 'storage',
      plugins: [fs({ dir: DIR })],
    },
    {
      port: 'search',
      plugins: [minisearch({ dir: DIR })],
    },
    {
      port: 'queue',
      plugins: [queue({ dir: DIR })],
    },
  ],
}
