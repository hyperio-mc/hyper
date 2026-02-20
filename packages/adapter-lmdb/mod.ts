import { adapter } from './adapter.ts'
import { join, exists, ensureDir, open, type RootDatabase, type Database } from './deps.ts'
import type { AdapterConfig, DbInfo } from './types.ts'
import PORT_NAME from './port_name.ts'

/**
 * LMDB adapter for hyper Data Port
 * 
 * This adapter provides a high-performance, ACID-compliant data storage
 * solution using LMDB (Lightning Memory-Mapped Database).
 * 
 * Features:
 * - Ultra-fast key-value storage
 * - ACID transactions
 * - Efficient memory usage with memory-mapped files
 * - Optional compression
 * - Full Data Port interface implementation
 * 
 * @param config - Configuration options for the adapter
 * @returns A hyper adapter plugin
 * 
 * @example
 * ```ts
 * import lmdb from 'hyper-adapter-lmdb'
 * 
 * export default {
 *   app,
 *   adapters: [
 *     {
 *       port: 'data',
 *       plugins: [lmdb({ dir: './data' })],
 *     },
 *   ],
 * }
 * ```
 */
export default (config: AdapterConfig = {}) => ({
  id: 'lmdb',
  port: PORT_NAME,
  
  /**
   * Load and initialize the LMDB environment
   * This is called once during hyper startup
   */
  load: async () => {
    const dir = config.dir || './data'
    const maxDbs = config.maxDbs || 100
    const mapSize = config.mapSize || 1073741824 // 1GB default
    
    // Ensure directory exists
    await ensureDir(dir)
    
    const dbPath = join(dir, 'hyper-data.mdb')
    
    // Open the root LMDB database
    const rootDb = open({
      path: dbPath,
      maxDbs,
      mapSize,
      compression: config.compression ?? true,
    }) as RootDatabase
    
    // Open the meta database for storing database metadata
    const meta = rootDb.openDB<DbInfo>('meta', {
      encoding: 'msgpack',
    }) as Database<DbInfo>
    
    // Cache for opened databases
    const dbs = new Map<string, Database<Record<string, unknown>>>()
    
    // Register cleanup handler
    addEventListener('unload', () => {
      if (rootDb) {
        try {
          rootDb.close()
        } catch (e) {
          console.error('Error closing LMDB:', e)
        }
      }
    })
    
    return {
      rootDb,
      meta,
      dbs,
    }
  },
  
  /**
   * Link the adapter to the loaded environment
   * Returns the adapter implementation
   */
  link: (env: {
    rootDb: RootDatabase
    meta: Database<DbInfo>
    dbs: Map<string, Database<Record<string, unknown>>>
  }) => () => adapter(env),
})

// Re-export types for consumers
export type { AdapterConfig, DbInfo, LmdbResult } from './types.ts'