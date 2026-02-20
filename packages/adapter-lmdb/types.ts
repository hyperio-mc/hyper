/**
 * Configuration options for the LMDB adapter
 * 
 * These options control how the LMDB environment is initialized.
 * LMDB uses memory-mapped files, so the mapSize determines the maximum
 * virtual address space that can be used (not actual disk usage).
 * 
 * @example
 * ```ts
 * const config: AdapterConfig = {
 *   dir: './data',           // Store data in ./data directory
 *   maxDbs: 50,              // Allow up to 50 named databases
 *   mapSize: 2 * 1024 ** 3,  // 2GB max virtual address space
 *   compression: true        // Enable LZ4 compression
 * }
 * ```
 */
export interface AdapterConfig {
  /**
   * Directory path where LMDB databases will be stored
   * 
   * The adapter will create a file named `hyper-data.mdb` in this directory.
   * For docker deployments, consider using a volume-mounted path.
   * 
   * @default './data'
   */
  dir?: string

  /**
   * Maximum number of databases that can be opened
   * 
   * Each hyper "database" (namespace) creates a separate LMDB database.
   * Set this higher if you need many separate data namespaces.
   * 
   * @default 100
   */
  maxDbs?: number

  /**
   * Map size in bytes for the LMDB environment
   * 
   * This is the maximum virtual address space, not actual disk usage.
   * LMDB will grow the file as needed up to this limit. On 64-bit systems,
   * you can safely set this very large (the OS handles virtual memory).
   * 
   * **Important**: Changing this on an existing database requires special handling.
   * 
   * @default 1073741824 (1GB)
   */
  mapSize?: number

  /**
   * Whether to enable LZ4 compression
   * 
   * Compression reduces disk usage with minimal performance overhead.
   * LZ4 is extremely fast - compression is essentially free for most workloads.
   * 
   * @default true
   */
  compression?: boolean
}

/**
 * Internal database info stored in the meta database
 * 
 * When you create a database with `createDatabase('my-db')`, the meta database
 * stores a mapping from the alias 'my-db' to the actual LMDB database name.
 * This indirection allows for future features like renaming or versioning.
 */
export interface DbInfo {
  /** The actual LMDB database name (e.g., 'db_abc123def') */
  name: string
  /** ISO timestamp when the database was created */
  createdAt: string
}

/**
 * Result from LMDB operations
 * 
 * All Data Port methods return a result in this format. The discriminated
 * union on `ok` allows TypeScript to narrow the result type.
 * 
 * @template T - The type of documents returned (defaults to unknown)
 * 
 * @example
 * ```ts
 * // Success case
 * const result: LmdbResult = { ok: true, id: 'user-123' }
 * 
 * // Error case
 * const result: LmdbResult = { ok: false, msg: 'not found', status: 404 }
 * 
 * // List documents result
 * const listResult: LmdbResult<UserDoc> = { ok: true, docs: [...] }
 * ```
 */
export interface LmdbResult<T = unknown> {
  /** true for success, false for error */
  ok: boolean
  /** The document ID (for create/update/remove operations) */
  id?: string
  /** A single document (for retrieve operations) */
  doc?: T
  /** Array of documents (for list/query operations) */
  docs?: T[]
  /** Error message (when ok is false) */
  msg?: string
  /** HTTP status code (when ok is false) */
  status?: number
  /** Individual results for bulk operations */
  results?: Array<{ ok: boolean; id: string; msg?: string }>
}