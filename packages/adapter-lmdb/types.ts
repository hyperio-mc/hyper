/**
 * Configuration options for the LMDB adapter
 */
export interface AdapterConfig {
  /**
   * Directory path where LMDB databases will be stored
   * Defaults to './data' if not provided
   */
  dir?: string

  /**
   * Maximum number of databases that can be opened
   * Defaults to 100
   */
  maxDbs?: number

  /**
   * Map size in bytes for the LMDB environment
   * Defaults to 1GB (1073741824 bytes)
   */
  mapSize?: number

  /**
   * Whether to enable compression
   * Defaults to true for better storage efficiency
   */
  compression?: boolean
}

/**
 * Internal database info stored in the meta database
 */
export interface DbInfo {
  name: string
  createdAt: string
}

/**
 * Result from LMDB operations
 */
export interface LmdbResult<T = unknown> {
  ok: boolean
  id?: string
  doc?: T
  docs?: T[]
  msg?: string
  status?: number
  results?: Array<{ ok: boolean; id: string; msg?: string }>
}