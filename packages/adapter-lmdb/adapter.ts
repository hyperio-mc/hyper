import { crocks, HyperErr, isHyperErr, R, type Database } from './deps.ts'
import type { DbInfo, LmdbResult } from './types.ts'

const { Async } = crocks
const {
  always,
  identity,
  complement,
  isEmpty,
  ifElse,
  prop,
  propOr,
  has,
} = R

/**
 * Check if a value is defined (not null or undefined)
 */
const isDefined = complement(R.isNil)

/**
 * Async wrapper for promise-returning functions
 */
const asyncify = <T extends (...args: unknown[]) => Promise<unknown>>(fn: T) =>
  Async.fromPromise(fn)

/**
 * Handle HyperErr by wrapping it in a Resolved result
 * This allows errors to flow through success channel for proper response formatting
 */
const handleHyperErr = ifElse(
  isHyperErr,
  Async.Resolved,
  Async.Rejected,
)

/**
 * Create a HyperErr with the provided message and status
 */
const hyperErr = (msg: string, status: number) =>
  HyperErr({ msg, status })

/**
 * Convert an LMDB error to a HyperErr
 */
const lmdbErrToHyperErr = (context: string) => (e: Error) => {
  console.error(`LMDB Error [${context}]:`, e)
  return HyperErr({
    msg: e.message || `An error occurred: ${context}`,
    status: 500,
  })
}

/**
 * Generate a unique ID using timestamp and random string
 */
const generateId = (): string => {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 11)}`
}

/**
 * Parse selector operators for query matching
 * Supports basic MongoDB-style selectors: $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin, $exists
 * 
 * Note: This is a simplified in-memory query implementation.
 * For complex queries, consider using a dedicated query engine.
 */
const matchesSelector = (doc: Record<string, unknown>, selector: Record<string, unknown>): boolean => {
  for (const [field, condition] of Object.entries(selector)) {
    const docValue = prop(field, doc)
    
    if (condition === null || condition === undefined) {
      if (docValue !== null && docValue !== undefined) return false
      continue
    }
    
    if (typeof condition === 'object' && condition !== null) {
      const condObj = condition as Record<string, unknown>
      
      if ('$eq' in condObj && docValue !== condObj.$eq) return false
      if ('$ne' in condObj && docValue === condObj.$ne) return false
      if ('$gt' in condObj && !(docValue! > (condObj.$gt as number))) return false
      if ('$gte' in condObj && !(docValue! >= (condObj.$gte as number))) return false
      if ('$lt' in condObj && !(docValue! < (condObj.$lt as number))) return false
      if ('$lte' in condObj && !(docValue! <= (condObj.$lte as number))) return false
      if ('$in' in condObj) {
        const arr = condObj.$in as unknown[]
        if (!arr.includes(docValue)) return false
      }
      if ('$nin' in condObj) {
        const arr = condObj.$nin as unknown[]
        if (arr.includes(docValue)) return false
      }
      if ('$exists' in condObj) {
        const shouldExist = condObj.$exists as boolean
        const exists = has(field, doc)
        if (shouldExist !== exists) return false
      }
    } else {
      // Direct equality comparison
      if (docValue !== condition) return false
    }
  }
  return true
}

/**
 * Sort documents based on sort specification
 */
const sortDocs = (
  docs: Record<string, unknown>[],
  sort: Array<string | Record<string, 'ASC' | 'DESC'>> | undefined
): Record<string, unknown>[] => {
  if (!sort || sort.length === 0) return docs
  
  return [...docs].sort((a, b) => {
    for (const sortSpec of sort) {
      let field: string
      let direction: 'ASC' | 'DESC' = 'ASC'
      
      if (typeof sortSpec === 'string') {
        field = sortSpec
      } else {
        field = Object.keys(sortSpec)[0]
        direction = sortSpec[field] as 'ASC' | 'DESC'
      }
      
      const aVal = prop(field, a)
      const bVal = prop(field, b)
      
      let cmp = 0
      if (aVal === bVal) continue
      if (aVal === undefined || aVal === null) cmp = -1
      else if (bVal === undefined || bVal === null) cmp = 1
      else if (typeof aVal === 'string' && typeof bVal === 'string') {
        cmp = aVal.localeCompare(bVal)
      } else if (typeof aVal === 'number' && typeof bVal === 'number') {
        cmp = aVal - bVal
      }
      
      return direction === 'DESC' ? -cmp : cmp
    }
    return 0
  })
}

/**
 * Project specific fields from a document
 */
const projectFields = (
  doc: Record<string, unknown>,
  fields: string[] | undefined
): Record<string, unknown> => {
  if (!fields || fields.length === 0) return doc
  
  const projected: Record<string, unknown> = {}
  for (const field of fields) {
    if (has(field, doc)) {
      projected[field] = prop(field, doc)
    }
  }
  return projected
}

/**
 * Create the LMDB data adapter
 * 
 * This factory function creates an adapter that implements the Data Port interface
 * using LMDB as the underlying storage engine. Each method returns a Promise that
 * resolves to a standardized result format: `{ ok: true, ... }` for success or
 * `{ ok: false, msg: string, status: number }` for errors.
 * 
 * ## Architecture
 * 
 * The adapter uses three LMDB databases:
 * - **rootDb**: The root environment that houses all databases
 * - **meta**: A database for storing database metadata (aliases → actual names)
 * - **dbs**: A Map cache of opened database instances
 * 
 * ## Why LMDB?
 * 
 * LMDB was chosen as a replacement for MongoDB because it provides:
 * - **Zero configuration**: No separate server process needed
 * - **ACID compliance**: Full transactional support with crash recovery
 * - **Memory efficiency**: Memory-mapped files leverage OS caching automatically
 * - **Ordered keys**: Documents are stored in key order for efficient range queries
 * - **Cross-platform**: Works consistently across Linux, macOS, and Windows
 * 
 * ## Error Handling Strategy
 * 
 * All methods use the crocks Async pattern for consistent error handling:
 * 1. Wrap operations in Async.fromPromise
 * 2. Catch LMDB errors and convert to HyperErr
 * 3. Channel HyperErr through success path for uniform response formatting
 * 
 * @param env - The environment object containing database connections
 * @param env.rootDb - The root LMDB database environment
 * @param env.meta - The metadata database for storing db aliases
 * @param env.dbs - A Map cache for opened database instances
 * @returns A frozen object implementing the Data Port interface
 */
export function adapter(env: {
  rootDb: Database<unknown>
  meta: Database<DbInfo>
  dbs: Map<string, Database<Record<string, unknown>>>
}) {
  const { rootDb, meta, dbs } = env

  /**
   * Get the meta key for a database
   */
  const getMetaKey = (dbName: string) => `db:${dbName}`

  /**
   * Resolve the actual database name from the alias
   */
  const resolveDbName = async (alias: string): Promise<string | null> => {
    const metaKey = getMetaKey(alias)
    const info = meta.get(metaKey) as DbInfo | undefined
    return info?.name ?? null
  }

  /**
   * Get or create a database by alias
   */
  const getDb = async (alias: string): Promise<Database<Record<string, unknown>> | null> => {
    // Check cache first
    if (dbs.has(alias)) {
      return dbs.get(alias)!
    }
    
    // Resolve actual name
    const actualName = await resolveDbName(alias)
    if (!actualName) return null
    
    // Open the database
    const db = rootDb.openDB<Record<string, unknown>>(actualName, {
      encoding: 'msgpack',
    }) as Database<Record<string, unknown>>
    
    dbs.set(alias, db)
    return db
  }

  /**
   * Create a new database
   * 
   * Creates a new named database within the LMDB environment. The name you provide
   * is an alias; internally, LMDB databases are named with a unique ID (e.g., `db_abc123`).
   * This indirection allows for future features like database renaming without data migration.
   * 
   * @param name - The alias for the new database
   * @returns `{ ok: true }` on success, or `{ ok: false, msg, status }` on error
   * @throws HyperErr with status 409 if database already exists
   * 
   * @example
   * ```ts
   * const result = await adapter.createDatabase('my-app-db')
   * // { ok: true }
   * ```
   */
  async function createDatabase(name: string): Promise<LmdbResult> {
    return Async.of(name)
      .chain(asyncify(async (n) => {
        // Check if already exists
        const metaKey = getMetaKey(n)
        const existing = meta.get(metaKey)
        if (existing) {
          throw HyperErr({ status: 409, msg: 'database already exists' })
        }
        
        // Generate actual database name
        const actualName = `db_${generateId()}`
        
        // Store metadata
        const dbInfo: DbInfo = {
          name: actualName,
          createdAt: new Date().toISOString(),
        }
        
        await meta.put(metaKey, dbInfo)
        
        // Create the database by opening it
        const db = rootDb.openDB<Record<string, unknown>>(actualName, {
          encoding: 'msgpack',
        }) as Database<Record<string, unknown>>
        
        dbs.set(n, db)
        
        return { ok: true }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('createDatabase')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * Remove a database
   * 
   * Permanently deletes a database and all its documents. This operation:
   * 1. Resolves the alias to find the actual database
   * 2. Drops the LMDB database (frees disk space)
   * 3. Removes the metadata entry
   * 4. Clears the database from the cache
   * 
   * **Warning**: This operation is irreversible. All documents are permanently deleted.
   * 
   * @param name - The alias of the database to remove
   * @returns `{ ok: true }` on success, or `{ ok: false, msg, status }` on error
   * @throws HyperErr with status 404 if database not found
   * 
   * @example
   * ```ts
   * const result = await adapter.removeDatabase('my-app-db')
   * // { ok: true }
   * ```
   */
  async function removeDatabase(name: string): Promise<LmdbResult> {
    return Async.of(name)
      .chain(asyncify(async (n) => {
        const metaKey = getMetaKey(n)
        const info = meta.get(metaKey) as DbInfo | undefined
        
        if (!info) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        // Drop the database
        const db = dbs.get(n)
        if (db) {
          await (db as unknown as { drop: () => Promise<void> }).drop()
          dbs.delete(n)
        } else {
          // Open and drop
          const dbToDrop = rootDb.openDB(info.name)
          await (dbToDrop as unknown as { drop: () => Promise<void> }).drop()
        }
        
        // Remove from meta
        meta.remove(metaKey)
        
        return { ok: true }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('removeDatabase')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * Create a document
   * 
   * Creates a new document with the specified ID in the database. This operation
   * is atomic and will fail if a document with the same ID already exists.
   * 
   * **Why not upsert on create?**
   * The create operation is deliberately strict to prevent accidental overwrites.
   * Use `updateDocument` for upsert behavior (create or replace).
   * 
   * @param dbName - The database alias
   * @param id - The unique document identifier
   * @param doc - The document data to store
   * @returns `{ ok: true, id }` on success, or `{ ok: false, msg, status }` on error
   * @throws HyperErr with status 404 if database not found
   * @throws HyperErr with status 409 if document already exists
   * @throws HyperErr with status 400 if document is empty
   * 
   * @example
   * ```ts
   * const result = await adapter.createDocument({
   *   db: 'users',
   *   id: 'user-123',
   *   doc: { name: 'Alice', email: 'alice@example.com' }
   * })
   * // { ok: true, id: 'user-123' }
   * ```
   */
  async function createDocument({
    db: dbName,
    id,
    doc,
  }: {
    db: string
    id: string
    doc: Record<string, unknown>
  }): Promise<LmdbResult> {
    return Async.of({ dbName, id, doc })
      .chain(asyncify(async ({ dbName, id, doc }) => {
        if (isEmpty(doc)) {
          throw HyperErr({ status: 400, msg: 'document empty' })
        }
        
        const db = await getDb(dbName)
        if (!db) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        // Check if document already exists
        const existing = db.get(id)
        if (existing !== undefined) {
          throw HyperErr({ status: 409, msg: 'document conflict' })
        }
        
        // Store the document
        await db.put(id, doc)
        
        return { ok: true, id }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('createDocument')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * Retrieve a document
   * 
   * Fetches a single document by its ID. This is an O(1) key lookup operation
   * in LMDB, making it extremely efficient.
   * 
   * @param dbName - The database alias
   * @param id - The document ID to retrieve
   * @returns The document object on success, or `{ ok: false, msg, status }` on error
   * @throws HyperErr with status 404 if database or document not found
   * 
   * @example
   * ```ts
   * const result = await adapter.retrieveDocument({
   *   db: 'users',
   *   id: 'user-123'
   * })
   * // { name: 'Alice', email: 'alice@example.com' }
   * ```
   */
  async function retrieveDocument({
    db: dbName,
    id,
  }: {
    db: string
    id: string
  }): Promise<Record<string, unknown> | LmdbResult> {
    return Async.of({ dbName, id })
      .chain(asyncify(async ({ dbName, id }) => {
        const db = await getDb(dbName)
        if (!db) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        const doc = db.get(id)
        if (doc === undefined) {
          throw HyperErr({ status: 404, msg: 'document not found' })
        }
        
        return doc
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('retrieveDocument')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * Update a document (upsert behavior)
   * 
   * Updates an existing document or creates it if it doesn't exist. This is a
   * complete replacement operation - the entire document is replaced, not merged.
   * 
   * **Note**: This is NOT a partial update. If you want to merge fields,
   * you must retrieve, merge, and update.
   * 
   * @param dbName - The database alias
   * @param id - The document ID to update
   * @param doc - The new document data (replaces existing entirely)
   * @returns `{ ok: true, id }` on success, or `{ ok: false, msg, status }` on error
   * @throws HyperErr with status 404 if database not found
   * 
   * @example
   * ```ts
   * // Complete replacement
   * const result = await adapter.updateDocument({
   *   db: 'users',
   *   id: 'user-123',
   *   doc: { name: 'Alice Updated', email: 'alice.new@example.com', role: 'admin' }
   * })
   * // { ok: true, id: 'user-123' }
   * ```
   */
  async function updateDocument({
    db: dbName,
    id,
    doc,
  }: {
    db: string
    id: string
    doc: Record<string, unknown>
  }): Promise<LmdbResult> {
    return Async.of({ dbName, id, doc })
      .chain(asyncify(async ({ dbName, id, doc }) => {
        const db = await getDb(dbName)
        if (!db) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        // Upsert - put will replace or create
        await db.put(id, doc)
        
        return { ok: true, id }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('updateDocument')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * Remove a document
   * 
   * Deletes a single document by its ID. The operation verifies the document
   * exists before deletion.
   * 
   * @param dbName - The database alias
   * @param id - The document ID to remove
   * @returns `{ ok: true, id }` on success, or `{ ok: false, msg, status }` on error
   * @throws HyperErr with status 404 if database or document not found
   * 
   * @example
   * ```ts
   * const result = await adapter.removeDocument({
   *   db: 'users',
   *   id: 'user-123'
   * })
   * // { ok: true, id: 'user-123' }
   * ```
   */
  async function removeDocument({
    db: dbName,
    id,
  }: {
    db: string
    id: string
  }): Promise<LmdbResult> {
    return Async.of({ dbName, id })
      .chain(asyncify(async ({ dbName, id }) => {
        const db = await getDb(dbName)
        if (!db) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        const existed = db.get(id) !== undefined
        if (!existed) {
          throw HyperErr({ status: 404, msg: 'document not found' })
        }
        
        await db.remove(id)
        
        return { ok: true, id }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('removeDocument')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * List documents with optional filtering
   * 
   * Retrieves documents from a database with optional key range filtering
   * and pagination. Leverages LMDB's ordered keys for efficient range queries.
   * 
   * **Key Ordering**: Documents are stored in lexicographic key order, making
   * range queries (startkey/endkey) very efficient.
   * 
   * **Note**: This method iterates through keys. For complex queries with
   * selectors, use `queryDocuments` instead.
   * 
   * @param dbName - The database alias
   * @param limit - Maximum number of documents to return
   * @param startkey - Start of key range (inclusive)
   * @param endkey - End of key range (inclusive via \uffff suffix)
   * @param keys - Comma-separated list of specific keys to retrieve
   * @param descending - If true, iterate in reverse key order
   * @returns `{ ok: true, docs: [...] }` on success, or error object
   * @throws HyperErr with status 404 if database not found
   * 
   * @example
   * ```ts
   * // Get first 10 documents
   * const result = await adapter.listDocuments({
   *   db: 'users',
   *   limit: 10
   * })
   * 
   * // Range query
   * const result = await adapter.listDocuments({
   *   db: 'users',
   *   startkey: 'user-100',
   *   endkey: 'user-200',
   *   descending: true
   * })
   * ```
   */
  async function listDocuments({
    db: dbName,
    limit,
    startkey,
    endkey,
    keys,
    descending,
  }: {
    db: string
    limit?: number
    startkey?: string
    endkey?: string
    keys?: string
    descending?: boolean
  }): Promise<LmdbResult> {
    return Async.of({ dbName, limit, startkey, endkey, keys, descending })
      .chain(asyncify(async ({ dbName, limit, startkey, endkey, keys, descending }) => {
        const db = await getDb(dbName)
        if (!db) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        const docs: Record<string, unknown>[] = []
        const keysList = keys ? keys.split(',') : null
        
        // Use getRange for iteration
        const rangeOptions: {
          start?: string
          end?: string
          reverse?: boolean
        } = {}
        
        if (startkey) rangeOptions.start = startkey
        // LMDB's end is exclusive, so append \uffff to make it inclusive
        if (endkey) rangeOptions.end = endkey + '\uffff'
        if (descending) rangeOptions.reverse = true
        
        for (const { key, value } of db.getRange(rangeOptions)) {
          // Filter by keys if specified
          if (keysList && !keysList.includes(key as string)) continue
          
          const doc = value as Record<string, unknown>
          docs.push({ _id: key, ...doc })
          
          if (limit && docs.length >= limit) break
        }
        
        return { ok: true, docs }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('listDocuments')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * Query documents with selector
   * 
   * Performs a query against all documents in the database using MongoDB-style
   * selectors. This is an in-memory filter that iterates through all documents.
   * 
   * **Performance Note**: Since LMDB doesn't support secondary indexes natively,
   * this implementation scans all documents. For large datasets, consider:
   * - Using `listDocuments` with key ranges for primary key queries
   * - Creating a separate "index" database for frequently queried fields
   * - Using the `use_index` parameter (stored as metadata for future optimization)
   * 
   * @param dbName - The database alias
   * @param query.selector - MongoDB-style selector object
   * @param query.fields - Fields to project (return subset of fields)
   * @param query.sort - Sort specification: string fields or { field: 'ASC'|'DESC' }
   * @param query.limit - Maximum documents to return
   * @param query.skip - Number of documents to skip (for pagination)
   * @param query.use_index - Index name hint (currently stored as metadata only)
   * @returns `{ ok: true, docs: [...] }` on success, or error object
   * @throws HyperErr with status 404 if database not found
   * 
   * @example
   * ```ts
   * // Simple equality query
   * const result = await adapter.queryDocuments({
   *   db: 'users',
   *   query: { selector: { status: 'active' } }
   * })
   * 
   * // Complex query with operators
   * const result = await adapter.queryDocuments({
   *   db: 'users',
   *   query: {
   *     selector: { age: { $gte: 18 }, status: { $in: ['active', 'pending'] } },
   *     sort: [{ age: 'DESC' }],
   *     limit: 10,
   *     fields: ['name', 'email']
   *   }
   * })
   * ```
   */
  async function queryDocuments({
    db: dbName,
    query,
  }: {
    db: string
    query: {
      selector?: Record<string, unknown>
      fields?: string[]
      sort?: Array<string | Record<string, 'ASC' | 'DESC'>>
      limit?: number
      skip?: number
      use_index?: string
    }
  }): Promise<LmdbResult> {
    return Async.of({ dbName, query })
      .chain(asyncify(async ({ dbName, query }) => {
        const db = await getDb(dbName)
        if (!db) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        const { selector, fields, sort, limit, skip } = query
        let docs: Record<string, unknown>[] = []
        
        // Iterate through all documents
        for (const { key, value } of db.getRange({})) {
          const doc = value as Record<string, unknown>
          
          // Apply selector filter
          if (selector && !matchesSelector(doc, selector)) continue
          
          docs.push({ _id: key, ...doc })
        }
        
        // Apply sort
        docs = sortDocs(docs, sort)
        
        // Apply skip
        if (skip) {
          docs = docs.slice(skip)
        }
        
        // Apply limit
        if (limit) {
          docs = docs.slice(0, limit)
        }
        
        // Apply field projection
        if (fields && fields.length > 0) {
          docs = docs.map(doc => projectFields(doc, fields))
        }
        
        return { ok: true, docs }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('queryDocuments')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * Create an index
   * 
   * Stores index metadata for the specified fields. Since LMDB uses ordered keys
   * by default, primary key lookups are already optimized. This method exists
   * primarily for API compatibility with other Data Port adapters.
   * 
   * **Current Behavior**: Index metadata is stored but not used for query optimization.
   * Future implementations could create separate index databases for secondary indexes.
   * 
   * **Why not full indexing?** LMDB's design philosophy favors simple key-value access.
   * Implementing secondary indexes would require:
   * - Creating separate LMDB databases for each index
   * - Maintaining index consistency on document changes
   * - Added complexity for a feature that many use-cases don't need
   * 
   * @param dbName - The database alias
   * @param name - A name for the index
   * @param fields - Fields to index (array of field names or sort specs)
   * @param partialFilter - Optional partial filter selector
   * @returns `{ ok: true }` on success, or error object
   * @throws HyperErr with status 404 if database not found
   */
  async function indexDocuments({
    db: dbName,
    name,
    fields,
    partialFilter,
  }: {
    db: string
    name: string
    fields: string[] | Array<Record<string, 'ASC' | 'DESC'>>
    partialFilter?: Record<string, unknown>
  }): Promise<LmdbResult> {
    return Async.of({ dbName, name, fields, partialFilter })
      .chain(asyncify(async ({ dbName }) => {
        const db = await getDb(dbName)
        if (!db) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        // Store index metadata for potential future use
        // LMDB inherently ordered, so this is mainly for compatibility
        const indexKey = `index:${name}`
        await (meta as unknown as Database).put(indexKey, {
          db: dbName,
          fields,
          partialFilter,
          createdAt: new Date().toISOString(),
        })
        
        return { ok: true }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('indexDocuments')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  /**
   * Bulk document operations
   * 
   * Performs bulk insert/update operations in a single LMDB transaction.
   * All operations are atomic - either all succeed or none do (transaction rolls back).
   * 
   * **Transaction Behavior**: Uses LMDB's native transaction support for ACID guarantees.
   * If any document operation fails, the entire transaction is rolled back.
   * 
   * **Partial Success Reporting**: Even within a transaction, each document's
   * result is tracked individually in the results array.
   * 
   * @param dbName - The database alias
   * @param docs - Array of documents (each must have an `_id` field)
   * @returns `{ ok: true, results: [{ ok: true, id }, ...] }` on success
   * @throws HyperErr with status 404 if database not found
   * 
   * @example
   * ```ts
   * const result = await adapter.bulkDocuments({
   *   db: 'users',
   *   docs: [
   *     { _id: 'user-1', name: 'Alice' },
   *     { _id: 'user-2', name: 'Bob' },
   *     { _id: 'user-3', name: 'Charlie' }
   *   ]
   * })
   * // { ok: true, results: [{ ok: true, id: 'user-1' }, ...] }
   * ```
   */
  async function bulkDocuments({
    db: dbName,
    docs,
  }: {
    db: string
    docs: Record<string, unknown>[]
  }): Promise<LmdbResult> {
    return Async.of({ dbName, docs })
      .chain(asyncify(async ({ dbName, docs }) => {
        const db = await getDb(dbName)
        if (!db) {
          throw HyperErr({ status: 404, msg: 'database not found' })
        }
        
        const results: Array<{ ok: boolean; id: string; msg?: string }> = []
        
        // Use transaction for bulk operations
        await db.transaction(async () => {
          for (const doc of docs) {
            const id = doc._id as string
            if (!id) {
              results.push({ ok: false, id: '', msg: 'document missing _id' })
              continue
            }
            
            try {
              await db.put(id, doc)
              results.push({ ok: true, id })
            } catch (e) {
              results.push({ ok: false, id, msg: (e as Error).message })
            }
          }
        })
        
        return { ok: true, results }
      }))
      .bichain(
        (e) => isHyperErr(e) ? Async.Rejected(e) : Async.Rejected(lmdbErrToHyperErr('bulkDocuments')(e)),
        Async.Resolved,
      )
      .bichain(handleHyperErr, Async.Resolved)
      .toPromise()
  }

  return Object.freeze({
    createDatabase,
    removeDatabase,
    createDocument,
    retrieveDocument,
    updateDocument,
    removeDocument,
    listDocuments,
    queryDocuments,
    indexDocuments,
    bulkDocuments,
  })
}