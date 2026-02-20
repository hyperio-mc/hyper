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
        if (endkey) rangeOptions.end = endkey
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
   * Note: LMDB uses ordered keys, so primary key index is automatic.
   * This is a no-op for secondary indexes (would need separate index databases).
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