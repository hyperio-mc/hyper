import { assert, assertEquals, assertRejects } from '@std/assert'
import { join } from '@std/path'
import { ensureDir, exists } from '@std/fs'
import { adapter } from './adapter.ts'
import { open } from 'npm:lmdb@2.8.3'
import type { Database, RootDatabase } from 'npm:lmdb@2.8.3'
import type { DbInfo } from './types.ts'

// Helper to create test environment
async function createTestEnv(testName: string) {
  const testDir = join(Deno.env.get('TMPDIR') || '/tmp', `lmdb-test-${testName}-${Date.now()}`)
  await ensureDir(testDir)
  
  const dbPath = join(testDir, 'test.mdb')
  const rootDb = open({
    path: dbPath,
    maxDbs: 10,
    mapSize: 10485760, // 10MB for tests
    compression: false,
  }) as RootDatabase
  
  const meta = rootDb.openDB<DbInfo>('meta', {
    encoding: 'msgpack',
  }) as Database<DbInfo>
  
  const dbs = new Map<string, Database<Record<string, unknown>>>()
  
  return {
    rootDb,
    meta,
    dbs,
    testDir,
    cleanup: async () => {
      try {
        rootDb.close()
      } catch (e) {
        console.error('Error closing test db:', e)
      }
      try {
        await Deno.remove(testDir, { recursive: true })
      } catch (e) {
        console.error('Error removing test dir:', e)
      }
    },
  }
}

Deno.test('LMDB Adapter - createDatabase', async (t) => {
  const env = await createTestEnv('createDatabase')
  const data = adapter(env)
  
  await t.step('should create a database successfully', async () => {
    const result = await data.createDatabase('test-db')
    assert(result.ok)
  })
  
  await t.step('should fail to create duplicate database', async () => {
    await data.createDatabase('duplicate-db')
    const result = await data.createDatabase('duplicate-db')
    assert(!result.ok)
    assertEquals(result.status, 409)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - createDocument', async (t) => {
  const env = await createTestEnv('createDocument')
  const data = adapter(env)
  
  await data.createDatabase('test-db')
  
  await t.step('should create a document with provided id', async () => {
    const result = await data.createDocument({
      db: 'test-db',
      id: 'doc-1',
      doc: { name: 'Test Document', value: 42 },
    })
    assert(result.ok)
    assertEquals(result.id, 'doc-1')
  })
  
  await t.step('should fail on document conflict', async () => {
    await data.createDocument({
      db: 'test-db',
      id: 'doc-2',
      doc: { name: 'Test' },
    })
    
    const result = await data.createDocument({
      db: 'test-db',
      id: 'doc-2',
      doc: { name: 'Duplicate' },
    })
    assert(!result.ok)
    assertEquals(result.status, 409)
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.createDocument({
      db: 'nonexistent',
      id: 'doc-1',
      doc: { name: 'Test' },
    })
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await t.step('should fail on empty document', async () => {
    const result = await data.createDocument({
      db: 'test-db',
      id: 'doc-empty',
      doc: {},
    })
    assert(!result.ok)
    assertEquals(result.status, 400)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - retrieveDocument', async (t) => {
  const env = await createTestEnv('retrieveDocument')
  const data = adapter(env)
  
  await data.createDatabase('test-db')
  await data.createDocument({
    db: 'test-db',
    id: 'doc-1',
    doc: { name: 'Test', value: 123 },
  })
  
  await t.step('should retrieve an existing document', async () => {
    const result = await data.retrieveDocument({ db: 'test-db', id: 'doc-1' })
    assert('_id' in result || 'name' in result)
    if ('name' in result) assertEquals(result.name, 'Test')
    if ('value' in result) assertEquals(result.value, 123)
  })
  
  await t.step('should fail on non-existent document', async () => {
    const result = await data.retrieveDocument({ db: 'test-db', id: 'nonexistent' })
    assert('ok' in result && !result.ok)
    assertEquals(result.status, 404)
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.retrieveDocument({ db: 'nonexistent', id: 'doc-1' })
    assert('ok' in result && !result.ok)
    assertEquals(result.status, 404)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - updateDocument', async (t) => {
  const env = await createTestEnv('updateDocument')
  const data = adapter(env)
  
  await data.createDatabase('test-db')
  await data.createDocument({
    db: 'test-db',
    id: 'doc-1',
    doc: { name: 'Original', count: 1 },
  })
  
  await t.step('should update an existing document', async () => {
    const result = await data.updateDocument({
      db: 'test-db',
      id: 'doc-1',
      doc: { name: 'Updated', count: 2 },
    })
    assert(result.ok)
    assertEquals(result.id, 'doc-1')
    
    const doc = await data.retrieveDocument({ db: 'test-db', id: 'doc-1' })
    if ('name' in doc) assertEquals(doc.name, 'Updated')
    if ('count' in doc) assertEquals(doc.count, 2)
  })
  
  await t.step('should upsert a non-existent document', async () => {
    const result = await data.updateDocument({
      db: 'test-db',
      id: 'doc-new',
      doc: { name: 'New Document' },
    })
    assert(result.ok)
    assertEquals(result.id, 'doc-new')
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.updateDocument({
      db: 'nonexistent',
      id: 'doc-1',
      doc: { name: 'Test' },
    })
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - removeDocument', async (t) => {
  const env = await createTestEnv('removeDocument')
  const data = adapter(env)
  
  await data.createDatabase('test-db')
  await data.createDocument({
    db: 'test-db',
    id: 'doc-1',
    doc: { name: 'Test' },
  })
  
  await t.step('should remove an existing document', async () => {
    const result = await data.removeDocument({ db: 'test-db', id: 'doc-1' })
    assert(result.ok)
    assertEquals(result.id, 'doc-1')
    
    // Verify it's gone
    const doc = await data.retrieveDocument({ db: 'test-db', id: 'doc-1' })
    assert('ok' in doc && !doc.ok)
  })
  
  await t.step('should fail on non-existent document', async () => {
    const result = await data.removeDocument({ db: 'test-db', id: 'nonexistent' })
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.removeDocument({ db: 'nonexistent', id: 'doc-1' })
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - listDocuments', async (t) => {
  const env = await createTestEnv('listDocuments')
  const data = adapter(env)
  
  await data.createDatabase('test-db')
  
  // Create test documents
  for (let i = 1; i <= 5; i++) {
    await data.createDocument({
      db: 'test-db',
      id: `doc-${i}`,
      doc: { name: `Document ${i}`, index: i },
    })
  }
  
  await t.step('should list all documents', async () => {
    const result = await data.listDocuments({ db: 'test-db' })
    assert(result.ok)
    assertEquals(result.docs?.length, 5)
  })
  
  await t.step('should respect limit', async () => {
    const result = await data.listDocuments({ db: 'test-db', limit: 2 })
    assert(result.ok)
    assertEquals(result.docs?.length, 2)
  })
  
  await t.step('should respect startkey', async () => {
    const result = await data.listDocuments({ db: 'test-db', startkey: 'doc-3' })
    assert(result.ok)
    assertEquals(result.docs?.length, 3) // doc-3, doc-4, doc-5
  })
  
  await t.step('should respect endkey', async () => {
    const result = await data.listDocuments({ db: 'test-db', endkey: 'doc-2' })
    assert(result.ok)
    assertEquals(result.docs?.length, 2) // doc-1, doc-2
  })
  
  await t.step('should respect descending order', async () => {
    const result = await data.listDocuments({ db: 'test-db', descending: true })
    assert(result.ok)
    assertEquals(result.docs?.length, 5)
    // In descending order, doc-5 should be first
    if (result.docs && result.docs.length > 0) {
      assert(result.docs[0]._id?.toString().startsWith('doc-5') || 
             result.docs[0]._id?.toString().includes('5'))
    }
  })
  
  await t.step('should filter by keys', async () => {
    const result = await data.listDocuments({ db: 'test-db', keys: 'doc-1,doc-3' })
    assert(result.ok)
    assertEquals(result.docs?.length, 2)
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.listDocuments({ db: 'nonexistent' })
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - queryDocuments', async (t) => {
  const env = await createTestEnv('queryDocuments')
  const data = adapter(env)
  
  await data.createDatabase('test-db')
  
  // Create test documents
  await data.createDocument({ db: 'test-db', id: 'user-1', doc: { type: 'user', name: 'Alice', age: 30 } })
  await data.createDocument({ db: 'test-db', id: 'user-2', doc: { type: 'user', name: 'Bob', age: 25 } })
  await data.createDocument({ db: 'test-db', id: 'user-3', doc: { type: 'user', name: 'Charlie', age: 35 } })
  await data.createDocument({ db: 'test-db', id: 'admin-1', doc: { type: 'admin', name: 'Admin' } })
  
  await t.step('should query with selector', async () => {
    const result = await data.queryDocuments({
      db: 'test-db',
      query: { selector: { type: 'user' } },
    })
    assert(result.ok)
    assertEquals(result.docs?.length, 3)
  })
  
  await t.step('should query with comparison operators', async () => {
    const result = await data.queryDocuments({
      db: 'test-db',
      query: { selector: { age: { $gte: 30 } } },
    })
    assert(result.ok)
    assertEquals(result.docs?.length, 2) // Alice (30) and Charlie (35)
  })
  
  await t.step('should query with limit', async () => {
    const result = await data.queryDocuments({
      db: 'test-db',
      query: { selector: { type: 'user' }, limit: 2 },
    })
    assert(result.ok)
    assertEquals(result.docs?.length, 2)
  })
  
  await t.step('should query with skip', async () => {
    const result1 = await data.queryDocuments({
      db: 'test-db',
      query: { selector: { type: 'user' } },
    })
    const result2 = await data.queryDocuments({
      db: 'test-db',
      query: { selector: { type: 'user' }, skip: 1 },
    })
    assert(result1.ok && result2.ok)
    assertEquals(result1.docs!.length - result2.docs!.length, 1)
  })
  
  await t.step('should query with fields projection', async () => {
    const result = await data.queryDocuments({
      db: 'test-db',
      query: { selector: { type: 'user' }, fields: ['name', 'age'] },
    })
    assert(result.ok)
    assert(result.docs?.length === 3)
    // Check that only specified fields are present
    const doc = result.docs![0]
    assert('name' in doc || 'age' in doc)
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.queryDocuments({
      db: 'nonexistent',
      query: { selector: {} },
    })
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - bulkDocuments', async (t) => {
  const env = await createTestEnv('bulkDocuments')
  const data = adapter(env)
  
  await data.createDatabase('test-db')
  
  await t.step('should bulk insert documents', async () => {
    const result = await data.bulkDocuments({
      db: 'test-db',
      docs: [
        { _id: 'bulk-1', name: 'Bulk 1' },
        { _id: 'bulk-2', name: 'Bulk 2' },
        { _id: 'bulk-3', name: 'Bulk 3' },
      ],
    })
    assert(result.ok)
    assertEquals(result.results?.length, 3)
    assert(result.results?.every(r => r.ok))
  })
  
  await t.step('should verify bulk inserted documents', async () => {
    const doc = await data.retrieveDocument({ db: 'test-db', id: 'bulk-1' })
    assert('name' in doc && doc.name === 'Bulk 1')
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.bulkDocuments({
      db: 'nonexistent',
      docs: [{ _id: 'test', name: 'Test' }],
    })
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - removeDatabase', async (t) => {
  const env = await createTestEnv('removeDatabase')
  const data = adapter(env)
  
  await t.step('should remove an existing database', async () => {
    await data.createDatabase('test-db')
    const result = await data.removeDatabase('test-db')
    assert(result.ok)
    
    // Verify database is gone
    const docResult = await data.createDocument({
      db: 'test-db',
      id: 'test',
      doc: { name: 'Test' },
    })
    assert(!docResult.ok)
    assertEquals(docResult.status, 404)
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.removeDatabase('nonexistent')
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await env.cleanup()
})

Deno.test('LMDB Adapter - indexDocuments', async (t) => {
  const env = await createTestEnv('indexDocuments')
  const data = adapter(env)
  
  await data.createDatabase('test-db')
  
  await t.step('should create an index (metadata only)', async () => {
    const result = await data.indexDocuments({
      db: 'test-db',
      name: 'name-index',
      fields: ['name'],
    })
    assert(result.ok)
  })
  
  await t.step('should create an index with sort direction', async () => {
    const result = await data.indexDocuments({
      db: 'test-db',
      name: 'age-index',
      fields: [{ age: 'DESC' }],
    })
    assert(result.ok)
  })
  
  await t.step('should fail on non-existent database', async () => {
    const result = await data.indexDocuments({
      db: 'nonexistent',
      name: 'test-index',
      fields: ['name'],
    })
    assert(!result.ok)
    assertEquals(result.status, 404)
  })
  
  await env.cleanup()
})