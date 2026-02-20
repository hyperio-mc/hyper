# @hyper63/adapter-lmdb

A high-performance LMDB adapter for hyper Data Port.

## Overview

This adapter implements the hyper Data Port interface using [LMDB](https://github.com/kriszyp/lmdb-js) (Lightning Memory-Mapped Database), providing:

- **Ultra-fast performance** - LMDB is one of the fastest key-value stores available
- **ACID compliance** - Full transactional support with crash recovery
- **Memory efficiency** - Memory-mapped files for optimal resource usage
- **Optional compression** - Built-in LZ4 compression for storage efficiency
- **Full Data Port API** - Complete implementation of all Data Port methods

## Installation

```typescript
import lmdb from '@hyper63/adapter-lmdb'
```

## Usage

### Basic Configuration

```typescript
import hyper from '@hyper63/core'
import app from '@hyper63/app-opine'
import lmdb from '@hyper63/adapter-lmdb'

export default {
  app,
  adapters: [
    {
      port: 'data',
      plugins: [lmdb({ dir: './data' })],
    },
  ],
}
```

### Configuration Options

```typescript
interface AdapterConfig {
  /**
   * Directory path where LMDB databases will be stored
   * Defaults to './data'
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
   * Defaults to true
   */
  compression?: boolean
}
```

## API Implementation

The adapter implements all Data Port methods:

### `createDatabase(name: string)`
Creates a new database with the given name.

### `removeDatabase(name: string)`
Removes an existing database and all its documents.

### `createDocument({ db, id, doc })`
Creates a new document in the specified database. Fails if document already exists.

### `retrieveDocument({ db, id })`
Retrieves a document by its ID from the specified database.

### `updateDocument({ db, id, doc })`
Updates an existing document or creates it if it doesn't exist (upsert).

### `removeDocument({ db, id })`
Removes a document from the specified database.

### `listDocuments({ db, limit, startkey, endkey, keys, descending })`
Lists documents from a database with optional filtering and pagination.

### `queryDocuments({ db, query })`
Queries documents using a MongoDB-style selector. Supports:
- Basic equality matching
- Comparison operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
- Array operators: `$in`, `$nin`
- Existence check: `$exists`
- Sorting, limiting, skipping, and field projection

### `indexDocuments({ db, name, fields, partialFilter })`
Creates index metadata for compatibility. Note: LMDB uses ordered keys by default, so primary key lookups are already optimized.

### `bulkDocuments({ db, docs })`
Performs bulk document operations (insert/replace) in a single transaction.

## Query Examples

### Basic Query

```typescript
const result = await data.query({
  db: 'mydb',
  query: {
    selector: { type: 'user' }
  }
})
// { ok: true, docs: [...] }
```

### Query with Operators

```typescript
const result = await data.query({
  db: 'mydb',
  query: {
    selector: { 
      type: 'user',
      age: { $gte: 18 }
    },
    sort: [{ age: 'DESC' }],
    limit: 10
  }
})
```

### List Documents

```typescript
const result = await data.listDocuments({
  db: 'mydb',
  startkey: 'user-100',
  endkey: 'user-200',
  limit: 50
})
```

## Performance Considerations

- **Memory-mapped**: LMDB uses memory-mapped files, so it benefits from OS-level caching
- **Ordered keys**: Documents are stored in key order, making range queries efficient
- **Transactions**: Bulk operations use LMDB transactions for atomic commits
- **Compression**: Enable compression for storage efficiency with minimal performance overhead

## License

MIT