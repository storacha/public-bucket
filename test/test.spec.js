import http from 'node:http'
import { equals } from 'uint8arrays'
import { Buffer } from 'buffer'
import * as ByteRanges from 'byteranges'
import { MemoryBucket } from '../src/bucket.js'
import * as Server from '../src/server.node.js'

/** @typedef {{ bucket: MemoryBucket, bucketURL: URL }} Context */

/** @param {(assert: import('entail').assert, ctx: Context) => unknown} testfn */
const withBucketServer = testfn =>
  /** @type {(assert: import('entail').assert) => unknown} */
  // eslint-disable-next-line
  (async (assert) => {
    const bucket = new MemoryBucket()
    const server = http.createServer(Server.createHandler({ bucket }))
    await new Promise(resolve => server.listen(resolve))
    // @ts-expect-error
    const { port } = server.address()
    const bucketURL = new URL(`http://127.0.0.1:${port}`)
    try {
      await testfn(assert, { bucket, bucketURL })
    } finally {
      server.close()
    }
  })

export const test = {
  /** @param {Context} ctx */
  'should respond 404 when key not found': withBucketServer(async (/** @type {import('entail').assert} */ assert, ctx) => {
    const res = await fetch(new URL('/notfound', ctx.bucketURL))
    assert.equal(res.status, 404)
  }),

  /** @param {Context} ctx */
  'should GET a value for a key': withBucketServer(async (/** @type {import('entail').assert} */ assert, ctx) => {
    const key = 'test' + Date.now()
    const value = new Uint8Array([12, 3])
    ctx.bucket.put(key, value)

    const res = await fetch(new URL(`/${key}`, ctx.bucketURL))
    assert.equal(res.status, 200)
    assert.ok(equals(new Uint8Array(await res.arrayBuffer()), value))
  }),

  /** @param {Context} ctx */
  'should HEAD a key': withBucketServer(async (/** @type {import('entail').assert} */ assert, ctx) => {
    const key = 'test' + Date.now()
    const value = new Uint8Array([1, 3, 8])
    ctx.bucket.put(key, value)

    const res = await fetch(new URL(`/${key}`, ctx.bucketURL), { method: 'HEAD' })
    assert.equal(res.status, 200)
    assert.equal(res.headers.get('Accept-Ranges'), 'bytes')
    assert.equal(res.headers.get('Content-Length'), String(value.length))
    assert.equal(res.body, null)
  }),

  /** @param {Context} ctx */
  'should GET a byte range of a value for a key': withBucketServer(async (/** @type {import('entail').assert} */ assert, ctx) => {
    const key = 'test' + Date.now()
    const value = new Uint8Array([1, 1, 3, 8])
    const range = [1, 3]
    ctx.bucket.put(key, value)

    const res = await fetch(new URL(`/${key}`, ctx.bucketURL), {
      headers: { Range: `bytes=${range[0]}-${range[1]}` }
    })
    assert.equal(res.status, 206)
    assert.equal(res.headers.get('Content-Range'), `bytes ${range[0]}-${range[1]}/${value.length}`)
    assert.ok(equals(new Uint8Array(await res.arrayBuffer()), value.slice(range[0], range[1] + 1)))
  }),

  /** @param {Context} ctx */
  'should GET a multipart byte range of a value for a key': withBucketServer(async (/** @type {import('entail').assert} */ assert, ctx) => {
    const key = 'test' + Date.now()
    const value = new Uint8Array([1, 1, 3, 8, 1, 1, 3, 8])
    const ranges = [[1, 3], [6, 7]]
    ctx.bucket.put(key, value)

    const res = await fetch(new URL(`/${key}`, ctx.bucketURL), {
      headers: { Range: `bytes=${ranges.map(r => `${r[0]}-${r[1]}`).join(', ')}` }
    })
    assert.equal(res.status, 206)

    const contentType = res.headers.get('Content-Type')
    assert.ok(contentType)

    const boundary = contentType.replace('multipart/byteranges; boundary=', '')
    const body = Buffer.from(await res.arrayBuffer())

    const parts = ByteRanges.parse(body, boundary)
    assert.equal(parts.length, ranges.length)

    for (let i = 0; i < parts.length; i++) {
      assert.ok(equals(parts[i].octets, value.slice(ranges[i][0], ranges[i][1] + 1)))
    }
  })
}
