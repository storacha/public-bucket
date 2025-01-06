// eslint-disable-next-line
import * as API from './api.js'
import { MultipartByteRangeEncoder } from 'multipart-byte-range/encoder'
import { decodeRangeHeader, resolveRange } from './range.js'
import { createBatchingByteGetter } from './batch.js'

export { MaxBatchSize } from './batch.js'

class NotFoundError extends Error {}

/**
 * @param {{ bucket: API.Bucket } & API.HandlerOptions} model
 * @returns {API.Handler}
 */
export const createHandler = model => req => handler(model, req)

/**
 * @param {{ bucket: API.Bucket } & API.HandlerOptions} model
 * @param {Request} request
 */
export const handler = async ({ bucket, maxBatchSize }, request) => {
  const url = new URL(request.url)
  const key = url.pathname.slice(1)

  if (request.method === 'HEAD') {
    const object = await bucket.head(key)
    if (!object) return new Response('Object Not Found', { status: 404 })
    const headers = new Headers()
    headers.set('Etag', object.httpEtag)
    headers.set('Accept-Ranges', 'bytes')
    headers.set('Content-Length', object.size.toString())
    return new Response(undefined, { headers })
  }

  if (request.method !== 'GET') {
    return new Response('Method not allowed', { status: 405 })
  }

  /** @type {import('multipart-byte-range').Range[]} */
  let ranges = []
  if (request.headers.has('range')) {
    try {
      ranges = decodeRangeHeader(request.headers.get('range') ?? '')
    } catch (err) {
      return new Response('invalid range', { status: 400 })
    }
  }

  const headers = new Headers()
  headers.set('X-Content-Type-Options', 'nosniff')
  headers.set('Content-Type', 'application/octet-stream')
  headers.set('Cache-Control', 'public, max-age=29030400, immutable')
  headers.set('Vary', 'Range')

  try {
    if (ranges.length > 1) {
      return await handleMultipartRange(bucket, key, ranges, { headers, maxBatchSize })
    } else if (ranges.length === 1) {
      return await handleRange(bucket, key, ranges[0], { headers })
    }

    // no range is effectively Range: bytes=0-
    return await handleRange(bucket, key, [0], { headers })
  } catch (err) {
    if (err instanceof NotFoundError) {
      return new Response('Object Not Found', { status: 404 })
    }
    console.error(err)
    return new Response('Internal Server Error', { status: 500 })
  }
}

/**
 * @param {API.Bucket} bucket
 * @param {string} key
 * @param {import('multipart-byte-range').Range} range
 * @param {{ headers?: Headers }} [options]
 */
const handleRange = async (bucket, key, range, options) => {
  const getTotalSize = async () => {
    const object = await bucket.head(key)
    if (!object) throw new NotFoundError('Object Not Found')
    return object.size
  }
  await getTotalSize()
  const [first, last] = await resolveRange(range, getTotalSize)
  const contentLength = last - first + 1

  const object = await bucket.get(key, { range: { offset: first, length: contentLength } })
  if (!object || !object.body) throw new Error('Object Not Found')

  const headers = new Headers(options?.headers)
  headers.set('Content-Length', String(contentLength))

  if (object.size !== contentLength) {
    const contentRange = `bytes ${first}-${last}/${object.size}`
    headers.set('Content-Range', contentRange)
  }
  headers.set('Etag', object.httpEtag)

  const status = object.size === contentLength ? 200 : 206
  const source = /** @type {ReadableStream} */ (object.body)
  return new Response(source, { status, headers })
}

/**
 * @param {API.Bucket} bucket
 * @param {string} key
 * @param {import('multipart-byte-range').Range[]} ranges
 * @param {{ headers?: Headers, maxBatchSize?: number }} [options]
 */
const handleMultipartRange = async (bucket, key, ranges, options) => {
  /** @type {number|undefined} */
  let totalSize
  const getTotalSize = async () => {
    if (totalSize != null) return totalSize
    const object = await bucket.head(key)
    if (!object) throw new NotFoundError('Object Not Found')
    totalSize = object.size
    return totalSize
  }
  await getTotalSize()

  const resolvedRanges = await Promise.all(ranges.map(r => resolveRange(r, getTotalSize)))

  const getBytes = createBatchingByteGetter(async range => {
    const options = { range: { offset: range[0], length: range[1] - range[0] + 1 } }
    const object = await bucket.get(key, options)
    if (!object || !object.body) throw new Error('Object Not Found')
    return /** @type {ReadableStream} */ (object.body)
  }, resolvedRanges, { maxSize: options?.maxBatchSize })
  const source = new MultipartByteRangeEncoder(ranges, getBytes, { totalSize })

  const headers = new Headers(options?.headers)
  for (const [k, v] of Object.entries(source.headers)) {
    headers.set(k, v)
  }

  return new Response(source, { status: 206, headers })
}
