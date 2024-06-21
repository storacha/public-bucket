// eslint-disable-next-line
import * as API from './api.js'
import { MultipartByteRangeEncoder } from 'multipart-byte-range/encoder'
import { decodeRangeHeader, resolveRange } from './range.js'
import { createBatchingByteGetter } from './batch.js'

export { MaxBatchSize } from './batch.js'

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

  const object = await bucket.head(key)
  if (!object) return new Response('Object Not Found', { status: 404 })

  const headers = new Headers()
  headers.set('Etag', object.httpEtag)

  if (request.method === 'HEAD') {
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

  headers.set('X-Content-Type-Options', 'nosniff')
  headers.set('Content-Type', 'application/octet-stream')
  headers.set('Etag', object.httpEtag)
  headers.set('Cache-Control', 'public, max-age=29030400, immutable')
  headers.set('Vary', 'Range')

  if (ranges.length > 1) {
    return handleMultipartRange(bucket, key, object.size, ranges, { headers, maxBatchSize })
  } else if (ranges.length === 1) {
    return handleRange(bucket, key, object.size, ranges[0], { headers })
  }

  // no range is effectively Range: bytes=0-
  return handleRange(bucket, key, object.size, [0], { headers })
}

/**
 * @param {API.Bucket} bucket
 * @param {string} key
 * @param {number} size
 * @param {import('multipart-byte-range').Range} range
 * @param {{ headers?: Headers }} [options]
 */
const handleRange = async (bucket, key, size, range, options) => {
  const [first, last] = resolveRange(range, size)
  const contentLength = last - first + 1

  const headers = new Headers(options?.headers)
  headers.set('Content-Length', String(contentLength))

  if (size !== contentLength) {
    const contentRange = `bytes ${first}-${last}/${size}`
    headers.set('Content-Range', contentRange)
  }

  const status = size === contentLength ? 200 : 206
  const object = await bucket.get(key, { range: { offset: first, length: contentLength } })
  if (!object || !object.body) throw new Error('Object Not Found')

  const source = /** @type {ReadableStream} */ (object.body)
  return new Response(source, { status, headers })
}

/**
 * @param {API.Bucket} bucket
 * @param {string} key
 * @param {number} size
 * @param {import('multipart-byte-range').Range[]} ranges
 * @param {{ headers?: Headers, maxBatchSize?: number }} [options]
 */
const handleMultipartRange = async (bucket, key, size, ranges, options) => {
  const getBytes = createBatchingByteGetter(async range => {
    const options = { range: { offset: range[0], length: range[1] - range[0] + 1 } }
    const object = await bucket.get(key, options)
    if (!object || !object.body) throw new Error('Object Not Found')
    return /** @type {ReadableStream} */ (object.body)
  }, ranges.map(r => resolveRange(r, size)), { maxSize: options?.maxBatchSize })
  const source = new MultipartByteRangeEncoder(ranges, getBytes, { totalSize: size })

  const headers = new Headers(options?.headers)
  for (const [k, v] of Object.entries(source.headers)) {
    headers.set(k, v)
  }

  return new Response(source, { status: 206, headers })
}
