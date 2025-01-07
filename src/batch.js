import { context, SpanStatusCode, trace } from '@opentelemetry/api'
import defer from 'p-defer'
import { Uint8ArrayList } from 'uint8arraylist'

/** The default maximum size in bytes of a batch request to the bucket (10MiB). */
export const MaxBatchSize = 10 * 1024 * 1024

/**
 * @param {import('multipart-byte-range').ByteGetter} getBytes
 * @param {import('multipart-byte-range').AbsoluteRange[]} ranges
 * @param {{ maxSize?: number }} [options]
 * @returns {import('multipart-byte-range').ByteGetter}
 */
export const createBatchingByteGetter = (getBytes, ranges, options) => {
  /** @type {Record<string, import('p-defer').DeferredPromise<ReadableStream<Uint8Array>>>} */
  const requests = {}
  const batches = batchRanges(ranges, options)

  return async range => {
    if (requests[range.toString()]) {
      return requests[range.toString()].promise
    }

    const batch = batches.find(b => b.some(r => r[0] === range[0] && r[1] === range[1]))
    if (!batch) throw new Error(`batch not found for range: ${range[0]}-${range[1]}`)
    for (const r of batch) {
      requests[r.toString()] = defer()
    }

    const source = await getBytes([batch[0][0], batch[batch.length - 1][1]])
    consumeSource(source, batch, requests)
    return requests[range.toString()].promise
  }
}

/**
 * @template {unknown[]} A
 * @template {*} T
 * @template {*} This
 * @param {string} spanName
 * @param {(this: This, ...args: A) => Promise<T>} fn
 * @param {This} [thisParam]
 */
export const withSimpleSpan = (spanName, fn, thisParam) =>
  /**
   * @param {A} args
  */
  async (...args) => {
    const tracer = trace.getTracer('public-bucket')
    const span = tracer.startSpan(spanName)
    const ctx = trace.setSpan(context.active(), span)

    try {
      const result = await context.with(ctx, fn, thisParam, ...args)
      span.setStatus({ code: SpanStatusCode.OK })
      span.end()
      return result
    } catch (err) {
      if (err instanceof Error) {
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: err.message
        })
      } else {
        span.setStatus({
          code: SpanStatusCode.ERROR
        })
      }
      span.end()
      throw err
    }
  }

const consumeSource = withSimpleSpan('consumeSource',
  /**
 *
 * @param {ReadableStream<Uint8Array>} source
 * @param {import('multipart-byte-range').AbsoluteRange[]} ranges
 * @param {Record<string, import('p-defer').DeferredPromise<ReadableStream<Uint8Array>>>} requests
 */
  async (source, ranges, requests) => {
    const parts = new Uint8ArrayList()
    // start at first byte of first blob
    let farthestRead = ranges[0][0]
    const offset = ranges[0][0]
    let currentRange = 0
    for await (const chunk of source) {
    // append the chunk to our buffer
      parts.append(chunk)
      // update the absolute position of how far we've read
      farthestRead += chunk.byteLength
      // resolve any blobs in the current buffer
      // note that as long as blobs are sorted ascending by start
      // this should be resilient to overlapping ranges
      while (farthestRead >= ranges[currentRange][1] + 1) {
        const start = ranges[currentRange][0] - offset
        const end = ranges[currentRange][1] + 1 - offset
        // generate blob out of the current buffer
        requests[ranges[currentRange].toString()].resolve(new ReadableStream({
          pull (controller) {
            controller.enqueue(parts.subarray(start, end))
            controller.close()
          }
        }))
        currentRange++
        if (currentRange >= ranges.length) {
          return
        }
      }
    }
    throw new Error('did not consume all parts')
  }, null)

/**
 * @param {import('multipart-byte-range').AbsoluteRange[]} ranges
 * @param {{ maxSize?: number }} [options]
 */
export const batchRanges = (ranges, options) => {
  ranges = [...ranges].sort((a, b) => a[0] - b[0])

  const maxSize = options?.maxSize ?? MaxBatchSize
  const batches = []
  /** @type {import('multipart-byte-range').AbsoluteRange[]} */
  let batch = []
  let batchSize = 0
  for (const r of ranges) {
    const size = r[1] - r[0]
    const prevRange = batch.at(-1)
    const bytesBetween = prevRange ? r[0] - prevRange[1] : 0
    if (bytesBetween < 0) throw new Error('overlapping byte ranges')

    if (batchSize + bytesBetween + size > maxSize) {
      batches.push(batch)
      batch = []
      batchSize = 0
    }

    batch.push(r)
    batchSize += bytesBetween + size
  }
  batches.push(batch)
  return batches
}
