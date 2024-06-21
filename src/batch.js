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

    const offset = batch[0][0]
    const source = await getBytes([offset, batch[batch.length - 1][1]])

    const buffer = new Uint8ArrayList()
    await source.pipeTo(new WritableStream({ write: chunk => { buffer.append(chunk) } }))

    for (const r of batch) {
      requests[r.toString()].resolve(new ReadableStream({
        pull (controller) {
          controller.enqueue(buffer.subarray(r[0] - offset, (r[1] + 1) - offset))
          controller.close()
        }
      }))
    }

    return requests[range.toString()].promise
  }
}

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
