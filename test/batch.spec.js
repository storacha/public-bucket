import { webcrypto as crypto } from 'node:crypto'
import { equals } from 'uint8arrays'
import { Uint8ArrayList } from 'uint8arraylist'
import { batchRanges, createBatchingByteGetter } from '../src/batch.js'

export const test = {
  'should batch ranges within batch size': (/** @type {import('entail').assert} */ assert) => {
    const batches = batchRanges([[3, 5], [7, 9], [10, 16], [17, 20], [21, 22]], { maxSize: 6 })
    assert.deepEqual(batches, [[[3, 5], [7, 9]], [[10, 16]], [[17, 20], [21, 22]]])
  },

  'should not batch ranges larger than max batch size': (/** @type {import('entail').assert} */ assert) => {
    const batches = batchRanges([[3, 5], [7, 9]], { maxSize: 5 })
    assert.deepEqual(batches, [[[3, 5]], [[7, 9]]])
  },

  'should not batch ranges when bytes between exceeds max batch size': (/** @type {import('entail').assert} */ assert) => {
    const batches = batchRanges([[3, 5], [8, 10]], { maxSize: 6 })
    assert.deepEqual(batches, [[[3, 5]], [[8, 10]]])
  },

  'should fail when ranges overlap': (/** @type {import('entail').assert} */ assert) => {
    assert.throws(() => batchRanges([[3, 5], [4, 6]]), /overlapping/)
  },

  'should fetch correct bytes from batching byte getter': async (/** @type {import('entail').assert} */ assert) => {
    const bytes = crypto.getRandomValues(new Uint8Array(50))
    /** @type {import('multipart-byte-range').AbsoluteRange[]} */
    const ranges = [[3, 5], [7, 9], [10, 16], [17, 20], [21, 22]]
    const getBytes = createBatchingByteGetter(async range => {
      return new ReadableStream({
        pull (controller) {
          controller.enqueue(bytes.subarray(range[0], range[1] + 1))
          controller.close()
        }
      })
    }, ranges)

    for (const r of ranges) {
      const buf = new Uint8ArrayList()
      const source = await getBytes(r)
      await source.pipeTo(new WritableStream({ write: chunk => { buf.append(chunk) } }))
      assert.ok(equals(buf.slice(), bytes.slice(r[0], r[1] + 1)))
    }
  }
}
