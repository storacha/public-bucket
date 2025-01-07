import { parseRange } from '@httpland/range-parser'

/**
 * @param {string} [str]
 * @returns {import('multipart-byte-range').Range[]}
 */
export const decodeRangeHeader = (str) => {
  if (!str) throw new Error('missing Range header value')
  /** @type {import('multipart-byte-range').Range[]} */
  const ranges = []
  for (const r of parseRange(str).rangeSet) {
    if (typeof r === 'string') {
      // "other" - ignore
    } else if ('firstPos' in r) {
      ranges.push(r.lastPos != null ? [r.firstPos, r.lastPos] : [r.firstPos])
    } else {
      ranges.push([-r.suffixLength])
    }
  }
  return ranges
}

/**
 * Resolve a range to an absolute range.
 *
 * @param {import('multipart-byte-range').Range} range
 * @param {() => Promise<number>} getTotalSize
 * @returns {Promise<import('multipart-byte-range').AbsoluteRange>}
 */
export const resolveRange = async ([first, last], getTotalSize) => {
  let totalSize = 0
  if (first < 0 || last == null) {
    totalSize = await getTotalSize()
  }
  return [
    first < 0 ? totalSize + first : first,
    last ?? totalSize - 1
  ]
}
