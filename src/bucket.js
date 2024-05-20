// eslint-disable-next-line
import * as API from './api.js'

/** @implements {API.Bucket} */
export class MemoryBucket {
  /** @type {Map<string, Uint8Array>} */
  #data = new Map()

  /** @param {string} key */
  async head (key) {
    const value = this.#data.get(key)
    if (!value) return null
    return {
      httpEtag: `"${key}"`,
      size: value.length,
      body: null
    }
  }

  /**
   * @param {string} key
   * @param {API.GetOptions} [options]
   */
  async get (key, options) {
    const value = this.#data.get(key)
    if (!value) return null
    return {
      httpEtag: `"${key}"`,
      size: value.length,
      body: new ReadableStream({
        pull: (controller) => {
          if (options?.range) {
            const { offset, length } = options.range
            controller.enqueue(value.slice(offset, offset + length))
          } else {
            controller.enqueue(value)
          }
          controller.close()
        }
      })
    }
  }

  /**
   * @param {string} key
   * @param {Uint8Array} value
   */
  put (key, value) {
    this.#data.set(key, value)
  }

  clear () {
    this.#data.clear()
  }
}
