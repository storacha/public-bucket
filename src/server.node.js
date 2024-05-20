// eslint-disable-next-line
import * as API from './api.js'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import * as Server from './server.js'

/** @param {API.Handler} handler */
const toRequestListener = (handler) => {
  /** @type {import('node:http').RequestListener} */
  return async (req, res) => {
    const url = new URL(req.url || '', `http://${req.headers.host ?? 'localhost'}`)
    const headers = new Headers()
    for (let i = 0; i < req.rawHeaders.length; i += 2) {
      headers.append(req.rawHeaders[i], req.rawHeaders[i + 1])
    }
    const { method } = req
    const body =
      /** @type {ReadableStream|undefined} */
      (['GET', 'HEAD'].includes(method ?? '') ? undefined : Readable.toWeb(req))
    const request = new Request(url, { method, headers, body })

    const response = await handler(request)

    res.statusCode = response.status
    res.statusMessage = response.statusText
    response.headers.forEach((v, k) => res.setHeader(k, v))
    if (!response.body) {
      res.end()
      return
    }

    // @ts-expect-error
    await pipeline(Readable.fromWeb(response.body), res)
  }
}

/**
 * @param {{ bucket: API.Bucket }} model
 * @returns {import('node:http').RequestListener}
 */
export const createHandler = ({ bucket }) => toRequestListener(Server.createHandler({ bucket }))
