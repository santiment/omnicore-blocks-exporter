const pkg = require('./package.json');
const { send } = require('micro')
const url = require('url')
const { Exporter } = require('san-exporter')
const rp = require('request-promise-native')
const PQueue = require('p-queue')
const uuidv1 = require('uuid/v1')
const metrics = require('./src/metrics')

const exporter = new Exporter(pkg.name)

const SEND_BATCH_SIZE = parseInt(process.env.SEND_BATCH_SIZE || "10")
const DEFAULT_TIMEOUT = parseInt(process.env.DEFAULT_TIMEOUT || "10000")
const CONFIRMATIONS = parseInt(process.env.CONFIRMATIONS || "3")
const NODE_URL = process.env.NODE_URL || 'http://omnicored-bitcoind.default.svc.cluster.local:8332'
const RPC_USERNAME = process.env.RPC_USERNAME || 'rpcuser'
const RPC_PASSWORD = process.env.RPC_PASSWORD || 'rpcpassword'
const MAX_CONCURRENCY = parseInt(process.env.MAX_CONCURRENCY || "5")

const requestQueue = new PQueue({ concurrency: MAX_CONCURRENCY })

const request = rp.defaults({
  method: 'POST',
  uri: NODE_URL,
  auth: {
    user: RPC_USERNAME,
    pass: RPC_PASSWORD
  },
  timeout: DEFAULT_TIMEOUT,
  gzip: true,
  json: true
})

let lastProcessedPosition = {
  blockNumber: parseInt(process.env.BLOCK || "1"),
}

const sendRequest = (async (method, params) => {
  metrics.requestsCounter.inc()

  return requestQueue.add(() => {
    const startTime = new Date()
    return request({
      body: {
        jsonrpc: '1.0',
        id: uuidv1(),
        method: method,
        params: params
      }
    }).then(({ result, error }) => {
      metrics.requestsResponseTime.observe(new Date() - startTime)

      if (error) {
        return Promise.reject(error)
      }

      return result
    })
  })
})
  
const fetchBlock = async (block_index) => {
  const transactionsRequests = sendRequest('omni_listblocktransactions', [block_index]).then((transactions) => {
    return Promise.all(
      transactions.map((transactionHash) =>
        sendRequest('omni_gettransaction', [transactionHash])
      )
    )
  })

  const blockHash = await sendRequest('getblockhash', [block_index])
  const block = await sendRequest('getblock', [blockHash, true])

  const transactions = await transactionsRequests

  return {
    ...block,
    tx: transactions
  }
}

async function work() {
  const blockchainInfo = await sendRequest('getblockchaininfo', [])
  const currentBlock = blockchainInfo.blocks - CONFIRMATIONS

  metrics.currentBlock.set(currentBlock)

  const requests = []

  while (lastProcessedPosition.blockNumber + requests.length < currentBlock) {
    const blockToDownload = lastProcessedPosition.blockNumber + requests.length

    requests.push(fetchBlock(blockToDownload))

    if (requests.length >= SEND_BATCH_SIZE || blockToDownload == currentBlock) {
      const blocks = await Promise.all(requests).map((block) => {
        metrics.downloadedTransactionsCounter.inc(block.tx.length)
        metrics.downloadedBlocksCounter.inc()

        return block
      })

      console.log(`Flushing blocks ${blocks[0].height}:${blocks[blocks.length - 1].height}`)
      await exporter.sendDataWithKey(blocks, "height")

      lastProcessedPosition.blockNumber += blocks.length
      await exporter.savePosition(lastProcessedPosition)
      metrics.lastExportedBlock.set(lastProcessedPosition.blockNumber)

      requests.length = 0
    }
  }
}

async function initLastProcessedLedger() {
  const lastPosition = await exporter.getLastPosition()

  if (lastPosition) {
    lastProcessedPosition = lastPosition
    console.info(`Resuming export from position ${JSON.stringify(lastPosition)}`)
  } else {
    await exporter.savePosition(lastProcessedPosition)
    console.info(`Initialized exporter with initial position ${JSON.stringify(lastProcessedPosition)}`)
  }
}

const fetchEvents = () => {
  return work()
    .then(() => {
      // Look for new events every 1 sec
      setTimeout(fetchEvents, 1000)
    })
}

const init = async () => {
  metrics.startCollection()
  await exporter.connect()
  await initLastProcessedLedger()
  await fetchEvents()
}

init()

const healthcheckKafka = () => {
  return new Promise((resolve, reject) => {
    if (exporter.producer.isConnected()) {
      resolve()
    } else {
      reject("Kafka client is not connected to any brokers")
    }
  })
}

module.exports = async (request, response) => {
  const req = url.parse(request.url, true);

  switch (req.pathname) {
    case '/healthcheck':
      return healthcheckKafka()
        .then(() => send(response, 200, "ok"))
        .catch((err) => send(response, 500, `Connection to kafka failed: ${err}`))

    case '/metrics':
      response.setHeader('Content-Type', metrics.register.contentType);
      return send(response, 200, metrics.register.metrics())

    default:
      return send(response, 404, 'Not found');
  }
}
