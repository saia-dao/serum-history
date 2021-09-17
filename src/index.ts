import { Account, Commitment, Connection, PublicKey } from '@solana/web3.js'
import { Market } from '@project-serum/serum'
import cors from 'cors'
import express from 'express'
import { Tedis, TedisPool } from 'tedis'
import { URL } from 'url'
import { decodeRecentEvents } from './events'
import { MarketConfig, Trade, TradeSide } from './interfaces'
import { RedisConfig, RedisStore, createRedisStore } from './redis'
import { resolutions, sleep } from './time'
import {
  Config,
  getMarketByBaseSymbolAndKind,
  GroupConfig,
  MangoClient,
  PerpMarketConfig,
  FillEvent,
} from '@blockworks-foundation/mango-client'
import BN from 'bn.js'

async function collectEventQueue(m: MarketConfig, r: RedisConfig) {
  const store = await createRedisStore(r, m.marketName)
  const marketAddress = new PublicKey(m.marketPk)
  const programKey = new PublicKey(m.programId)
  const connection = new Connection(m.clusterUrl)
  const market = await Market.load(
    connection,
    marketAddress,
    undefined,
    programKey
  )

  async function fetchTrades(lastSeqNum?: number): Promise<[Trade[], number]> {
    const now = Date.now()
    const accountInfo = await connection.getAccountInfo(
      market['_decoded'].eventQueue
    )
    if (accountInfo === null) {
      throw new Error(
        `Event queue account for market ${m.marketName} not found`
      )
    }
    const { header, events } = decodeRecentEvents(accountInfo.data, lastSeqNum)
    const takerFills = events.filter(
      (e) => e.eventFlags.fill && !e.eventFlags.maker
    )
    const trades = takerFills
      .map((e) => market.parseFillEvent(e))
      .map((e) => {
        return {
          price: e.price,
          side: e.side === 'buy' ? TradeSide.Buy : TradeSide.Sell,
          size: e.size,
          ts: now,
        }
      })
    /*
    if (trades.length > 0)
      console.log({e: events.map(e => e.eventFlags), takerFills, trades})
    */
    return [trades, header.seqNum]
  }

  async function storeTrades(ts: Trade[]) {
    if (ts.length > 0) {
      console.log(m.marketName, ts.length)
      for (let i = 0; i < ts.length; i += 1) {
        await store.storeTrade(ts[i])
      }
    }
  }

  while (true) {
    try {
      const lastSeqNum = await store.loadNumber('LASTSEQ')
      const [trades, currentSeqNum] = await fetchTrades(lastSeqNum)
      storeTrades(trades)
      store.storeNumber('LASTSEQ', currentSeqNum)
    } catch (err) {
      console.error(m.marketName, err.toString())
    }
    await sleep({
      Seconds: process.env.INTERVAL ? parseInt(process.env.INTERVAL) : 10,
    })
  }
}

const redisUrl = new URL(process.env.REDISCLOUD_URL || 'redis://localhost:6379')
const host = redisUrl.hostname
const port = parseInt(redisUrl.port)
let password: string | undefined
if (redisUrl.password !== '') {
  password = redisUrl.password
}

const network = 'mainnet-beta'
const clusterUrl =
  process.env.RPC_ENDPOINT_URL || 'https://solana-api.projectserum.com'
const programIdV3 = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'

const nativeMarketsV3: Record<string, string> = {
  'POLIS': 'HxFLKUAmAMLz1jtT3hbvCMELwH5H9tpM2QugP8sKyfhW',
  'ATLAS': 'Di66GTLsV64JgCCYGVcY21RZ173BHkjJVgPyezNN7P1K',
  'CMPAS':'4Gg2WPaYbZNpy86tMhmw6w4CsiVupnNXwvbsLREMB5UQ',
  'BGEXE':'Fn5uYTGamghYcQfch6UbcSpj7VeMZukWmbLVNePnknvf',
  'PILOT':'4WckBAhK5YK78sZaYgiLAYZYgyhNkMN1JXkjRwWuyAg6',
  'PASS':'7bPXjodb2c4ZfBPL9QxyFF2JbErB67Rdbpd41ChpDSof',
  'MPAS':'6unizr2gWwNVZgzSTCmD9ih6QBScGm9dkG1dy7oWTweb',
  'BGSUP':'DBtN3BJJc5TDs4psnnoiRV5LL8Kk4YM2xdZ6M3VzBH4V',
  'BGOF':'3Dv3VmbzngyedBXcSgjxCVLS7w9giWHLzmCBmP2bQD6j',
  'BGCPN':'uSnksnhQim5QN49PkpRHjSZmcbjHSJ1WiQHD7SJ6cWc',
  'VIP':'CFJFbG6dWnh9Skr1UtH3UrqDRfngRDJQ4iBMuYsV2rp7',
  'CAPN':'5QsRgRyvrhvePAtwBMZNPSmVwZR5vKkga3n31HxS4P3L',
  'BGUNQ':'3X7koctR3jYVh4XV9QKNtbT9gkqsGWMSpwHs6nHtd9sR',
  'BGPR':'6B7idqUbnCaUMdvzPdQkHpBR287xPk46q3afYMxvsa6M',
  'BGAT':'8vycGWEqhvnTJR4aTcN2TvtUUNtLXoCTrpCGtoD8R4bo',
  'CHMVOS':'GxJDcyN83nWmcsC5arPpsuB8NEts1n6zK6FTDtDw29z',
  'CALGEI':'6kqfE89LpaPn25zMxxz83bWZK59BGzH9DJsDw2GZyxue',
  'VOPESQ':'CGuJJJp4UbBrph6EDoWgAKcP6J8ZfWJ28r72uoj9WBqU',
  'CEMPOP':'GSzxxsnVeTGmCLJ84y2dQeBoBVW1Su8bTqR7bJyDxjxn',
  'FBPLER':'Dauk5SEHR8sx3FNmgZVBUaAV7heWAujkxMbGSZkQXs1n',
  'TIGUEA':'6U9c1n2GU7tRr8Bs7vcXoXACmmEQ87X6To3ie5H6rKyB',
  'CORUS':'Cgk6aeUfK78maSWdFLxdEW8BEsyGcvdLz7Cue6SjUk8J',
  'CONCC':'5ovwY44rgJm1vYNmgAGnEjszhoULJqbCVrFiSS2a3T4c',
  'CONSO':'BA1XAKTYjMJBzYbB1usSZXesj9FudygvJ4wSWSFMK6xV',
  'PCHAUP':'GJtKxACMhWM9hNeX56RwNyCbE9Bet9zkRHDoLicLK9VK',
  'COVAS':'6Y4bSkNwcugHzDc8eDSqDL9m15mQi9Mc8UncxVm5YZRX',
  'TIGU':'yWNmeZVXg7My9PMdZGiSx7jsKYkkeCiqHeQbD7zdJeq',
  'STAR':'KHw8L2eE6kpp8ziWPghBTtiAVCUvdSKMvGtT1e9AuJd',
  'OMPH':'HdvXMScwAQQh9pEvLZjuaaeJcLTmixxYoMFefeqHFn2E',
  'PBA':'4jN1R453Acv9egnr7Dry3x9Xe3jqh1tqz5RokniaeVhy',
  'SPT':'FZ9xhZbkt9bKKVpWmFxRhEJyzgxqU5w5xu3mXcF6Eppe',
  'TLS':'AVHndcEDUjP9Liz5dfcvAPAMffADXG6KMPn8sWB1XhFQ',
  'ASF':'3J73XDv9QUsXJdWKD8J6YFk4XxPNx5hijqjyxdNJqaG9',
  'UWB':'J99HsFQEWKR3UiFQpKTnF11iaNiR1enf2LxHfgsbVc59',
  'AVE':'8yQzsbraXJFoPG5PdX73B8EVYFuPR9aC2axAqWearGKu',
  'PFP':'7JzaEAuVfjkrZyMwJgZF5aQkiEyVyCaTWA3N1fQK7Y6V',
  'MRDR':'BJiV2gCLwMvj2c1CbhnMjjy68RjqoMzYT8brDrpVyceA',
  'LOVE':'AM9sNDh48N2qhYSgpA58m9dHvrMoQongtyYu2u2XoYTc',
  'LOST':'73d9N7BbWVKBG6A2xwwwEHcxzPB26YzbMnRjue3DPzqs',
  'TCW':'DXPv2ZyMD6Y2mDenqYkAhkvGSjNahkuMkm4zv6DqB7RF',
  'HOSA':'5Erzgrw9pTjNWLeqHp2sChJq7smB7WXRQYw9wvkvA59t',
  'DOI':'AYXTVttPfhYmn3jryX5XbRjwPK2m9445mbN2iLyRD6nq',
  'OJ':'2z52mwzBPqA2jGf8jJhCQijHTJ1EUEscX5Mz718SBvmM',
  'PX4':'MTc1macY8G2v1MubFxDp4W8cooaSBUZvc2KqaCNwhQE',
  'CALG':'4TpCAobnJfGFRbZ8gAppS9aZBwEGG1k9tRVmx6FPUvUp',
  'OT':'2qU6MtkBS4NQhzx7FQyxS7qfsjU3ZdbLVyUFjea3KBV2',
  'CCH':'6ybME9qMbXgLs3PLWvEv8uyL2LWnUZUz7CYGE4m8kEFm',
  'FB':'BeqGJwPnRb3fJwhSrfhzgUYKqegUtGtvXajTYzEpgGYr',
  'VOP':'AT1AoPoFU8WZkW8AnpQsSpUDaRyymvLPvG7k2kGZSwZr',
  'OJJ':'2W6ff8LajAwekXVxDARGQ9QFNcRbxmJPE2p3eNsGR7Qu',
  'PX5':'3ySaxSspDCsEM53zRTfpyr9s9yfq9yNpZFXSEbvbadLf',
  'PX4NSB':'5YHdWMXtAvEuUDZgsXNxvHAAKwuNu9Dq4DDWU8268qqr',
  'FBPLSL':'5KcER4cXLToXSYMoykdXiGwXg97Hjs7FErKaoDYKq4y8',
  'OTSS':'Ac8niMjqfCWruVF493iW1LeFzyDMGCrdDrC64iLL4ecC',
  'VOPSUS':'3oK293Mgd6tbqXgUZp4tw48h9DHrxH5pig1MPLK73iLL',
  'OJWS':'BdivWfkYfE9XPVzpeeYZ6xZqv5p6jiYym5mU22Rfscd5',
  'TIGUSS':'3rZ9g6JC47U6aLMWEUhEAkSf7Kqt11FkPXvWM2ssLgM9',
  'CALGBS':'4RHxqBc76NAQxcT8DQJZN9i9s1BeRWGiKviHyWgsj8EV',
  'CCHSB':'9ZjTVYfw8ock5QZAiCwp13pz9ZPLRa7DtA29sJU9FQoH',
  'OJJSBB':'8YX24DNagnSHqFkTbts8NihegSceo3qZaeEWWTgJeDSa',
  'PX5SSP':'3u7Lm2uwxS8Prfd1HfZShpoYhCxXJZLi8YXem4b6FZFQ',
  'CLAIM1':'A4MZ55qsrBTerqWHMa5o36RpAXycNnoZKbQ4y1fkg3SA',
  'CLAIM4':'Gu9ZNcmd6wTKuLh88dVM64WZ3EZT5T6DfbUvu1hpc8Ju',
  'CLAIM5':'4AhBa7rJg1ryedTYyKdRmaFZMkotSEbjENRnAtBuaA3k',
  'CLAIM3':'AwmsP69aHDU9ZqUhX98xo9oX1GWCyoNaDmrSCqyZd98k',
  'CLAIM2':'4m18ExgKckX8eVty96yX7rfUtXs8AZN31iM6mVKDRdBp',
  'OSTD1':'3BS6iuWRYW1HCHZP1ftcur1TLNiEEhfhxXkmYpn816W4',
  'MSD1':'ErFFAyjV9iHAVJcA9AeBHAZrHNNJnjWDiZR4LvvkfACK',
  'MSPP1':'G4E59tNgqkPYWWebTKoDFEpwwtrvG7MFdLku3XX4SvB9',
  'FM-PLG':'6GgPHefcbFu78kE9GZg3z1A9eHAaXeCPMqdjhRscFDSL',
  'FM-NLDB':'ChJKLWcHQb4VXFmA9Q9gjLvRapkboUvWCfCfqmLsnhSw',
  'FM-PP':'Fjcdi6CJJqvy2aSknEodKheB1dYVD9rdtZMJNjFo8Fck',
  'FM-CO92':'3AuyQppPMFToedMm8Xf1x79CfDUzpzLDorHHQmf11Rzy',

}

const symbolsByPk = Object.assign(
  {},
  ...Object.entries(nativeMarketsV3).map(([a, b]) => ({ [b]: a }))
)

function collectMarketData(programId: string, markets: Record<string, string>) {
  Object.entries(markets).forEach((e) => {
    const [marketName, marketPk] = e
    const marketConfig = {
      clusterUrl,
      programId,
      marketName,
      marketPk,
    } as MarketConfig
    collectEventQueue(marketConfig, { host, port, password, db: 0 })
  })
}

collectMarketData(programIdV3, nativeMarketsV3)

const groupConfig = Config.ids().getGroup('mainnet', 'mainnet.1') as GroupConfig

async function collectPerpEventQueue(r: RedisConfig, m: PerpMarketConfig) {
  const connection = new Connection(
    'https://mango.rpcpool.com',
    'processed' as Commitment
  )

  const store = await createRedisStore(r, m.name)
  const mangoClient = new MangoClient(connection, groupConfig!.mangoProgramId)
  const mangoGroup = await mangoClient.getMangoGroup(groupConfig!.publicKey)
  const perpMarket = await mangoGroup.loadPerpMarket(
    connection,
    m.marketIndex,
    m.baseDecimals,
    m.quoteDecimals
  )

  async function fetchTrades(lastSeqNum?: BN): Promise<[Trade[], BN]> {
    const now = Date.now()

    const eventQueue = await perpMarket.loadEventQueue(connection)
    const events = eventQueue.eventsSince(lastSeqNum || new BN(0))

    const trades = events
      .map((e) => e.fill)
      .filter((e) => !!e)
      .map((e) => perpMarket.parseFillEvent(e))
      .map((e) => {
        return {
          price: e.price,
          side: e.takerSide === 'buy' ? TradeSide.Buy : TradeSide.Sell,
          size: e.quantity,
          ts: e.timestamp.toNumber() * 1000,
        }
      })

    return [trades, eventQueue.seqNum as any]
  }

  async function storeTrades(ts: Trade[]) {
    if (ts.length > 0) {
      console.log(m.name, ts.length)
      for (let i = 0; i < ts.length; i += 1) {
        await store.storeTrade(ts[i])
      }
    }
  }

  while (true) {
    try {
      const lastSeqNum = await store.loadNumber('LASTSEQ')
      const [trades, currentSeqNum] = await fetchTrades(new BN(lastSeqNum || 0))
      storeTrades(trades)
      store.storeNumber('LASTSEQ', currentSeqNum.toString() as any)
    } catch (err) {
      console.error(m.name, err.toString())
    }
    await sleep({
      Seconds: process.env.INTERVAL ? parseInt(process.env.INTERVAL) : 10,
    })
  }
}

groupConfig.perpMarkets.forEach((m) =>
  collectPerpEventQueue({ host, port, password, db: 0 }, m)
)

const max_conn = parseInt(process.env.REDIS_MAX_CONN || '') || 200
const redisConfig = { host, port, password, db: 0, max_conn }
const pool = new TedisPool(redisConfig)

const app = express()
app.use(cors())

app.get('/tv/config', async (req, res) => {
  const response = {
    supported_resolutions: Object.keys(resolutions),
    supports_group_request: false,
    supports_marks: false,
    supports_search: true,
    supports_timescale_marks: false,
  }
  res.set('Cache-control', 'public, max-age=360')
  res.send(response)
})

const priceScales: any = {
  'BTC/USDC': 1,
  'BTC-PERP': 1,

  'ETH/USDC': 10,
  'ETH-PERP': 10,

  'SOL/USDC': 1000,
  'SOL-PERP': 1000,

  'SRM/USDC': 1000,
  'SRM-PERP': 1000,

  'MNGO/USDC': 10000,
  'MNGO-PERP': 10000,

  'USDT/USDC': 10000,
  'USDT-PERP': 10000,
}

app.get('/tv/symbols', async (req, res) => {
  const symbol = req.query.symbol as string
  const response = {
    name: symbol,
    ticker: symbol,
    description: symbol,
    type: 'Spot',
    session: '24x7',
    exchange: 'Mango',
    listed_exchange: 'Mango',
    timezone: 'Etc/UTC',
    has_intraday: true,
    supported_resolutions: Object.keys(resolutions),
    minmov: 1,
    pricescale: priceScales[symbol] || 100,
  }
  res.set('Cache-control', 'public, max-age=360')
  res.send(response)
})

app.get('/tv/history', async (req, res) => {
  // parse
  const marketName = req.query.symbol as string
  const market =
    nativeMarketsV3[marketName] ||
    groupConfig.perpMarkets.find((m) => m.name === marketName)
  const resolution = resolutions[req.query.resolution as string] as number
  let from = parseInt(req.query.from as string) * 1000
  let to = parseInt(req.query.to as string) * 1000

  // validate
  const validSymbol = market != undefined
  const validResolution = resolution != undefined
  const validFrom = true || new Date(from).getFullYear() >= 2021
  if (!(validSymbol && validResolution && validFrom)) {
    const error = { s: 'error', validSymbol, validResolution, validFrom }
    console.error({ marketName, error })
    res.status(404).send(error)
    return
  }

  // respond
  try {
    const conn = await pool.getTedis()
    try {
      const store = new RedisStore(conn, marketName)

      // snap candle boundaries to exact hours
      from = Math.floor(from / resolution) * resolution
      to = Math.ceil(to / resolution) * resolution

      // ensure the candle is at least one period in length
      if (from == to) {
        to += resolution
      }
      const candles = await store.loadCandles(resolution, from, to)
      const response = {
        s: 'ok',
        t: candles.map((c) => c.start / 1000),
        c: candles.map((c) => c.close),
        o: candles.map((c) => c.open),
        h: candles.map((c) => c.high),
        l: candles.map((c) => c.low),
        v: candles.map((c) => c.volume),
      }
      res.set('Cache-control', 'public, max-age=1')
      res.send(response)
      return
    } finally {
      pool.putTedis(conn)
    }
  } catch (e) {
    console.error({ req, e })
    const error = { s: 'error' }
    res.status(500).send(error)
  }
})

app.get('/trades/address/:marketPk', async (req, res) => {
  // parse
  const marketPk = req.params.marketPk as string
  const marketName =
    symbolsByPk[marketPk] ||
    groupConfig.perpMarkets.find((m) => m.publicKey.toBase58() === marketPk)
      ?.name

  // validate
  const validPk = marketName != undefined
  if (!validPk) {
    const error = { s: 'error', validPk }
    console.error({ marketPk, error })
    res.status(404).send(error)
    return
  }

  // respond
  try {
    const conn = await pool.getTedis()
    try {
      const store = new RedisStore(conn, marketName)
      const trades = await store.loadRecentTrades()
      const response = {
        success: true,
        data: trades.map((t) => {
          return {
            market: marketName,
            marketAddress: marketPk,
            price: t.price,
            size: t.size,
            side: t.side == TradeSide.Buy ? 'buy' : 'sell',
            time: t.ts,
            orderId: '',
            feeCost: 0,
          }
        }),
      }
      res.set('Cache-control', 'public, max-age=5')
      res.send(response)
      return
    } finally {
      pool.putTedis(conn)
    }
  } catch (e) {
    console.error({ req, e })
    const error = { s: 'error' }
    res.status(500).send(error)
  }
})

const httpPort = parseInt(process.env.PORT || '5000')
app.listen(httpPort)
