/**
 * Fallback static symbol metadata for the top ~100 crypto assets.
 * Used when CoinGecko API is unavailable or during initial load.
 *
 * Logo URLs use CoinGecko's CDN for reliable, free hosting.
 */

export interface SymbolMetaEntry {
  id: string;
  name: string;
  symbol: string;
  logoUrl: string;
  category: string;
}

const CG = "https://assets.coingecko.com/coins/images";

// Map: Binance symbol (without USDT) → metadata
const fallbackSymbolMeta: Record<string, SymbolMetaEntry> = {
  BTC: { id: "bitcoin", name: "Bitcoin", symbol: "BTC", logoUrl: `${CG}/1/small/bitcoin.png`, category: "crypto" },
  ETH: { id: "ethereum", name: "Ethereum", symbol: "ETH", logoUrl: `${CG}/279/small/ethereum.png`, category: "crypto" },
  BNB: { id: "binancecoin", name: "BNB", symbol: "BNB", logoUrl: `${CG}/825/small/bnb.png`, category: "crypto" },
  SOL: { id: "solana", name: "Solana", symbol: "SOL", logoUrl: `${CG}/4128/small/solana.png`, category: "crypto" },
  XRP: { id: "ripple", name: "XRP", symbol: "XRP", logoUrl: `${CG}/44/small/xrp-symbol-white-128.png`, category: "crypto" },
  DOGE: { id: "dogecoin", name: "Dogecoin", symbol: "DOGE", logoUrl: `${CG}/5/small/dogecoin.png`, category: "crypto" },
  ADA: { id: "cardano", name: "Cardano", symbol: "ADA", logoUrl: `${CG}/975/small/cardano.png`, category: "crypto" },
  AVAX: { id: "avalanche-2", name: "Avalanche", symbol: "AVAX", logoUrl: `${CG}/12559/small/Avalanche_Circle_RedWhite_Trans.png`, category: "crypto" },
  DOT: { id: "polkadot", name: "Polkadot", symbol: "DOT", logoUrl: `${CG}/12171/small/polkadot.png`, category: "crypto" },
  LINK: { id: "chainlink", name: "Chainlink", symbol: "LINK", logoUrl: `${CG}/877/small/chainlink-new-logo.png`, category: "crypto" },
  MATIC: { id: "matic-network", name: "Polygon", symbol: "MATIC", logoUrl: `${CG}/4713/small/polygon.png`, category: "crypto" },
  LTC: { id: "litecoin", name: "Litecoin", symbol: "LTC", logoUrl: `${CG}/2/small/litecoin.png`, category: "crypto" },
  SHIB: { id: "shiba-inu", name: "Shiba Inu", symbol: "SHIB", logoUrl: `${CG}/11939/small/shiba.png`, category: "crypto" },
  TRX: { id: "tron", name: "TRON", symbol: "TRX", logoUrl: `${CG}/1094/small/tron-logo.png`, category: "crypto" },
  UNI: { id: "uniswap", name: "Uniswap", symbol: "UNI", logoUrl: `${CG}/12504/small/uniswap-logo.png`, category: "crypto" },
  ATOM: { id: "cosmos", name: "Cosmos", symbol: "ATOM", logoUrl: `${CG}/1481/small/cosmos_hub.png`, category: "crypto" },
  ETC: { id: "ethereum-classic", name: "Ethereum Classic", symbol: "ETC", logoUrl: `${CG}/453/small/ethereum-classic-logo.png`, category: "crypto" },
  FIL: { id: "filecoin", name: "Filecoin", symbol: "FIL", logoUrl: `${CG}/12817/small/filecoin.png`, category: "crypto" },
  NEAR: { id: "near", name: "NEAR Protocol", symbol: "NEAR", logoUrl: `${CG}/10365/small/near.jpg`, category: "crypto" },
  APT: { id: "aptos", name: "Aptos", symbol: "APT", logoUrl: `${CG}/26455/small/aptos_round.png`, category: "crypto" },
  ICP: { id: "internet-computer", name: "Internet Computer", symbol: "ICP", logoUrl: `${CG}/14495/small/Internet_Computer_logo.png`, category: "crypto" },
  IMX: { id: "immutable-x", name: "Immutable", symbol: "IMX", logoUrl: `${CG}/17233/small/immutableX-symbol-BLK-RGB.png`, category: "crypto" },
  ARB: { id: "arbitrum", name: "Arbitrum", symbol: "ARB", logoUrl: `${CG}/16547/small/photo_2023-03-29_21.47.00.jpeg`, category: "crypto" },
  OP: { id: "optimism", name: "Optimism", symbol: "OP", logoUrl: `${CG}/25244/small/Optimism.png`, category: "crypto" },
  SUI: { id: "sui", name: "Sui", symbol: "SUI", logoUrl: `${CG}/26375/small/sui_asset.jpeg`, category: "crypto" },
  SEI: { id: "sei-network", name: "Sei", symbol: "SEI", logoUrl: `${CG}/28205/small/Sei_Logo_-_Transparent.png`, category: "crypto" },
  INJ: { id: "injective-protocol", name: "Injective", symbol: "INJ", logoUrl: `${CG}/12882/small/Secondary_Symbol.png`, category: "crypto" },
  PEPE: { id: "pepe", name: "Pepe", symbol: "PEPE", logoUrl: `${CG}/33566/small/pepe-token.jpeg`, category: "crypto" },
  WIF: { id: "dogwifcoin", name: "dogwifhat", symbol: "WIF", logoUrl: `${CG}/33566/small/dogwifhat.jpg`, category: "crypto" },
  FET: { id: "fetch-ai", name: "Fetch.ai", symbol: "FET", logoUrl: `${CG}/5681/small/Fetch.jpg`, category: "crypto" },
  RENDER: { id: "render-token", name: "Render", symbol: "RENDER", logoUrl: `${CG}/11636/small/rndr.png`, category: "crypto" },
  GRT: { id: "the-graph", name: "The Graph", symbol: "GRT", logoUrl: `${CG}/13397/small/Graph_Token.png`, category: "crypto" },
  ALGO: { id: "algorand", name: "Algorand", symbol: "ALGO", logoUrl: `${CG}/4380/small/download.png`, category: "crypto" },
  VET: { id: "vechain", name: "VeChain", symbol: "VET", logoUrl: `${CG}/1167/small/VeChain-Logo-768x725.png`, category: "crypto" },
  SAND: { id: "the-sandbox", name: "The Sandbox", symbol: "SAND", logoUrl: `${CG}/12129/small/sandbox_logo.jpg`, category: "crypto" },
  MANA: { id: "decentraland", name: "Decentraland", symbol: "MANA", logoUrl: `${CG}/878/small/decentraland-mana.png`, category: "crypto" },
  AAVE: { id: "aave", name: "Aave", symbol: "AAVE", logoUrl: `${CG}/12645/small/AAVE.png`, category: "crypto" },
  MKR: { id: "maker", name: "Maker", symbol: "MKR", logoUrl: `${CG}/1364/small/Mark_Maker.png`, category: "crypto" },
  SNX: { id: "havven", name: "Synthetix", symbol: "SNX", logoUrl: `${CG}/3406/small/SNX.png`, category: "crypto" },
  CRV: { id: "curve-dao-token", name: "Curve DAO", symbol: "CRV", logoUrl: `${CG}/12124/small/Curve.png`, category: "crypto" },
  COMP: { id: "compound-governance-token", name: "Compound", symbol: "COMP", logoUrl: `${CG}/10775/small/COMP.png`, category: "crypto" },
  EGLD: { id: "elrond-erd-2", name: "MultiversX", symbol: "EGLD", logoUrl: `${CG}/12335/small/egld-token-logo.png`, category: "crypto" },
  RUNE: { id: "thorchain", name: "THORChain", symbol: "RUNE", logoUrl: `${CG}/6595/small/Rune200x200.png`, category: "crypto" },
  XLM: { id: "stellar", name: "Stellar", symbol: "XLM", logoUrl: `${CG}/100/small/Stellar_symbol_black_RGB.png`, category: "crypto" },
  HBAR: { id: "hedera-hashgraph", name: "Hedera", symbol: "HBAR", logoUrl: `${CG}/3688/small/hbar.png`, category: "crypto" },
  ENS: { id: "ethereum-name-service", name: "ENS", symbol: "ENS", logoUrl: `${CG}/19785/small/acatchangelong.png`, category: "crypto" },
  DYDX: { id: "dydx", name: "dYdX", symbol: "DYDX", logoUrl: `${CG}/17500/small/dydx.png`, category: "crypto" },
  FTM: { id: "fantom", name: "Fantom", symbol: "FTM", logoUrl: `${CG}/4001/small/Fantom_round.png`, category: "crypto" },
  THETA: { id: "theta-token", name: "Theta", symbol: "THETA", logoUrl: `${CG}/2538/small/theta-token-logo.png`, category: "crypto" },
  AXS: { id: "axie-infinity", name: "Axie Infinity", symbol: "AXS", logoUrl: `${CG}/13029/small/axie_infinity_logo.png`, category: "crypto" },
  APE: { id: "apecoin", name: "ApeCoin", symbol: "APE", logoUrl: `${CG}/24383/small/apecoin.jpg`, category: "crypto" },
  FLOW: { id: "flow", name: "Flow", symbol: "FLOW", logoUrl: `${CG}/13446/small/5f6294c0c7a8cda55cb1c936_Flow_Wordmark.png`, category: "crypto" },
  CHZ: { id: "chiliz", name: "Chiliz", symbol: "CHZ", logoUrl: `${CG}/8834/small/CHZ_Token_updated.png`, category: "crypto" },
  LDO: { id: "lido-dao", name: "Lido DAO", symbol: "LDO", logoUrl: `${CG}/13573/small/Lido_DAO.png`, category: "crypto" },
  QNT: { id: "quant-network", name: "Quant", symbol: "QNT", logoUrl: `${CG}/3370/small/5ZOu7brX_400x400.jpg`, category: "crypto" },
  XTZ: { id: "tezos", name: "Tezos", symbol: "XTZ", logoUrl: `${CG}/976/small/Tezos-logo.png`, category: "crypto" },
  EOS: { id: "eos", name: "EOS", symbol: "EOS", logoUrl: `${CG}/738/small/eos-eos-logo.png`, category: "crypto" },
  IOTA: { id: "iota", name: "IOTA", symbol: "IOTA", logoUrl: `${CG}/692/small/IOTA_Swirl.png`, category: "crypto" },
  NEO: { id: "neo", name: "Neo", symbol: "NEO", logoUrl: `${CG}/480/small/NEO_512_512.png`, category: "crypto" },
  ZEC: { id: "zcash", name: "Zcash", symbol: "ZEC", logoUrl: `${CG}/486/small/circle-zcash-color.png`, category: "crypto" },
  DASH: { id: "dash", name: "Dash", symbol: "DASH", logoUrl: `${CG}/19/small/dash-logo.png`, category: "crypto" },
  CAKE: { id: "pancakeswap-token", name: "PancakeSwap", symbol: "CAKE", logoUrl: `${CG}/12632/small/pancakeswap-cake-logo.png`, category: "crypto" },
  ONE: { id: "harmony", name: "Harmony", symbol: "ONE", logoUrl: `${CG}/4344/small/Y88JAze.png`, category: "crypto" },
  ZIL: { id: "zilliqa", name: "Zilliqa", symbol: "ZIL", logoUrl: `${CG}/2687/small/Zilliqa-logo.png`, category: "crypto" },
  ROSE: { id: "oasis-network", name: "Oasis Network", symbol: "ROSE", logoUrl: `${CG}/13162/small/rose.png`, category: "crypto" },
  KAVA: { id: "kava", name: "Kava", symbol: "KAVA", logoUrl: `${CG}/9761/small/kava.png`, category: "crypto" },
  BAT: { id: "basic-attention-token", name: "Basic Attention Token", symbol: "BAT", logoUrl: `${CG}/677/small/basic-attention-token.png`, category: "crypto" },
  OCEAN: { id: "ocean-protocol", name: "Ocean Protocol", symbol: "OCEAN", logoUrl: `${CG}/3687/small/ocean-protocol-logo.jpg`, category: "crypto" },
  STORJ: { id: "storj", name: "Storj", symbol: "STORJ", logoUrl: `${CG}/943/small/STORJ.png`, category: "crypto" },
  CELO: { id: "celo", name: "Celo", symbol: "CELO", logoUrl: `${CG}/11090/small/InjsE3W.png`, category: "crypto" },
  "1INCH": { id: "1inch", name: "1inch", symbol: "1INCH", logoUrl: `${CG}/13469/small/1inch-token.png`, category: "crypto" },
  MASK: { id: "mask-network", name: "Mask Network", symbol: "MASK", logoUrl: `${CG}/14051/small/6YKRe22j_400x400.jpg`, category: "crypto" },
  SKL: { id: "skale", name: "SKALE", symbol: "SKL", logoUrl: `${CG}/13245/small/SKALE_token_300x300.png`, category: "crypto" },
  CELR: { id: "celer-network", name: "Celer Network", symbol: "CELR", logoUrl: `${CG}/4379/small/Celr.png`, category: "crypto" },
  IOTX: { id: "iotex", name: "IoTeX", symbol: "IOTX", logoUrl: `${CG}/3334/small/iotex-logo.png`, category: "crypto" },
  POL: { id: "matic-network", name: "Polygon", symbol: "POL", logoUrl: `${CG}/4713/small/polygon.png`, category: "crypto" },
  TIA: { id: "celestia", name: "Celestia", symbol: "TIA", logoUrl: `${CG}/31967/small/tia.jpg`, category: "crypto" },
  JUP: { id: "jupiter-exchange-solana", name: "Jupiter", symbol: "JUP", logoUrl: `${CG}/34188/small/jup.png`, category: "crypto" },
  STX: { id: "blockstack", name: "Stacks", symbol: "STX", logoUrl: `${CG}/2069/small/Stacks_logo_full.png`, category: "crypto" },
  WLD: { id: "worldcoin-wld", name: "Worldcoin", symbol: "WLD", logoUrl: `${CG}/31069/small/worldcoin.jpeg`, category: "crypto" },
  BONK: { id: "bonk", name: "Bonk", symbol: "BONK", logoUrl: `${CG}/28600/small/bonk.jpg`, category: "crypto" },
  PYTH: { id: "pyth-network", name: "Pyth Network", symbol: "PYTH", logoUrl: `${CG}/31924/small/pyth.png`, category: "crypto" },
  JTO: { id: "jito-governance-token", name: "Jito", symbol: "JTO", logoUrl: `${CG}/33228/small/jto.png`, category: "crypto" },
  ORDI: { id: "ordinals", name: "ORDI", symbol: "ORDI", logoUrl: `${CG}/30162/small/ordi.png`, category: "crypto" },
  PENDLE: { id: "pendle", name: "Pendle", symbol: "PENDLE", logoUrl: `${CG}/15069/small/Pendle_Logo_Normal-03.png`, category: "crypto" },
  W: { id: "wormhole", name: "Wormhole", symbol: "W", logoUrl: `${CG}/35087/small/wormhole_logo.png`, category: "crypto" },
  STRK: { id: "starknet", name: "Starknet", symbol: "STRK", logoUrl: `${CG}/26997/small/starknet.png`, category: "crypto" },
  TAO: { id: "bittensor", name: "Bittensor", symbol: "TAO", logoUrl: `${CG}/28452/small/ARUsPeNQ_400x400.jpg`, category: "crypto" },
  FLR: { id: "flare-networks", name: "Flare", symbol: "FLR", logoUrl: `${CG}/28624/small/FLR-icon200x200.png`, category: "crypto" },
  JASMY: { id: "jasmycoin", name: "JasmyCoin", symbol: "JASMY", logoUrl: `${CG}/13876/small/JASMY200x200.jpg`, category: "crypto" },
  SUPER: { id: "superfarm", name: "SuperVerse", symbol: "SUPER", logoUrl: `${CG}/14040/small/SuperVerse_Logo_256x256.png`, category: "crypto" },
  BLUR: { id: "blur", name: "Blur", symbol: "BLUR", logoUrl: `${CG}/28453/small/blur.png`, category: "crypto" },
};

export default fallbackSymbolMeta;
