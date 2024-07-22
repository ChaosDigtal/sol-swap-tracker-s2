const http = require("http");
const util = require("util");
const bs58 = require("bs58");
const solanaWeb3 = require("@solana/web3.js");
const { Decimal } = require("decimal.js");
const { Client } = require("pg");
const axios = require("axios");
const WebSocket = require("ws");
const csv = require("csv-parse");
const fs = require("fs");
require("./logger");

const client = new Client({
  host: "trading.copaicjskl31.us-east-2.rds.amazonaws.com",
  database: "trading",
  user: "creative_dev",
  password: "4hXWW1%G$",
  port: 5000,
  ssl: {
    rejectUnauthorized: false, // Bypass certificate validation
  },
});

client.connect((err) => {
  if (err) {
    console.error("Connection error", err.stack);
  } else {
    console.log("Connected to the database");
  }
});

// Connect to Solana
const connection_helius = new solanaWeb3.Connection(
  "https://mainnet.helius-rpc.com/?api-key=35eb685f-3541-4c70-a396-7aa18696c965",
  {}
);

const programIds = [
  "DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1", // Orca Token Swap
  "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", // Orca
  "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4", // Jupiter Aggregator V6
  "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB", // Jupiter Aggregator V4
  "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", // Raydium CPMM
  "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // Raydium Liquidity Pool V4
  "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h", // Raydium liquidity pool AMM
  "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", // Raydium Concentrated Liquidity
  "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", // Meteora DLMM Program
  "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", // Meteora Pools Program
];

// Function to send a request to the WebSocket server

async function test() {
  const SOL2USD = await getTokenPrice(
    "So11111111111111111111111111111111111111112"
  );
  console.log(SOL2USD);
  //   const tx = await connection_helius.getParsedTransactions(
  //     [
  //       "ts4HUgjk4oH4uBFqkWGqCpf4PvbwMRkbCwD5TLSEaRbMVGYRStnmWH1mAVimA52R9fziyM3fCqp1Rstto8wgWme",
  //       "4QXtRHnWrVRCVzxuZN9b33h6Y18Bs6sbdsKqpn5QjnzK44kAvoGsB754o2mN5NwEfZ7Br2ukcWHuhY3bhpSNrnnb",
  //     ],
  //     { maxSupportedTransactionVersion: 0 }
  //   );
  //   var fs = require("fs");
  //   const jsonString = JSON.stringify(tx, null, 2);
  //   fs.writeFile("d.json", jsonString, (err) => {
  //     if (err) {
  //       console.error("Error writing to file", err);
  //     } else {
  //       console.log("Transaction saved to b.json");
  //     }
  //   });
}

//test();

var signatures_stack = [];
var token2symbol = new Map();

async function getTokenInfo(accountInfo, quantity) {
  const mint = accountInfo.mint;
  const decimals = accountInfo.decimals;
  const amount = new Decimal(quantity).dividedBy(new Decimal(10).pow(decimals));
  var symbol = "";
  //   if (token2symbol.has(mint)) {
  //     symbol = token2symbol.get(mint);
  //   } else {
  //     const response = JSON.stringify(
  //       (
  //         await axios.get(
  //           "https://s3.coinmarketcap.com/generated/core/crypto/cryptos.json"
  //         )
  //       ).data
  //     ).toLowerCase();
  //     var pattern = new RegExp(
  //       `\\[([^\\[]*?)","([^\\[]*?)"([^\\[]*?)\\[([^\\[]*?)${mint.toLowerCase()}`
  //     );
  //     var matches = response.match(pattern);
  //     if (matches) {
  //       symbol = matches[2];
  //       token2symbol.set(mint, symbol);
  //     }
  //   }
  return { id: mint, symbol, amount };
}

async function getTokenPrice(token_address) {
  try {
    // const response = await axios.get(`https://api.coingecko.com/api/v3/coins/ethereum/contract/${token_address}`);

    // return new Decimal(response.data.market_data.current_price.usd);
    const headers = {
      accept: "application/json, multipart/mixed",
      "accept-language": "en-US,en;q=0.9",
      authorization: "d57e4f7f224c8359f433c5882e1180726b1d48e4",
      "content-type": "application/json",
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    };
    const json_data = {
      query:
        '{\n  filterTokens(\n    filters: {\n      network: [1399811149]\n    }\n    limit: 200\n    tokens:["' +
        token_address +
        '"]\n  ) {\n    results {\n      change1\n      change4\n      change12\n      change24\n      createdAt\n      volume1\n      volume4\n      volume12\n      isScam\n      holders\n      liquidity\n      marketCap\n      priceUSD\n      volume24\n      pair {\n        token0Data{symbol}\n        token1Data{symbol}\n        address\n      }\n      exchanges {\n        address\n      }\n      token {\n        address\n        decimals\n        name\n        networkId\n        symbol\n        \n      }\n    }\n  }\n}',
    };

    const response = await axios.post(
      "https://graph.defined.fi/graphql",
      json_data,
      { headers }
    );
    return new Decimal(response.data.data.filterTokens.results[0].priceUSD);
  } catch (e) {
    console.error(e);
    return new Decimal(0);
  }
}

const safeNumber = (value) => {
  if (value.isNaN() || !value.isFinite()) {
    return new Decimal(0); // or new Decimal(null), depending on your database schema
  }
  const maxPrecision = 50;
  const maxScale = 18;
  const maxValue = new Decimal(
    "9.999999999999999999999999999999999999999999999999E+31"
  ); // Adjust based on precision and scale
  const minValue = maxValue.negated();

  if (value.greaterThan(maxValue)) {
    return maxValue;
  }
  if (value.lessThan(minValue)) {
    return minValue;
  }
  return value;
};

function addEdge(graph, A, B, ratio) {
  if (graph.has(A)) {
    graph.get(A).push({ id: B, ratio: ratio });
  } else {
    graph.set(A, [{ id: B, ratio: ratio }]);
  }
}

async function db_save_batch(events, SOL2USD) {
  const BATCH_SIZE = 100;

  const batches = [];
  for (let i = 0; i < events.length; i += BATCH_SIZE) {
    const batch = events.slice(i, i + BATCH_SIZE);
    batches.push(batch);
  }

  for (let i = 0; i < batches.length; ++i) {
    const batch = batches[i];
    const values = [];
    const placeholders = batch
      .map((_, i) => {
        const offset = i * 15;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${
          offset + 4
        }, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${
          offset + 9
        }, $${offset + 10}, $${offset + 11}, $${offset + 12}, $${
          offset + 13
        }, $${offset + 14}, $${offset + 15})`;
      })
      .join(",");
    for (let j = 0; j < batch.length; ++j) {
      const event = batch[j];
      values.push(
        event.slot,
        event.signature,
        event.wallet,
        event.token0.id.toLowerCase(),
        event.token0.symbol,
        safeNumber(event.token0.amount ?? new Decimal(0)).toString(),
        safeNumber(event.token0.value_in_usd ?? new Decimal(0)).toString(),
        safeNumber(
          event.token0.total_exchanged_usd ?? new Decimal(0)
        ).toString(),
        event.token1.id.toLowerCase(),
        event.token1.symbol,
        safeNumber(event.token1.amount ?? new Decimal(0)).toString(),
        safeNumber(event.token1.value_in_usd ?? new Decimal(0)).toString(),
        safeNumber(
          event.token1.total_exchanged_usd ?? new Decimal(0)
        ).toString(),
        safeNumber(SOL2USD ?? new Decimal(0)).toString(),
        event.created_at.toISOString()
      );
    }
    const query = `
    INSERT INTO sol_swap_events (
        block_number,
        transaction_hash,
        wallet_address,
        token0_id,
        token0_symbol,
        token0_amount,
        token0_value_in_usd,
        token0_total_exchanged_usd,
        token1_id,
        token1_symbol,
        token1_amount,
        token1_value_in_usd,
        token1_total_exchanged_usd,
        sol_usd_price,
        created_at
    ) VALUES ${placeholders}
        `;
    try {
      await client.query(query, values);
    } catch (err) {
      console.error("Error saving batch of events", err);
      fs.appendFile("./logs/error.txt", err + "\n", (err) => {
        if (err) {
          console.error("Error writing file", err);
        } else {
          console.log("File has been written successfully");
        }
      });
    }
  }
}

async function fillUSDAmount(swapEvents, SOL2USD) {
  if (swapEvents.length == 0) return;
  var graph = new Map();

  for (let i = 0; i < swapEvents.length; ++i) {
    const se = swapEvents[i];
    if (
      se.token0.id &&
      se.token1.id &&
      se.token0.amount &&
      se.token1.amount &&
      !se.token0.amount.isNaN() &&
      !se.token1.amount.isNaN() &&
      !se.token0.amount.isZero() &&
      !se.token1.amount.isZero()
    ) {
      addEdge(
        graph,
        se.token0.id.toLowerCase(),
        se.token1.id.toLowerCase(),
        se.token0.amount.dividedBy(se.token1.amount)
      );
      addEdge(
        graph,
        se.token1.id.toLowerCase(),
        se.token0.id.toLowerCase(),
        se.token1.amount.dividedBy(se.token0.amount)
      );
    }
  }

  const stack = [
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".toLowerCase(),
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".toLowerCase(),
    "So11111111111111111111111111111111111111112".toLowerCase(),
  ];

  var id2USD = new Map();

  id2USD.set(
    "So11111111111111111111111111111111111111112".toLowerCase(),
    SOL2USD
  );
  for (var i = 0; i < stack.length - 1; ++i) {
    id2USD.set(stack[i], new Decimal(1.0));
  }

  while (stack.length > 0) {
    const id = stack.pop();

    if (!graph.has(id)) continue;
    const rights = graph.get(id);
    for (let i = 0; i < rights.length; ++i) {
      const right = rights[i];
      if (!id2USD.has(right.id)) {
        id2USD.set(right.id, id2USD.get(id).times(right.ratio));
        stack.push(right.id);
      }
    }
  }
  for (var i = 0; i < swapEvents.length; ++i) {
    if (id2USD.has(swapEvents[i].token0.id.toLowerCase())) {
      swapEvents[i].token0.value_in_usd = id2USD.get(
        swapEvents[i].token0.id.toLowerCase()
      );
      swapEvents[i].token0.total_exchanged_usd = swapEvents[
        i
      ].token0.value_in_usd.times(swapEvents[i].token0.amount);
      if (id2USD.has(swapEvents[i].token1.id.toLowerCase())) {
        swapEvents[i].token1.value_in_usd = id2USD.get(
          swapEvents[i].token1.id.toLowerCase()
        );
        swapEvents[i].token1.total_exchanged_usd = swapEvents[
          i
        ].token1.value_in_usd.times(swapEvents[i].token1.amount);
      }
    } else {
      stack.push(swapEvents[i].token0.id.toLowerCase());
      const usdPrice = await getTokenPrice(swapEvents[i].token0.id);
      if (usdPrice == new Decimal(0)) {
        stack.pop();
        continue;
      }
      id2USD.set(swapEvents[i].token0.id.toLowerCase(), usdPrice);
      while (stack.length > 0) {
        const id = stack.pop();

        if (!graph.has(id)) continue;
        const rights = graph.get(id);
        for (let j = 0; j < rights.length; ++j) {
          const right = rights[j];
          if (!id2USD.has(right.id)) {
            id2USD.set(right.id, id2USD.get(id).times(right.ratio));
            stack.push(right.id);
          }
        }
      }
    }
    if (id2USD.has(swapEvents[i].token0.id.toLowerCase())) {
      swapEvents[i].token0.value_in_usd = id2USD.get(
        swapEvents[i].token0.id.toLowerCase()
      );
      swapEvents[i].token0.total_exchanged_usd = swapEvents[
        i
      ].token0.value_in_usd.times(swapEvents[i].token0.amount);
      if (id2USD.has(swapEvents[i].token1.id.toLowerCase())) {
        swapEvents[i].token1.value_in_usd = id2USD.get(
          swapEvents[i].token1.id.toLowerCase()
        );
        swapEvents[i].token1.total_exchanged_usd = swapEvents[
          i
        ].token1.value_in_usd.times(swapEvents[i].token1.amount);
      }
    }
  }

  await db_save_batch(swapEvents, SOL2USD);
}

// async function parseSignatures(signatures) {
//   const start_time = new Date();
//   console.log(start_time, signatures.length);
//   const txs = await connection_helius.getParsedTransactions(signatures, {
//     maxSupportedTransactionVersion: 0,
//   });

//   console.log(
//     `Tx info fetched in ${
//       (new Date().getTime() - start_time.getTime()) / 1000
//     } seconds`
//   );

//   var swapEvents = [];
//   const SOL2USD = await getTokenPrice(
//     "So11111111111111111111111111111111111111112"
//   );

//   console.log(
//     `SOL price fetched in ${
//       (new Date().getTime() - start_time.getTime()) / 1000
//     } seconds`
//   );

//   var cnt = 0;

//   for (let k = 0; k < txs.length; ++k) {
//     const tx = txs[k];
//     console.log(tx.transaction.signatures[0]);
//     var accountIndex2info = new Map();
//     for (let j = 0; j < tx.meta.postTokenBalances.length; ++j) {
//       const account = tx.meta.postTokenBalances[j];
//       accountIndex2info.set(account.accountIndex, account);
//     }
//     var account2info = new Map();
//     for (let idx = 0; idx < tx.transaction.message.accountKeys.length; ++idx) {
//       const account = tx.transaction.message.accountKeys[idx];
//       const info = accountIndex2info.get(idx);
//       if (info == null) continue;
//       account2info.set(account.pubkey.toString(), {
//         mint: info.mint,
//         decimals: info.uiTokenAmount.decimals,
//       });
//     }
//     // console.log(
//     //   `Account info constructed in ${
//     //     (new Date().getTime() - start_time.getTime()) / 1000
//     //   } seconds`
//     // );
//     //console.log(account2info);
//     for (let j = 0; j < tx.meta.innerInstructions.length; ++j) {
//       const innerInstruction = tx.meta.innerInstructions[j];
//       const swapInstructions = innerInstruction.instructions;
//       var left = null;
//       for (let i = 0; i < swapInstructions.length; i += 1) {
//         if (swapInstructions[i].accounts) {
//           left = null;
//           continue;
//         }
//         if (
//           swapInstructions[i].parsed.type != "transfer" &&
//           swapInstructions[i].parsed.type != "transferChecked"
//         )
//           continue;
//         if (swapInstructions[i].parsed.info.authority == undefined) continue;
//         if (left == null) {
//           left = swapInstructions[i];
//           continue;
//         }

//         var info = account2info.get(left.parsed.info.source);
//         if (info == null) {
//           info = account2info.get(left.parsed.info.destination);
//         }
//         var sellToken = await getTokenInfo(
//           info,
//           left.parsed.info.amount
//             ? left.parsed.info.amount
//             : left.parsed.info.tokenAmount.amount
//         );
//         var info = account2info.get(swapInstructions[i].parsed.info.source);
//         if (info == null) {
//           info = account2info.get(swapInstructions[i].parsed.info.destination);
//         }
//         var buyToken = await getTokenInfo(
//           info,
//           swapInstructions[i].parsed.info.amount
//             ? swapInstructions[i].parsed.info.amount
//             : swapInstructions[i].parsed.info.tokenAmount.amount
//         );
//         cnt += 2;
//         swapEvents.push({
//           token0: sellToken,
//           token1: buyToken,
//           slot: tx.slot,
//           signature: tx.transaction.signatures[0],
//           wallet: tx.transaction.message.accountKeys[0].pubkey.toString(),
//           created_at: tx.blockTime,
//         });
//         left = null;
//       }
//     }

//     // console.log(
//     //   `Tx finished in ${
//     //     (new Date().getTime() - start_time.getTime()) / 1000
//     //   } seconds`
//     // );
//   }

//   //   console.log("==============");
//   //   console.log(swapEvents);

//   await fillUSDAmount(swapEvents, SOL2USD);
//   //console.log(swapEvents);
//   console.log(
//     `Finished in ${
//       (new Date().getTime() - start_time.getTime()) / 1000
//     } seconds`
//   );
// }

// // Subscribe to logs
// connection_helius.onLogs(
//   "all",
//   async (logInfo) => {
//     if (logInfo.err != null) return;
//     const signature = logInfo.signature;
//     var isSwapSignature = false;
//     for (let i = 0; i < logInfo.logs.length; ++i) {
//       for (let j = 0; j < programIds.length; ++j) {
//         if (logInfo.logs[i].includes(programIds[j])) {
//           isSwapSignature = true;
//           break;
//         }
//       }
//     }
//     if (!isSwapSignature) return;
//     signatures_stack.push(signature);
//     if (signatures_stack.length >= 50 && first == 1) {
//       first = 1;
//       //console.log(new Date());
//       const signatures = signatures_stack.splice(0, 50);
//       setTimeout(() => parseSignatures(signatures), 30 * 1000);
//     }
//   },
//   "confirmed"
// );

async function parseTransaction(tx) {
  const start_time = new Date();
  console.log(start_time, tx.transaction.signatures[0]);

  const blockTime = new Date(start_time.getTime() - 3000);

  var swapEvents = [];
  const SOL2USD = await getTokenPrice(
    "So11111111111111111111111111111111111111112"
  );

  var cnt = 0;

  var accountIndex2info = new Map();
  for (let j = 0; j < tx.meta.postTokenBalances.length; ++j) {
    const account = tx.meta.postTokenBalances[j];
    accountIndex2info.set(account.accountIndex, account);
  }
  var account2info = new Map();
  for (let idx = 0; idx < tx.transaction.message.accountKeys.length; ++idx) {
    const account = tx.transaction.message.accountKeys[idx];
    const info = accountIndex2info.get(idx);
    if (info == null) continue;
    account2info.set(account.pubkey.toString(), {
      mint: info.mint,
      decimals: info.uiTokenAmount.decimals,
    });
  }
  // console.log(
  //   `Account info constructed in ${
  //     (new Date().getTime() - start_time.getTime()) / 1000
  //   } seconds`
  // );
  //console.log(account2info);
  for (let j = 0; j < tx.meta.innerInstructions.length; ++j) {
    const innerInstruction = tx.meta.innerInstructions[j];
    const swapInstructions = innerInstruction.instructions;
    var left = null;
    for (let i = 0; i < swapInstructions.length; i += 1) {
      if (swapInstructions[i].accounts) {
        left = null;
        continue;
      }
      if (
        swapInstructions[i].parsed.type != "transfer" &&
        swapInstructions[i].parsed.type != "transferChecked"
      )
        continue;
      if (swapInstructions[i].parsed.info.authority == undefined) continue;
      if (left == null) {
        left = swapInstructions[i];
        continue;
      }

      var info = account2info.get(left.parsed.info.source);
      if (info == null) {
        info = account2info.get(left.parsed.info.destination);
      }
      var sellToken = await getTokenInfo(
        info,
        left.parsed.info.amount
          ? left.parsed.info.amount
          : left.parsed.info.tokenAmount.amount
      );
      var info = account2info.get(swapInstructions[i].parsed.info.source);
      if (info == null) {
        info = account2info.get(swapInstructions[i].parsed.info.destination);
      }
      var buyToken = await getTokenInfo(
        info,
        swapInstructions[i].parsed.info.amount
          ? swapInstructions[i].parsed.info.amount
          : swapInstructions[i].parsed.info.tokenAmount.amount
      );
      cnt += 2;
      swapEvents.push({
        token0: sellToken,
        token1: buyToken,
        slot: tx.slot,
        signature: tx.transaction.signatures[0],
        wallet: tx.transaction.message.accountKeys[0].pubkey.toString(),
        created_at: blockTime,
      });
      left = null;
    }
  }

  await fillUSDAmount(swapEvents, SOL2USD);
  //console.log(swapEvents);
  console.log(
    `Finished in ${
      (new Date().getTime() - start_time.getTime()) / 1000
    } seconds`
  );
}

function sendRequest(ws, walletList) {
  //console.log(walletList);
  const request = {
    jsonrpc: "2.0",
    id: 420,
    method: "transactionSubscribe",
    params: [
      {
        accountInclude: walletList,
      },
      {
        commitment: "confirmed",
        encoding: "jsonParsed",
        transactionDetails: "full",
        showRewards: true,
        maxSupportedTransactionVersion: 0,
      },
    ],
  };
  ws.send(JSON.stringify(request));
}

// Function to send a ping to the WebSocket server
function startPing(ws) {
  setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
      console.log("Ping sent");
    }
  }, 30000); // Ping every 30 seconds
}

// Define WebSocket event handlers

function initializeWebSocket() {
  // Create a WebSocket connection
  const ws = new WebSocket(
    "wss://atlas-mainnet.helius-rpc.com/?api-key=07a1e8c0-d021-46db-a05a-d26f9eb8cd1c"
  );

  ws.on("open", async function open() {
    console.log("WebSocket is open");
    const filePath = "sol_wallets.csv";

    // Create a list to store the rows' data
    const dataList = [];

    // Create a read stream from the CSV file
    const readStream = fs.createReadStream(filePath);

    // Create a CSV parser
    const parser = csv.parse({ delimiter: ",", from_line: 2 });

    // Handle each row
    parser.on("readable", () => {
      let record;
      while ((record = parser.read()) !== null) {
        dataList.push(record[0]);
      }
    });

    // Handle the end of the file
    parser.on("end", () => {
      sendRequest(ws, dataList); // Send a request once the WebSocket is open
      startPing(ws); // Start sending pings
    });

    // Handle errors
    parser.on("error", (err) => {
      console.error(err.message);
    });

    readStream.pipe(parser);
  });

  ws.on("message", function incoming(data) {
    const messageStr = data.toString("utf8");
    try {
      const messageObj = JSON.parse(messageStr);
      if (!messageObj.params) return;
      const tx = messageObj.params.result.transaction;
      parseTransaction(tx);
    } catch (e) {
      console.error("Failed to parse JSON:", e);
    }
  });

  ws.on("error", function error(err) {
    console.error("WebSocket error:", err);
  });

  ws.on("close", function close() {
    console.log("WebSocket is closed");
    console.log("WebSocket is restarting in 5 seconds");
    setTimeout(initializeWebSocket, 5000);
  });
}

initializeWebSocket();

const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader("Content-Type", "text/plain");
  res.end("Hello, World!\n");
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}/`);
});
