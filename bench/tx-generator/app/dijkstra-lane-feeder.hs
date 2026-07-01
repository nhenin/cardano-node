module Main (main) where

import Cardano.TxGenerator.Dijkstra.LaneFeeder

import Options.Applicative

main :: IO ()
main =
  execParser parserInfo >>= runLaneFeeder

parserInfo :: ParserInfo LaneFeederOptions
parserInfo =
  info
    (optionsParser <**> helper)
    ( fullDesc
        <> progDesc "Submit Dijkstra transactions with explicit urgent/optimistic inclusion lanes"
    )

optionsParser :: Parser LaneFeederOptions
optionsParser =
  LaneFeederOptions
    <$> optional
      ( strOption
          ( long "socket"
              <> metavar "PATH"
              <> help "Node local socket path. Defaults to CARDANO_NODE_SOCKET_PATH."
          )
      )
    <*> strOption
      ( long "funds"
          <> metavar "FILE"
          <> value "funds.json"
          <> showDefault
          <> help "proto-devnet funds.json file."
      )
    <*> option auto
      ( long "network-magic"
          <> metavar "N"
          <> value 164
          <> showDefault
          <> help "Testnet network magic."
      )
    <*> option auto
      ( long "fee"
          <> metavar "LOVELACE"
          <> value 1000000
          <> showDefault
          <> help "Explicit fee paid by each generated transaction."
      )
    <*> option auto
      ( long "metadata-bytes"
          <> metavar "N"
          <> value 0
          <> showDefault
          <> help "Optional transaction metadata payload size for pressure tests."
      )
    <*> option auto
      ( long "optimistic-first"
          <> metavar "N"
          <> value 20
          <> showDefault
          <> help "Optimistic transactions submitted before the urgent tranche in each cycle."
      )
    <*> option auto
      ( long "urgent-after"
          <> metavar "N"
          <> value 5
          <> showDefault
          <> help "Urgent transactions submitted after the optimistic tranche in each cycle."
      )
    <*> option auto
      ( long "cycles"
          <> metavar "N"
          <> value 1
          <> showDefault
          <> help "Repeat the optimistic-then-urgent submission pattern."
      )
    <*> switch
      ( long "independent-lanes"
          <> help "Use separate genesis funds for optimistic and urgent transaction chains."
      )
    <*> switch
      ( long "conflict-mode"
          <> help "Each cycle, spend one input with both lanes: the optimistic tx is submitted first and holds the input; the conflicting urgent tx is rejected at admission (AllInputsAreSpent) — first-come, a real traceable cross-lane conflict."
      )
    <*> switch
      ( long "independent-funding"
          <> help "Split one genesis UTxO into a pool, then fund each urgent tx independently (no chaining). A backlog can build until a rising quote evicts the admitted txs whose bid it overtakes (traceable Mempool.RemoveTxs)."
      )
    <*> option auto
      ( long "fund-index"
          <> metavar "N"
          <> value 0
          <> showDefault
          <> help "Which genesis fund entry to spend (0-based). Lets two feeders run on separate UTxOs."
      )
    <*> option auto
      ( long "delay-ms"
          <> metavar "N"
          <> value 0
          <> showDefault
          <> help "Pause between submissions in milliseconds (0 = as fast as the mempool drains)."
      )
    <*> switch
      ( long "actor-mode"
          <> help "Drive submissions with the actor decision policy (buy urgent/optimistic by retained value, or walk away) instead of a fixed lane."
      )
    <*> strOption
      ( long "quotes-file"
          <> metavar "FILE"
          <> value "latest-quotes.json"
          <> showDefault
          <> help "JSON file the actor reads live published quotes from ({urgent,optimistic})."
      )
    <*> strOption
      ( long "actor-config"
          <> metavar "FILE"
          <> value "actor-config.json"
          <> showDefault
          <> help "JSON file the actor reads its population + demand spread from, live."
      )
    <*> optional
      ( strOption
          ( long "initial-txin"
              <> metavar "TXID#IX"
              <> help "Start the spending chain from this UTxO instead of the fund's genesis pseudo-input (lets a generator restart after its genesis UTxO is spent)."
          )
      )
    <*> optional
      ( option auto
          ( long "initial-value"
              <> metavar "LOVELACE"
              <> help "Lovelace value of --initial-txin (required together with it)."
          )
      )
    <*> optional
      ( strOption
          ( long "initial-txin-2"
              <> metavar "TXID#IX"
              <> help "Same as --initial-txin, for the second fund (actor mode: fund 0 optimistic, fund 1 urgent)."
          )
      )
    <*> optional
      ( option auto
          ( long "initial-value-2"
              <> metavar "LOVELACE"
              <> help "Lovelace value of --initial-txin-2 (required together with it)."
          )
      )
