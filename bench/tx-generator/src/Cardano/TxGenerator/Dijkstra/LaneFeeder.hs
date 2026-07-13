{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}

-- | Submit controlled Dijkstra transactions to a local proto-devnet.
--
-- This module is intentionally small and direct: it spends the first genesis
-- UTxO listed in @funds.json@, then creates a single dependent transaction
-- chain whose tx bodies carry explicit Dijkstra inclusion lanes.
module Cardano.TxGenerator.Dijkstra.LaneFeeder
  ( LaneFeederOptions (..)
  , runLaneFeeder
  )
where

import Cardano.Api
import Cardano.Benchmarking.GeneratorTx.SizedMetadata (mkMetadata)
import Cardano.Ledger.Address (AccountAddress (..), AccountId (..))
import Cardano.Ledger.Core (ppTxFeeFixedL)
import Lens.Micro ((^.))
import Cardano.TxGenerator.Dijkstra.ActorPolicy

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (link, wait, withAsync)
import Control.Concurrent.STM (
  TBQueue,
  TVar,
  atomically,
  isFullTBQueue,
  newTBQueueIO,
  newTVarIO,
  readTVar,
  retry,
  tryReadTBQueue,
  writeTBQueue,
  writeTVar,
 )
import Control.Monad (foldM, when)
import Data.IORef (newIORef, readIORef, writeIORef)
import Numeric.Natural (Natural)
import qualified Data.Aeson as Aeson
import Data.Bifunctor (first)
import Data.Function ((&))
import Data.List (isInfixOf, sortOn)
import qualified Data.Map.Strict as Map
import Data.Ord (Down (..))
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Set as Set
import Data.Word (Word32)
import System.Directory (doesFileExist, makeAbsolute)
import System.Environment (lookupEnv)
import System.Exit (die)
import System.FilePath (isRelative, takeDirectory, (</>))
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

-- | Runtime options for the Dijkstra lane feeder.
data LaneFeederOptions = LaneFeederOptions
  { lfoSocket :: !(Maybe FilePath)
    -- ^ Node local socket path, or 'Nothing' to read @CARDANO_NODE_SOCKET_PATH@.
  , lfoFundsFile :: !FilePath
    -- ^ Proto-devnet @funds.json@ file.
  , lfoNetworkMagic :: !Word32
    -- ^ Testnet network magic.
  , lfoFeeLovelace :: !Integer
    -- ^ Explicit lovelace fee paid by each generated transaction.
  , lfoMetadataBytes :: !Int
    -- ^ Optional metadata payload size to make transactions larger for pressure tests.
  , lfoOptimisticFirst :: !Int
    -- ^ Number of optimistic transactions submitted before each urgent tranche.
  , lfoUrgentAfter :: !Int
    -- ^ Number of urgent transactions submitted after each optimistic tranche.
  , lfoCycles :: !Int
    -- ^ Number of times to repeat the optimistic-then-urgent pattern.
  , lfoIndependentLanes :: !Bool
    -- ^ Use separate genesis funds for the optimistic and urgent chains.
  , lfoConflictMode :: !Bool
    -- ^ Spend one input with both lanes each cycle, so the optimistic tx is evicted.
  , lfoIndependentFunding :: !Bool
    -- ^ Split one genesis UTxO into a pool, then fund each tx independently (no chaining).
  , lfoFundIndex :: !Int
    -- ^ Which genesis fund entry to spend (single-lane chains pick their own).
  , lfoDelayMs :: !Int
    -- ^ Pause between submissions, in milliseconds (0 = as fast as the mempool drains).
  , lfoActorMode :: !Bool
    -- ^ Drive submissions with the actor decision policy instead of a fixed lane.
  , lfoQuotesFile :: !FilePath
    -- ^ JSON file the actor reads the live published quotes from (@{urgent,optimistic}@).
  , lfoActorConfig :: !FilePath
    -- ^ JSON file the actor reads its population + demand spread from, live.
  , lfoInitialTxIn :: !(Maybe String)
    -- ^ Start the spending chain from this UTxO (@txid#ix@) instead of the fund's
    -- genesis pseudo-input — lets a generator restart after its genesis UTxO is spent.
  , lfoInitialValue :: !(Maybe Integer)
    -- ^ Lovelace value of '--initial-txin' (required together with it).
  , lfoInitialTxIn2 :: !(Maybe String)
    -- ^ Same as '--initial-txin', for the SECOND fund (actor mode drives two
    -- chains: fund 0 optimistic, fund 1 urgent) — lets a restarted feeder
    -- re-anchor both chains on the funds' current UTxOs.
  , lfoInitialValue2 :: !(Maybe Integer)
    -- ^ Lovelace value of '--initial-txin-2' (required together with it).
  , lfoFanout :: !Int
    -- ^ Actor mode: parallel dependent chains per lane. Each lane's fund is
    -- fanned out into this many chain heads, so one broken chain costs 1/N of
    -- the lane's throughput instead of all of it.
  , lfoFeeRefundStakeKey :: !(Maybe FilePath)
    -- ^ Staking verification key whose account collects every sender's fee
    -- refund (the unused headroom between the bid and the charged quote).
    -- Setting it makes the ledger's fee split real: base stays in the fee pot,
    -- premium goes to the treasury, the refund flows back to this account.
    -- Absent, the full bid stays in the fee pot. The account must be
    -- REGISTERED (the devnet's genesis delegators are) — refunds owed to an
    -- unregistered account stay pending forever.
  }

data FundEntry = FundEntry
  { entrySigningKey :: !FilePath
  , entryValue :: !Coin
  }

instance Aeson.FromJSON FundEntry where
  parseJSON =
    Aeson.withObject "FundEntry" $ \o ->
      FundEntry
        <$> o Aeson..: "signing_key"
        <*> (Coin <$> o Aeson..: "value")

data SpendWitness
  = SpendGenesis !(SigningKey GenesisUTxOKey)
  | SpendPayment !(SigningKey PaymentKey)

data SpendableFund = SpendableFund
  { spendTxIn :: !TxIn
  , spendValue :: !Coin
  , spendPaymentKey :: !(SigningKey PaymentKey)
  , spendWitness :: !SpendWitness
  , spendFeeRefundAccount :: !(Maybe AccountAddress)
    -- ^ Account collecting this sender's fee refunds (bid − quote). Successor
    -- funds inherit it, so one flag at startup covers the whole chain.
  }

-- | Submit the configured optimistic/urgent Dijkstra transaction chain.
runLaneFeeder :: LaneFeederOptions -> IO ()
runLaneFeeder options@LaneFeederOptions{..} = do
  hSetBuffering stdout LineBuffering
  validateOptions options

  socketPath <- resolveSocket lfoSocket
  fundsFile <- makeAbsolute lfoFundsFile
  fundEntries <- either die pure =<< Aeson.eitherDecodeFileStrict' fundsFile
  selectedFundEntry <- case drop lfoFundIndex fundEntries of
    fundEntry : _ -> pure fundEntry
    [] -> die $ "No funds entry at --fund-index " <> show lfoFundIndex <> " in " <> fundsFile

  let networkId = Testnet (NetworkMagic lfoNetworkMagic)
      fee = Coin lfoFeeLovelace
      connectInfo =
        LocalNodeConnectInfo
          (CardanoModeParams (EpochSlots 21600))
          networkId
          (File socketPath)

  metadata <- either die pure (mkMetadata @DijkstraEra lfoMetadataBytes)
  if lfoActorMode
    then runActorLanes options connectInfo metadata fundsFile fundEntries
    else if lfoConflictMode
    then do
      initialFund <-
        overrideInitialFund options
          =<< loadInitialFund networkId lfoFeeRefundStakeKey (takeDirectory fundsFile) selectedFundEntry
      runConflictLanes options connectInfo fee metadata initialFund
    else if lfoIndependentFunding
    then do
      initialFund <-
        overrideInitialFund options
          =<< loadInitialFund networkId lfoFeeRefundStakeKey (takeDirectory fundsFile) selectedFundEntry
      runIndependentFunding options connectInfo fee metadata initialFund
    else if lfoIndependentLanes
    then runIndependentLanes options connectInfo fee metadata fundsFile fundEntries
    else do
      initialFund <- loadInitialFund networkId lfoFeeRefundStakeKey (takeDirectory fundsFile) selectedFundEntry
      let lanePlan =
            concat $
              replicate lfoCycles $
                replicate lfoOptimisticFirst Optimistic <> replicate lfoUrgentAfter Urgent
      putStrLn $
        "Submitting "
          <> show (length lanePlan)
          <> " dependent Dijkstra txs to "
          <> socketPath

      _ <- foldM (submitLaneTx connectInfo fee metadata lfoDelayMs) initialFund (zip [(1 :: Int)..] lanePlan)
      pure ()

runIndependentLanes ::
  LaneFeederOptions ->
  LocalNodeConnectInfo ->
  Coin ->
  TxMetadataInEra DijkstraEra ->
  FilePath ->
  [FundEntry] ->
  IO ()
runIndependentLanes
  LaneFeederOptions{lfoOptimisticFirst, lfoUrgentAfter, lfoCycles, lfoDelayMs, lfoFeeRefundStakeKey}
  connectInfo@LocalNodeConnectInfo{localNodeNetworkId}
  fee
  metadata
  fundsFile
  fundEntries = do
  (optimisticEntry, urgentEntry) <-
    case fundEntries of
      optimisticEntry : urgentEntry : _ ->
        pure (optimisticEntry, urgentEntry)
      _ ->
        die "--independent-lanes requires at least two entries in --funds"

  let fundsDir = takeDirectory fundsFile
      optimisticPlan = replicate (lfoCycles * lfoOptimisticFirst) Optimistic
      urgentPlan = replicate (lfoCycles * lfoUrgentAfter) Urgent

  optimisticFund <- loadInitialFund localNodeNetworkId lfoFeeRefundStakeKey fundsDir optimisticEntry
  urgentFund <- loadInitialFund localNodeNetworkId lfoFeeRefundStakeKey fundsDir urgentEntry

  putStrLn $
    "Submitting "
      <> show (length optimisticPlan)
      <> " independent optimistic Dijkstra txs followed by "
      <> show (length urgentPlan)
      <> " independent urgent Dijkstra txs"

  _ <- foldM (submitLaneTx connectInfo fee metadata lfoDelayMs) optimisticFund (zip [(1 :: Int)..] optimisticPlan)
  _ <-
    foldM
      (submitLaneTx connectInfo fee metadata lfoDelayMs)
      urgentFund
      (zip [(1 + length optimisticPlan :: Int)..] urgentPlan)
  pure ()

-- | Deterministically generate cross-lane conflicts. Each cycle spends ONE input
-- with both lanes: an urgent transaction (the RB applies it first, so it wins and
-- the next cycle chains from it) and an optimistic transaction spending the very
-- same input (which is therefore invalidated — AllInputsAreSpent — once the RB is
-- applied: a real mempool eviction). Losing is the point of the optimistic one, so
-- we submit it once and never retry.
runConflictLanes ::
  LaneFeederOptions ->
  LocalNodeConnectInfo ->
  Coin ->
  TxMetadataInEra DijkstraEra ->
  SpendableFund ->
  IO ()
runConflictLanes LaneFeederOptions{lfoCycles, lfoDelayMs} connectInfo fee metadata initialFund = do
  putStrLn $
    "Conflict mode: spending one input per cycle with both lanes for "
      <> show lfoCycles
      <> " cycles (first-come: the optimistic tx is admitted first and protected, "
      <> "the conflicting urgent tx is rejected)."
  _ <- foldM step initialFund [(1 :: Int) .. lfoCycles]
  pure ()
  where
    -- Submit the optimistic tx FIRST (it is admitted and holds the input), then the
    -- conflicting urgent tx SECOND (rejected at admission under the first-come policy).
    step fund index = do
      optimisticNext <- submitLaneTx connectInfo fee metadata lfoDelayMs fund (index, Optimistic)
      case buildLaneTx connectInfo fee metadata fund Urgent of
        Left err -> die $ "conflict tx " <> show index <> " (urgent) build failed: " <> err
        Right (txUrgent, _) -> submitConflictingUrgent connectInfo index txUrgent
      pure optimisticNext

-- | Submit the losing side of a cross-lane conflict: the urgent tx that arrives after
-- the optimistic one already holds the input. Once, no retry, logging whatever the node
-- reports (under the first-come policy this is a rejection at admission).
submitConflictingUrgent :: LocalNodeConnectInfo -> Int -> Tx DijkstraEra -> IO ()
submitConflictingUrgent connectInfo index tx = do
  result <- submitTxToNodeLocal connectInfo (TxInMode ShelleyBasedEraDijkstra tx)
  putStrLn $
    show index
      <> ": urgent "
      <> show (getTxId (getTxBody tx))
      <> " (conflicting) -> "
      <> case result of
        TxSubmitSuccess -> "accepted (unexpected — optimistic should have held the input)"
        TxSubmitFail err -> "rejected: " <> show err
        TxSubmitError err -> "error: " <> show err

-- | Independent-tx funding: split one genesis UTxO into a pool of independent UTxOs,
-- then fund each urgent transaction from its own pool entry. Nothing chains, so an
-- evicted tx never breaks its neighbours — and a large backlog can pile up until a
-- rising quote evicts the admitted txs whose bid it has overtaken (a real,
-- node-traced Mempool.RemoveTxs), which the chained feeder could never show.
runIndependentFunding ::
  LaneFeederOptions ->
  LocalNodeConnectInfo ->
  Coin ->
  TxMetadataInEra DijkstraEra ->
  SpendableFund ->
  IO ()
runIndependentFunding LaneFeederOptions{lfoCycles, lfoDelayMs} connectInfo bid metadata initialFund = do
  putStrLn $
    "Independent funding: "
      <> show lfoCycles
      <> " rounds of "
      <> show independentPoolSize
      <> " independent urgent txs, each from its own UTxO (bid "
      <> show bid
      <> ")."
  _ <- foldM runRound initialFund [(1 :: Int) .. lfoCycles]
  pure ()
  where
    runRound changeFund roundIx =
      case buildSplitTx connectInfo bid changeFund of
        Left err -> die $ "round " <> show roundIx <> " split failed: " <> err
        Right (splitTx, poolFunds, nextChange) -> do
          putStrLn $
            "round " <> show roundIx <> ": split into " <> show (length poolFunds) <> " UTxOs, waiting for it to settle"
          submitSplit splitTx
          threadDelay (independentSplitSettleMs * 1000)
          mapM_ submitDemand (zip [(1 :: Int) ..] poolFunds)
          pure nextChange

    submitSplit tx = do
      result <- submitTxToNodeLocal connectInfo (TxInMode ShelleyBasedEraDijkstra tx)
      case result of
        TxSubmitSuccess -> putStrLn $ "split accepted " <> show (getTxId (getTxBody tx))
        TxSubmitFail err -> die $ "split rejected: " <> show err
        TxSubmitError err -> die $ "split error: " <> show err

    -- One independent urgent tx per pool UTxO, submitted once (no retry): it is
    -- meant to sit in the mempool and be evicted if the quote climbs past its bid.
    submitDemand (index, fund) =
      case buildLaneTx connectInfo bid metadata fund Urgent of
        Left err -> putStrLn $ show index <> ": build failed: " <> err
        Right (tx, _) -> do
          result <- submitTxToNodeLocal connectInfo (TxInMode ShelleyBasedEraDijkstra tx)
          putStrLn $
            show index <> ": urgent " <> show (getTxId (getTxBody tx)) <> " -> " <> renderSubmit result
          pauseMs lfoDelayMs

    renderSubmit = \case
      TxSubmitSuccess -> "accepted (evicted if the quote climbs past the bid)"
      TxSubmitFail err -> "rejected: " <> show err
      TxSubmitError err -> "error: " <> show err

-- | Build the pool-splitting transaction: spend one fund into 'independentPoolSize'
-- equal outputs (each covering the bid plus headroom) plus one change output, all to
-- the same key. Returns the tx, the pool funds, and the change fund for the next round.
buildSplitTx ::
  LocalNodeConnectInfo ->
  Coin ->
  SpendableFund ->
  Either String (Tx DijkstraEra, [SpendableFund], SpendableFund)
buildSplitTx LocalNodeConnectInfo{localNodeNetworkId} (Coin bidLovelace) SpendableFund{spendTxIn, spendValue = Coin available, spendPaymentKey, spendWitness, spendFeeRefundAccount} = do
  let perTx = bidLovelace + independentPoolHeadroom
      Coin splitFee = independentSplitFee
      change = available - perTx * fromIntegral independentPoolSize - splitFee
  if change < 1000000
    then Left "insufficient funds to split the pool"
    else do
      let poolOutput = mkOutput localNodeNetworkId spendPaymentKey (Coin perTx)
          changeOutput = mkOutput localNodeNetworkId spendPaymentKey (Coin change)
      body <-
        first displayError $
          createTransactionBody ShelleyBasedEraDijkstra $
            defaultTxBodyContent ShelleyBasedEraDijkstra
              & setTxIns [(spendTxIn, BuildTxWith (KeyWitness KeyWitnessForSpending))]
              & setTxOuts (replicate independentPoolSize poolOutput <> [changeOutput])
              & setTxFee (TxFeeExplicit ShelleyBasedEraDijkstra independentSplitFee)
              & setTxValidityLowerBound TxValidityNoLowerBound
              & setTxValidityUpperBound (defaultTxValidityUpperBound ShelleyBasedEraDijkstra)
              & setTxMetadata TxMetadataNone
              -- Urgent (RB): the pool outputs must exist before the urgent demand txs
              -- spend them. The RB is applied before the EB, so an optimistic split
              -- would have its outputs created *after* the urgent demand is applied.
              & setTxInclusion (TxInclusion Urgent)
              & setTxFeeRefundAccount (feeRefundAccountFor spendFeeRefundAccount)
      let splitTxId = getTxId body
          tx = signShelleyTransaction ShelleyBasedEraDijkstra body [signingWitness spendWitness]
          poolFunds =
            [ SpendableFund (TxIn splitTxId (TxIx (fromIntegral i))) (Coin perTx) spendPaymentKey (SpendPayment spendPaymentKey) spendFeeRefundAccount
            | i <- [0 .. independentPoolSize - 1]
            ]
          nextChange =
            SpendableFund
              (TxIn splitTxId (TxIx (fromIntegral independentPoolSize)))
              (Coin change)
              spendPaymentKey
              (SpendPayment spendPaymentKey)
              spendFeeRefundAccount
      pure (tx, poolFunds, nextChange)

-- | Pool entries created per split round. Bounded so the multi-output split tx
-- stays under the protocol's max tx size (16384 bytes).
independentPoolSize :: Int
independentPoolSize = 220

-- | Lovelace each pool UTxO carries above the bid, so its change output stays above
-- the minimum UTxO value after the bid is paid.
independentPoolHeadroom :: Integer
independentPoolHeadroom = 2000000

-- | A generous flat fee for the large, multi-output split transaction.
independentSplitFee :: Coin
independentSplitFee = Coin 20000000

-- | How long to let a split settle on-chain before spending its pool outputs.
independentSplitSettleMs :: Int
independentSplitSettleMs = 45000

-- | Drive submissions with the actor decision policy. Each step samples a demand,
-- reads the live published quotes, and lets the actor buy the urgent or optimistic
-- lane (whichever keeps more value after its fee) or walk away. Urgent and
-- optimistic each chain from their own genesis fund, so a tx never crosses lanes.
runActorLanes ::
  LaneFeederOptions ->
  LocalNodeConnectInfo ->
  TxMetadataInEra DijkstraEra ->
  FilePath ->
  [FundEntry] ->
  IO ()
runActorLanes
  LaneFeederOptions{lfoMetadataBytes, lfoCycles, lfoDelayMs, lfoFeeLovelace, lfoQuotesFile, lfoActorConfig, lfoInitialTxIn, lfoInitialValue, lfoInitialTxIn2, lfoInitialValue2, lfoFanout, lfoFeeRefundStakeKey}
  connectInfo@LocalNodeConnectInfo{localNodeNetworkId}
  metadata
  fundsFile
  fundEntries = do
    (optimisticEntry, urgentEntry) <-
      case fundEntries of
        optimisticEntry : urgentEntry : _ -> pure (optimisticEntry, urgentEntry)
        _ -> die "actor mode requires at least two entries in --funds"
    let fundsDir = takeDirectory fundsFile
        -- The on-chain bid is a flat, generous cap (the --fee), not the actor's
        -- economic quote: our txs form a dependent chain, so an eviction (a rising
        -- quote overtaking a per-tx bid) would break the whole chain downstream.
        -- The elasticity in the demo comes from the actor walking away, not from
        -- mempool eviction.
    optimisticAnchor <-
      overrideFund lfoInitialTxIn lfoInitialValue
        =<< loadInitialFund localNodeNetworkId lfoFeeRefundStakeKey fundsDir optimisticEntry
    urgentAnchor <-
      overrideFund lfoInitialTxIn2 lfoInitialValue2
        =<< loadInitialFund localNodeNetworkId lfoFeeRefundStakeKey fundsDir urgentEntry
    optimisticChains <-
      provisionLane connectInfo lfoFanout lfoFeeLovelace "optimistic" optimisticAnchor
    urgentChains <-
      provisionLane connectInfo lfoFanout lfoFeeLovelace "urgent" urgentAnchor
    putStrLn $
      "Actor mode: up to "
        <> show lfoCycles
        <> " demands, quotes from "
        <> lfoQuotesFile
        <> ", policy from "
        <> lfoActorConfig
        <> ", "
        <> show (length (laneFunds optimisticChains))
        <> "+"
        <> show (length (laneFunds urgentChains))
        <> " chains"
    -- The two lanes run as INDEPENDENT submitters. Before, one loop decided a
    -- demand AND submitted it inline, so a lane whose pool was full — where
    -- 'submitWithBackpressure' waits for the next block and resubmits — froze
    -- the whole feeder: the other lane got no new traffic even with room to
    -- spare (head-of-line blocking across lanes). Now a single decision loop
    -- only decides (reading BOTH quotes, so the cross-lane substitution an
    -- actor makes when one lane gets dear is preserved) and hands the work to
    -- that lane's own bounded queue; one worker per lane drains its queue and
    -- submits at its own pace. A full lane backs up ITS queue only — the other
    -- lane keeps flowing.
    minFeeB <- queryMinFeeB connectInfo
    putStrLn $
      "realistic bids: quote x live fee buffer, capped at "
        <> show lfoFeeLovelace
        <> " lovelace (minFeeB=" <> show minFeeB <> ")"
    urgentQueue <- newTBQueueIO laneQueueCapacity
    optimisticQueue <- newTBQueueIO laneQueueCapacity
    done <- newTVarIO False
    -- Per-lane held-back latch: log only on a transition (flowing -> held and
    -- back), never per demand — at delayMs=0 a full lane is offered thousands of
    -- times a second, and logging each would flood the feeder log (which the
    -- aggregator tails).
    urgentHeld <- newIORef False
    optimisticHeld <- newIORef False
    let deliver queue heldRef laneName pace item = do
          enqueued <- offerToLane queue item
          wasHeld <- readIORef heldRef
          if enqueued
            then do
              when wasHeld $ do
                writeIORef heldRef False
                putStrLn $ "lane flowing again: " <> laneName
              pauseMs pace
            else do
              when (not wasHeld) $ do
                writeIORef heldRef True
                putStrLn $ "lane held-back: " <> laneName <> " (pool full, senders held back)"
              -- Back off so a full lane doesn't spin the decision loop; the
              -- other lane's queue is untouched, so it keeps flowing.
              pauseMs (max heldBackBackoffMs pace)
    let decisionLoop counter
          | counter > lfoCycles = pure ()
          | otherwise = do
              quotes <- readPublishedQuotes lfoQuotesFile
              config <- readActorConfig lfoActorConfig
              let actorType = sampleActorType config counter
                  -- The cockpit can fatten every payload live (storm scenarios):
                  -- more bytes per tx fills a lane's byte budget at the same rate.
                  payloadBytes = maybe lfoMetadataBytes id (cfgMetadataBytes config)
                  liveMetadata
                    | payloadBytes == lfoMetadataBytes = metadata
                    | otherwise = either (const metadata) id (mkMetadata @DijkstraEra payloadBytes)
                  demand = sampleDemand config (payloadBytes + actorTxOverheadBytes) counter
                  -- The cockpit can take the wheel on the lane split (nobody
                  -- walks away then); otherwise each actor decides economically.
                  choice = case cfgLaneMix config of
                    Just mix
                      | pseudoUnit 5 counter < mix -> BuyOptimistic
                      | otherwise -> BuyUrgent
                    Nothing -> decide actorType (policyOf config) quotes demand
                  -- the dashboard can re-pace the demand live via the config
                  pace = maybe lfoDelayMs id (cfgDelayMs config)
                  generation = cfgLabel config
                  -- Realistic bid: the chosen lane's live quote x the cockpit's
                  -- fee buffer, capped by the FEE ceiling. The estimate is the
                  -- ledger's own arithmetic — quote = minFeeB + rate x size,
                  -- exact because the published rate never sits below the
                  -- floor and the floor here IS minFeeA. Buffer <= 0 is the
                  -- cockpit's escape hatch back to flat-ceiling bids.
                  laneBid rate =
                    let quoteEstimate = minFeeB + rate * fromIntegral (demandSize demand)
                        buffered = ceiling (cfgFeeBuffer config * fromIntegral quoteEstimate :: Double)
                     in if cfgFeeBuffer config <= 0
                          then lfoFeeLovelace
                          else max quoteEstimate (min lfoFeeLovelace buffered)
                  urgentBid = laneBid (urgentQuote quotes)
                  optimisticBid = laneBid (optimisticQuote quotes)
              case choice of
                WalkAway -> do
                  logDecision counter actorType WalkAway demand quotes 0 generation
                  pauseMs (max actorWalkAwayPauseMs pace)
                BuyUrgent -> do
                  logDecision counter actorType BuyUrgent demand quotes urgentBid generation
                  deliver urgentQueue urgentHeld "urgent" pace (counter, liveMetadata, Coin urgentBid)
                BuyOptimistic -> do
                  logDecision counter actorType BuyOptimistic demand quotes optimisticBid generation
                  deliver optimisticQueue optimisticHeld "optimistic" pace (counter, liveMetadata, Coin optimisticBid)
              decisionLoop (counter + 1)
    -- Run both lane workers alongside the decision loop; when it finishes,
    -- signal 'done' so each worker drains its queue and exits.
    withAsync (laneWorker connectInfo Urgent urgentQueue done urgentChains) $ \urgentW ->
      withAsync (laneWorker connectInfo Optimistic optimisticQueue done optimisticChains) $ \optimisticW -> do
        -- A worker that dies (its lane lost all chains -> 'die') must bring the
        -- whole feeder down, so the demo supervisor sees it exit and
        -- re-provisions both lanes. Without linking, the dead worker's exception
        -- sits unobserved while the decision loop keeps enqueueing into a queue
        -- nobody drains — the feeder looks alive but submits nothing.
        link urgentW
        link optimisticW
        decisionLoop 1
        atomically $ writeTVar done True
        wait urgentW
        wait optimisticW

-- | One lane's outstanding submissions, bounded. When it is full the lane's
-- pool cannot keep up (its worker is waiting out a full mempool), so the sender
-- is held back — but ONLY on this lane; the other lane's queue is untouched.
laneQueueCapacity :: Natural
laneQueueCapacity = 256

-- | How long the decision loop backs off after finding a lane's queue full, so
-- a congested lane throttles instead of spinning (the other lane is untouched).
heldBackBackoffMs :: Int
heldBackBackoffMs = 40

-- | Try to enqueue one demand for a lane without ever blocking; returns whether
-- it was accepted. A full queue means the lane is congested (its pool is full
-- and the worker is backpressured) — the caller holds the sender back on THIS
-- lane only. The dashboard shows the lane as blocked from the pool's fullness.
offerToLane :: TBQueue (Int, TxMetadataInEra DijkstraEra, Coin) -> (Int, TxMetadataInEra DijkstraEra, Coin) -> IO Bool
offerToLane queue item =
  atomically $ do
    full <- isFullTBQueue queue
    if full
      then pure False
      else writeTBQueue queue item >> pure True

-- | Drain a lane's queue and submit each demand on the lane's own dependent
-- chains, threading the chain state. Submission keeps its backpressure retry
-- (a briefly-full pool clears on the next block), but a stall here now only
-- backs up THIS lane. Exits once the decision loop is done and the queue is dry.
laneWorker ::
  LocalNodeConnectInfo ->
  Inclusion ->
  TBQueue (Int, TxMetadataInEra DijkstraEra, Coin) ->
  TVar Bool ->
  LaneChains ->
  IO ()
laneWorker connectInfo inclusion queue done = go
 where
  go chains = do
    next <- atomically $ do
      item <- tryReadTBQueue queue
      case item of
        Just x -> pure (Just x)
        Nothing -> do
          finished <- readTVar done
          if finished then pure Nothing else retry
    case next of
      Nothing -> pure ()
      Just (index, liveMetadata, bid) -> do
        -- pace 0: the decision loop already sets the demand tempo, so the
        -- worker drains as fast as the pool accepts (retry waits still apply).
        chains' <- submitOnLane connectInfo bid liveMetadata 0 chains (index, inclusion)
        go chains'

-- | A lane's working set: several parallel dependent chains, spent round-robin.
-- One broken chain is dropped and costs 1/N of the throughput; the feeder only
-- dies (for the demo runner to re-provision) when a lane has no chain left.
data LaneChains = LaneChains
  { laneFunds :: ![SpendableFund]
  , laneCursor :: !Int
  }

-- | Submit one tx on the lane's next chain. A permanent rejection (the chain
-- head lost a first-come race, e.g. against a crashed feeder's in-flight txs)
-- drops that chain and the lane keeps going on the others.
submitOnLane
  :: LocalNodeConnectInfo
  -> Coin
  -> TxMetadataInEra DijkstraEra
  -> Int
  -> LaneChains
  -> (Int, Inclusion)
  -> IO LaneChains
submitOnLane connectInfo fee metadata delayMs LaneChains{laneFunds, laneCursor} (index, inclusion) = do
  let count = length laneFunds
      slot = laneCursor `mod` max 1 count
      fund = laneFunds !! slot
  outcome <- submitLaneTxCatch connectInfo fee metadata delayMs fund (index, inclusion)
  case outcome of
    Right next ->
      pure LaneChains{laneFunds = replaceAt slot next laneFunds, laneCursor = slot + 1}
    Left err -> do
      putStrLn $
        show index
          <> ": chain "
          <> show (slot + 1)
          <> "/"
          <> show count
          <> " dropped ("
          <> renderInclusion inclusion
          <> "): "
          <> err
      let rest = deleteAt slot laneFunds
      when (null rest) $
        die $ renderInclusion inclusion <> " lane lost all its chains — restart to re-provision"
      pure LaneChains{laneFunds = rest, laneCursor = slot}

replaceAt :: Int -> a -> [a] -> [a]
replaceAt i x xs = case splitAt i xs of
  (before, _ : after) -> before <> (x : after)
  _ -> xs

deleteAt :: Int -> [a] -> [a]
deleteAt i xs = case splitAt i xs of
  (before, _ : after) -> before <> after
  _ -> xs

-- | Build a lane's working set of parallel chains. Each attempt re-discovers
-- the UTxOs at the fund's address (a restarted feeder re-adopts the surviving
-- chain heads, so no working capital is ever stranded) and, if the lane has
-- fewer chains than requested, fans the largest one out. A failed fan-out is
-- retried from a FRESH discovery: right after a restart the adopted head may
-- still be contested by the previous feeder's in-flight txs (first-come), and
-- the head keeps moving until that backlog drains. Fan-out outputs are spent
-- straight from the mempool, so nothing waits to settle.
provisionLane
  :: LocalNodeConnectInfo
  -> Int
  -> Integer
  -> String
  -> SpendableFund
  -> IO LaneChains
provisionLane connectInfo fanout feeLovelace laneName anchor = attempt provisionAttempts
 where
  attempt :: Int -> IO LaneChains
  attempt attemptsLeft = do
    discovered <- discoverFunds connectInfo anchor
    let minChain = Coin (chainFloorFeeMultiple * feeLovelace)
        usable = take fanout (sortOn (Down . spendValue) (filter ((>= minChain) . spendValue) discovered))
    base <- case usable of
      [] -> do
        putStrLn $ laneName <> " lane: no spendable UTxO discovered, using the configured anchor"
        pure [anchor]
      _ -> do
        putStrLn $
          laneName
            <> " lane: adopted "
            <> show (length usable)
            <> " UTxO(s) already at the fund address"
        pure usable
    let missing = fanout - length base
        settled = pure LaneChains{laneFunds = base, laneCursor = 0}
    if missing <= 0
      then settled
      else case sortOn (Down . spendValue) base of
        [] -> settled
        biggest : rest ->
          case buildFanoutTx connectInfo (missing + 1) biggest of
            Left err -> do
              putStrLn $
                laneName <> " lane: fan-out skipped (" <> err <> "); running on " <> show (length base) <> " chain(s)"
              settled
            Right (splitTx, pieces) -> do
              result <- submitTxToNodeLocal connectInfo (TxInMode ShelleyBasedEraDijkstra splitTx)
              case result of
                TxSubmitSuccess -> do
                  putStrLn $
                    laneName
                      <> " lane: fanned the largest UTxO out into "
                      <> show (length pieces)
                      <> " chains ("
                      <> show (getTxId (getTxBody splitTx))
                      <> ")"
                  pure LaneChains{laneFunds = pieces <> rest, laneCursor = 0}
                failure
                  | attemptsLeft > 1 -> do
                      putStrLn $
                        laneName
                          <> " lane: fan-out attempt failed ("
                          <> renderSubmitFailure failure
                          <> "), re-discovering in "
                          <> show (provisionRetryDelayMs `div` 1000)
                          <> "s"
                      pauseMs provisionRetryDelayMs
                      attempt (attemptsLeft - 1)
                  | otherwise -> do
                      putStrLn $
                        laneName
                          <> " lane: fan-out gave up ("
                          <> renderSubmitFailure failure
                          <> "); running on "
                          <> show (length base)
                          <> " chain(s)"
                      settled

renderSubmitFailure :: TxSubmitResult -> String
renderSubmitFailure = \case
  TxSubmitSuccess -> "success"
  TxSubmitFail err -> "rejected: " <> show err
  TxSubmitError err -> "submit error: " <> show err

-- | How many discover-then-fan-out rounds a lane tries before settling for
-- whatever chains it has. Right after a restart the old feeder's in-flight
-- backlog can contest the head for a few blocks.
provisionAttempts :: Int
provisionAttempts = 10

-- | Pause between provisioning attempts, in milliseconds.
provisionRetryDelayMs :: Int
provisionRetryDelayMs = 5000

-- | A chain head must afford a good run of txs before it is worth adopting.
chainFloorFeeMultiple :: Integer
chainFloorFeeMultiple = 100

-- | Every UTxO currently at the fund's payment address, per the local node.
-- The genesis pseudo-input keeps the anchor's witness; everything else is a
-- plain payment-key spend.
discoverFunds :: LocalNodeConnectInfo -> SpendableFund -> IO [SpendableFund]
discoverFunds connectInfo@LocalNodeConnectInfo{localNodeNetworkId} anchor = do
  let credential =
        PaymentCredentialByKey $
          verificationKeyHash $
            getVerificationKey (spendPaymentKey anchor)
      address = AddressShelley (makeShelleyAddress localNodeNetworkId credential NoStakeAddress)
      query =
        QueryInEra $
          QueryInShelleyBasedEra ShelleyBasedEraDijkstra $
            QueryUTxO (QueryUTxOByAddress (Set.singleton address))
  result <- runExceptT (queryNodeLocalState connectInfo VolatileTip query)
  case result of
    Left failure -> do
      putStrLn $ "fund discovery failed (acquire): " <> show failure
      pure []
    Right (Left mismatch) -> do
      putStrLn $ "fund discovery failed (era): " <> show mismatch
      pure []
    Right (Right utxo) ->
      pure
        [ SpendableFund
            { spendTxIn = txIn
            , spendValue = txOutValueToLovelace value
            , spendPaymentKey = spendPaymentKey anchor
            , spendWitness =
                if txIn == spendTxIn anchor
                  then spendWitness anchor
                  else SpendPayment (spendPaymentKey anchor)
            , spendFeeRefundAccount = spendFeeRefundAccount anchor
            }
        | (txIn, TxOut _ value _ _) <- Map.toList (unUTxO utxo)
        ]

-- | Split a fund into @n@ equal chain heads at the fund's own address. Travels
-- urgent: the RB is applied first, so the heads exist for either lane's chains.
buildFanoutTx
  :: LocalNodeConnectInfo
  -> Int
  -> SpendableFund
  -> Either String (Tx DijkstraEra, [SpendableFund])
buildFanoutTx LocalNodeConnectInfo{localNodeNetworkId} n SpendableFund{spendTxIn, spendValue = Coin available, spendPaymentKey, spendWitness, spendFeeRefundAccount} = do
  let Coin splitFee = independentSplitFee
      per = (available - splitFee) `div` fromIntegral n
      -- value conservation is exact: the first head absorbs the division rest
      rest = (available - splitFee) - per * fromIntegral n
      headValues = Coin (per + rest) : replicate (n - 1) (Coin per)
  if per < 10000000
    then Left "fund too small to fan out"
    else do
      body <-
        first displayError $
          createTransactionBody ShelleyBasedEraDijkstra $
            defaultTxBodyContent ShelleyBasedEraDijkstra
              & setTxIns [(spendTxIn, BuildTxWith (KeyWitness KeyWitnessForSpending))]
              & setTxOuts (map (mkOutput localNodeNetworkId spendPaymentKey) headValues)
              & setTxFee (TxFeeExplicit ShelleyBasedEraDijkstra independentSplitFee)
              & setTxValidityLowerBound TxValidityNoLowerBound
              & setTxValidityUpperBound (defaultTxValidityUpperBound ShelleyBasedEraDijkstra)
              & setTxMetadata TxMetadataNone
              & setTxInclusion (TxInclusion Urgent)
              & setTxFeeRefundAccount (feeRefundAccountFor spendFeeRefundAccount)
      let splitTxId = getTxId body
          tx = signShelleyTransaction ShelleyBasedEraDijkstra body [signingWitness spendWitness]
          heads =
            [ SpendableFund (TxIn splitTxId (TxIx (fromIntegral i))) v spendPaymentKey (SpendPayment spendPaymentKey) spendFeeRefundAccount
            | (i, v) <- zip [(0 :: Int) ..] headValues
            ]
      pure (tx, heads)

-- | The actor population and demand spread, read live from a JSON file so it can
-- be changed without restarting the feeder. Missing fields fall back to
-- 'defaultActorConfig'.
data ActorConfig = ActorConfig
  { cfgHonest :: !Double
  , cfgPatient :: !Double
  , cfgImpatient :: !Double
  , cfgValueMinAda :: !Double
  , cfgValueMaxAda :: !Double
  , cfgUrgencyMin :: !Double
  , cfgUrgencyMax :: !Double
  , cfgUrgentLatency :: !Double
  , cfgOptimisticLatency :: !Double
  , cfgReservationMultiple :: !Double
  , cfgFeeBuffer :: !Double
  , cfgDelayMs :: !(Maybe Int)
    -- ^ Live override of the pause between demands — lets the dashboard pilot
    -- the demand rate without restarting the feeder ('Nothing' = the CLI value).
  , cfgLaneMix :: !(Maybe Double)
    -- ^ Live override of the lane choice: the share of demands sent optimistic
    -- (0 = all urgent, 1 = all optimistic). 'Nothing' = the actors decide
    -- economically; set, the cockpit has the wheel and nobody walks away.
  , cfgMetadataBytes :: !(Maybe Int)
    -- ^ Live override of the metadata payload size: fatter transactions fill a
    -- lane's byte budget faster at the same demand rate (storm scenarios).
    -- 'Nothing' = the CLI value.
  , cfgLabel :: !(Maybe String)
    -- ^ Cockpit command label ("generation"). Stamped on every decision log
    -- line so each tx can be tracked back to the command that caused it.
  }

defaultActorConfig :: ActorConfig
defaultActorConfig =
  ActorConfig
    { cfgHonest = 0.6
    , cfgPatient = 0.2
    , cfgImpatient = 0.2
    , cfgValueMinAda = 1
    , cfgValueMaxAda = 30
    , cfgUrgencyMin = 0.02
    , cfgUrgencyMax = 0.35
    , cfgUrgentLatency = 1
    , cfgOptimisticLatency = 4
    , cfgReservationMultiple = 1
    , cfgFeeBuffer = 1.2
    , cfgDelayMs = Nothing
    , cfgLaneMix = Nothing
    , cfgMetadataBytes = Nothing
    , cfgLabel = Nothing
    }

instance Aeson.FromJSON ActorConfig where
  parseJSON =
    Aeson.withObject "ActorConfig" $ \o ->
      ActorConfig
        <$> o Aeson..:? "honest" Aeson..!= cfgHonest defaultActorConfig
        <*> o Aeson..:? "patient" Aeson..!= cfgPatient defaultActorConfig
        <*> o Aeson..:? "impatient" Aeson..!= cfgImpatient defaultActorConfig
        <*> o Aeson..:? "valueMinAda" Aeson..!= cfgValueMinAda defaultActorConfig
        <*> o Aeson..:? "valueMaxAda" Aeson..!= cfgValueMaxAda defaultActorConfig
        <*> o Aeson..:? "urgencyMin" Aeson..!= cfgUrgencyMin defaultActorConfig
        <*> o Aeson..:? "urgencyMax" Aeson..!= cfgUrgencyMax defaultActorConfig
        <*> o Aeson..:? "urgentLatency" Aeson..!= cfgUrgentLatency defaultActorConfig
        <*> o Aeson..:? "optimisticLatency" Aeson..!= cfgOptimisticLatency defaultActorConfig
        <*> o Aeson..:? "reservationMultiple" Aeson..!= cfgReservationMultiple defaultActorConfig
        <*> o Aeson..:? "feeBuffer" Aeson..!= cfgFeeBuffer defaultActorConfig
        <*> o Aeson..:? "delayMs"
        <*> o Aeson..:? "laneMix"
        <*> o Aeson..:? "metadataBytes"
        <*> o Aeson..:? "label"

-- | Read the live actor config; fall back to the default when the file is missing
-- or mid-write.
readActorConfig :: FilePath -> IO ActorConfig
readActorConfig path = do
  present <- doesFileExist path
  if not present
    then pure defaultActorConfig
    else maybe defaultActorConfig id <$> Aeson.decodeFileStrict' path

-- | The decision parameters carried by a config.
policyOf :: ActorConfig -> ActorPolicy
policyOf config =
  ActorPolicy
    { urgentLatency = cfgUrgentLatency config
    , optimisticLatency = cfgOptimisticLatency config
    , reservationMultiple = cfgReservationMultiple config
    , feeBuffer = cfgFeeBuffer config
    }

-- | Pick an actor type from the (honest, patient, impatient) mix, deterministically.
sampleActorType :: ActorConfig -> Int -> ActorType
sampleActorType config counter
  | total <= 0 = Honest
  | p < cfgHonest config / total = Honest
  | p < (cfgHonest config + cfgPatient config) / total = Patient
  | otherwise = Impatient
 where
  total = cfgHonest config + cfgPatient config + cfgImpatient config
  p = pseudoUnit 3 counter

-- | Sample a demand from the configured spread, deterministically from the counter.
sampleDemand :: ActorConfig -> Int -> Int -> Demand
sampleDemand config sizeBytes counter =
  Demand
    { demandValue = round (lovelacePerAda * (cfgValueMinAda config + valueP * valueSpan))
    , demandUrgency = cfgUrgencyMin config + urgencyP * (cfgUrgencyMax config - cfgUrgencyMin config)
    , demandSize = sizeBytes
    }
 where
  lovelacePerAda = 1000000 :: Double
  valueSpan = max 0 (cfgValueMaxAda config - cfgValueMinAda config)
  valueP = pseudoUnit 1 counter
  urgencyP = pseudoUnit 2 counter

-- | A cheap deterministic pseudo-random number in [0,1) from a salt and counter.
pseudoUnit :: Int -> Int -> Double
pseudoUnit salt counter =
  fromIntegral remainder / fromIntegral modulus
 where
  modulus = 1000003 :: Int
  remainder = (counter * 2654435761 + salt * 40503 + 12345) `mod` modulus

-- | Read the live published quotes the tailer writes; fall back to the floor when
-- the file is missing or mid-write.
readPublishedQuotes :: FilePath -> IO PublishedQuotes
readPublishedQuotes path = do
  present <- doesFileExist path
  if not present
    then pure floorQuotes
    else do
      decoded <- Aeson.decodeFileStrict' path
      pure $ case decoded of
        Just (QuotesFile urgent optimistic) -> PublishedQuotes urgent optimistic
        Nothing -> floorQuotes

floorQuotes :: PublishedQuotes
floorQuotes = PublishedQuotes{urgentQuote = 704, optimisticQuote = 44}

-- | One line per actor decision, parseable by the dashboard aggregator. The
-- generation label ties the decision (and the accepted txid logged right after
-- it, under the same n=) back to the cockpit command that was active when the
-- tx was sent — that is what lets the dashboard track a command's lifecycle.
logDecision :: Int -> ActorType -> LaneChoice -> Demand -> PublishedQuotes -> Integer -> Maybe String -> IO ()
logDecision counter actorType choice demand quotes bid generation = do
  -- Wall-clock stamp: the aggregator prefers it over its own clock so a log
  -- replay (aggregator restart) reconstructs each generation's real time range.
  now <- getPOSIXTime
  putStrLn $
    "actor decision: n="
      <> show counter
      <> " type="
      <> renderType actorType
      <> " lane="
      <> renderChoice choice
      <> " value="
      <> show (demandValue demand)
      <> " urgency="
      <> show (demandUrgency demand)
      <> " size="
      <> show (demandSize demand)
      <> " quoteUrgent="
      <> show (urgentQuote quotes)
      <> " quoteOptimistic="
      <> show (optimisticQuote quotes)
      <> " bid="
      <> show bid
      <> maybe "" ((" gen=" <>) . sanitizeLabel) generation
      <> " t="
      <> show (floor now :: Integer)

-- | Keep the generation label single-token so the log line stays one-word-per-field.
sanitizeLabel :: String -> String
sanitizeLabel = map (\c -> if c == ' ' then '_' else c) . take 60

renderType :: ActorType -> String
renderType = \case
  Honest -> "honest"
  Patient -> "patient"
  Impatient -> "impatient"

renderChoice :: LaneChoice -> String
renderChoice = \case
  BuyUrgent -> "urgent"
  BuyOptimistic -> "optimistic"
  WalkAway -> "shed"

pauseMs :: Int -> IO ()
pauseMs ms = when (ms > 0) (threadDelay (ms * 1000))

-- | The published quotes the tailer publishes for the actor to read.
data QuotesFile = QuotesFile !Integer !Integer

instance Aeson.FromJSON QuotesFile where
  parseJSON =
    Aeson.withObject "QuotesFile" $ \o ->
      QuotesFile <$> o Aeson..: "urgent" <*> o Aeson..: "optimistic"

actorTxOverheadBytes :: Int
actorTxOverheadBytes = 300

actorWalkAwayPauseMs :: Int
actorWalkAwayPauseMs = 25

validateOptions :: LaneFeederOptions -> IO ()
validateOptions LaneFeederOptions{..} = do
  when (lfoFeeLovelace <= 0) $
    die "--fee must be positive"
  when (lfoMetadataBytes < 0) $
    die "--metadata-bytes must be non-negative"
  when (lfoOptimisticFirst < 0) $
    die "--optimistic-first must be non-negative"
  when (lfoUrgentAfter < 0) $
    die "--urgent-after must be non-negative"
  when (lfoCycles <= 0) $
    die "--cycles must be positive"
  when (lfoOptimisticFirst == 0 && lfoUrgentAfter == 0) $
    die "At least one of --optimistic-first or --urgent-after must be positive"
  when (lfoFanout < 1) $
    die "--fanout must be at least 1"

resolveSocket :: Maybe FilePath -> IO FilePath
resolveSocket = \case
  Just path -> pure path
  Nothing ->
    lookupEnv "CARDANO_NODE_SOCKET_PATH" >>= \case
      Just path -> pure path
      Nothing -> die "Missing --socket and CARDANO_NODE_SOCKET_PATH is not set"

loadInitialFund ::
  NetworkId -> Maybe FilePath -> FilePath -> FundEntry -> IO SpendableFund
loadInitialFund networkId feeRefundStakeKey fundsDir FundEntry{entrySigningKey, entryValue} = do
  feeRefundAccount <- traverse (readFeeRefundAccount networkId) feeRefundStakeKey
  let keyPath =
        if isRelative entrySigningKey
          then fundsDir </> entrySigningKey
          else entrySigningKey
  (paymentKey, genesisKey) <- readLaneSigningKey keyPath
  let txIn =
        genesisUTxOPseudoTxIn networkId $
          verificationKeyHash $
            getVerificationKey genesisKey
  pure $
    SpendableFund
      { spendTxIn = txIn
      , spendValue = entryValue
      , spendPaymentKey = paymentKey
      , spendWitness = SpendGenesis genesisKey
      , spendFeeRefundAccount = feeRefundAccount
      }

-- | Restart support: replace the fund's genesis pseudo-input with an explicit
-- UTxO (@--initial-txin@ / @--initial-value@). Change outputs land at the fund's
-- payment address, so a killed generator can be relaunched from whatever UTxO the
-- node currently holds there (key witness, not the genesis one).
overrideInitialFund :: LaneFeederOptions -> SpendableFund -> IO SpendableFund
overrideInitialFund LaneFeederOptions{lfoInitialTxIn, lfoInitialValue} =
  overrideFund lfoInitialTxIn lfoInitialValue

overrideFund :: Maybe String -> Maybe Integer -> SpendableFund -> IO SpendableFund
overrideFund initialTxIn initialValue fund =
  case (initialTxIn, initialValue) of
    (Nothing, Nothing) -> pure fund
    (Just txInText, Just lovelace) -> do
      txIn <-
        either (\err -> die $ "Bad --initial-txin: " <> err) pure $
          Aeson.eitherDecode (Aeson.encode txInText)
      pure
        fund
          { spendTxIn = txIn
          , spendValue = Coin lovelace
          , spendWitness = SpendPayment (spendPaymentKey fund)
          }
    _ -> die "--initial-txin and --initial-value must be given together"

-- | The protocol's fixed fee term, queried from the node once at startup.
-- Realistic bids price a transaction exactly as the ledger will:
-- quote = minFeeB + rate x size (the published rate never sits below the
-- floor, and the floor is minFeeA on this devnet).
queryMinFeeB :: LocalNodeConnectInfo -> IO Integer
queryMinFeeB connectInfo = do
  result <-
    runExceptT $
      queryNodeLocalState connectInfo VolatileTip $
        QueryInEra (QueryInShelleyBasedEra ShelleyBasedEraDijkstra QueryProtocolParameters)
  case result of
    Left failure -> die ("protocol-params query failed (acquire): " <> show failure)
    Right (Left mismatch) -> die ("protocol-params query failed (era): " <> show mismatch)
    Right (Right pp) ->
      let Coin fixedFee = pp ^. ppTxFeeFixedL
       in pure fixedFee

-- | The sender's refund account, as the tx-body field (absent = the ledger
-- keeps the full bid in the fee pot).
feeRefundAccountFor :: Maybe AccountAddress -> TxFeeRefundAccount DijkstraEra
feeRefundAccountFor = maybe TxFeeRefundAccountNone TxFeeRefundAccount

-- | Read the staking verification key whose account collects the fee refunds.
-- The devnet's genesis delegators are registered, so their refunds actually
-- flush; an unregistered account would leave them pending forever.
readFeeRefundAccount :: NetworkId -> FilePath -> IO AccountAddress
readFeeRefundAccount networkId path = do
  vkey <- either (die . show) pure =<< readStakeVKey
  pure $
    AccountAddress
      (toShelleyNetwork networkId)
      (AccountId (toShelleyStakeCredential (StakeCredentialByKey (verificationKeyHash vkey))))
 where
  readStakeVKey :: IO (Either (FileError TextEnvelopeError) (VerificationKey StakeKey))
  readStakeVKey = readFileTextEnvelope (File path)

readLaneSigningKey :: FilePath -> IO (SigningKey PaymentKey, SigningKey GenesisUTxOKey)
readLaneSigningKey keyPath = do
  result <-
    readFileTextEnvelopeAnyOf
      [ FromSomeType
          (AsSigningKey AsGenesisUTxOKey)
          (\genesisKey -> (castSigningKey genesisKey, genesisKey))
      , FromSomeType
          (AsSigningKey AsPaymentKey)
          (\paymentKey -> (paymentKey, castToGenesisUTxOKey paymentKey))
      ]
      (File keyPath)
  either (die . show) pure result

castToGenesisUTxOKey :: SigningKey PaymentKey -> SigningKey GenesisUTxOKey
castToGenesisUTxOKey (PaymentSigningKey skey) =
  GenesisUTxOSigningKey skey

submitLaneTx
  :: LocalNodeConnectInfo
  -> Coin
  -> TxMetadataInEra DijkstraEra
  -> Int
  -> SpendableFund
  -> (Int, Inclusion)
  -> IO SpendableFund
submitLaneTx connectInfo fee metadata delayMs fund step =
  submitLaneTxCatch connectInfo fee metadata delayMs fund step >>= either die pure

-- | Like 'submitLaneTx', but reports failure instead of exiting — the actor
-- mode drops the failed chain and keeps running on its other chains.
submitLaneTxCatch
  :: LocalNodeConnectInfo
  -> Coin
  -> TxMetadataInEra DijkstraEra
  -> Int
  -> SpendableFund
  -> (Int, Inclusion)
  -> IO (Either String SpendableFund)
submitLaneTxCatch connectInfo fee metadata delayMs fund (index, inclusion) =
  case buildLaneTx connectInfo fee metadata fund inclusion of
    Left err ->
      pure $ Left $ "Failed to build tx " <> show index <> ": " <> err
    Right (tx, nextFund) -> do
      outcome <- submitWithBackpressure submitRetryBudget tx
      case outcome of
        Left err -> pure (Left err)
        Right () -> do
          pace delayMs
          pure (Right nextFund)
  where
    -- Under sustained load the mempool briefly fills, or the published quote
    -- momentarily overtakes the flat fee. Both clear once the next block forges,
    -- so we wait and resubmit the very same transaction (the chain is fixed)
    -- rather than giving up. A finite budget still backstops a stuck chain.
    submitWithBackpressure attemptsLeft tx = do
      result <- submitTxToNodeLocal connectInfo (TxInMode ShelleyBasedEraDijkstra tx)
      case result of
        TxSubmitSuccess -> do
          putStrLn $
            show index
              <> ": accepted "
              <> renderInclusion inclusion
              <> " "
              <> show (getTxId (getTxBody tx))
          pure (Right ())
        TxSubmitFail err
          -- A spent input never comes back: the chain head lost a first-come
          -- race or was consumed on-chain. Fail fast so the caller can drop
          -- the chain instead of retrying for minutes.
          | isPermanentRejection (show err) ->
              pure $ Left $ show index <> ": rejected (permanent) " <> renderInclusion inclusion <> ": " <> show err
          | attemptsLeft > 0 -> retryAfterDrain attemptsLeft tx
          | otherwise ->
              pure $ Left $ show index <> ": rejected " <> renderInclusion inclusion <> ": " <> show err
        TxSubmitError err
          | attemptsLeft > 0 -> retryAfterDrain attemptsLeft tx
          | otherwise ->
              pure $ Left $ show index <> ": submit error " <> renderInclusion inclusion <> ": " <> show err

    retryAfterDrain attemptsLeft tx = do
      pace (max submitRetryDelayMs delayMs)
      submitWithBackpressure (attemptsLeft - 1) tx

    pace ms = when (ms > 0) (threadDelay (ms * 1000))

    isPermanentRejection rendered =
      any (`isInfixOf` rendered) ["AllInputsAreSpent", "BadInputsUTxO"]

-- | How many times a single transaction is resubmitted while the mempool drains
-- before the feeder gives up on it (at 'submitRetryDelayMs' apart).
submitRetryBudget :: Int
submitRetryBudget = 1200

-- | Minimum pause between resubmissions, in milliseconds.
submitRetryDelayMs :: Int
submitRetryDelayMs = 250

buildLaneTx
  :: LocalNodeConnectInfo
  -> Coin
  -> TxMetadataInEra DijkstraEra
  -> SpendableFund
  -> Inclusion
  -> Either String (Tx DijkstraEra, SpendableFund)
buildLaneTx LocalNodeConnectInfo{localNodeNetworkId} fee metadata SpendableFund{..} inclusion = do
  outputValue <- subtractFee spendValue fee
  body <-
    first displayError $
      createTransactionBody ShelleyBasedEraDijkstra $
        defaultTxBodyContent ShelleyBasedEraDijkstra
          & setTxIns [(spendTxIn, BuildTxWith (KeyWitness KeyWitnessForSpending))]
          & setTxOuts [mkOutput localNodeNetworkId spendPaymentKey outputValue]
          & setTxFee (TxFeeExplicit ShelleyBasedEraDijkstra fee)
          & setTxValidityLowerBound TxValidityNoLowerBound
          & setTxValidityUpperBound (defaultTxValidityUpperBound ShelleyBasedEraDijkstra)
          & setTxMetadata metadata
          & setTxInclusion (TxInclusion inclusion)
          & setTxFeeRefundAccount (feeRefundAccountFor spendFeeRefundAccount)

  let tx = signShelleyTransaction ShelleyBasedEraDijkstra body [signingWitness spendWitness]
      nextFund =
        SpendableFund
          { spendTxIn = TxIn (getTxId body) (TxIx 0)
          , spendValue = outputValue
          , spendPaymentKey = spendPaymentKey
          , spendWitness = SpendPayment spendPaymentKey
          , spendFeeRefundAccount = spendFeeRefundAccount
          }
  pure (tx, nextFund)

mkOutput :: NetworkId -> SigningKey PaymentKey -> Coin -> TxOut CtxTx DijkstraEra
mkOutput networkId paymentKey outputCoin =
  TxOut
    ( makeShelleyAddressInEra
        ShelleyBasedEraDijkstra
        networkId
        (PaymentCredentialByKey $ verificationKeyHash $ getVerificationKey paymentKey)
        NoStakeAddress
    )
    (lovelaceToTxOutValue ShelleyBasedEraDijkstra outputCoin)
    TxOutDatumNone
    ReferenceScriptNone

subtractFee :: Coin -> Coin -> Either String Coin
subtractFee (Coin available) (Coin fee)
  | available > fee = Right (Coin (available - fee))
  | otherwise = Left "insufficient funds for fee"

signingWitness :: SpendWitness -> ShelleyWitnessSigningKey
signingWitness = \case
  SpendGenesis key -> WitnessGenesisUTxOKey key
  SpendPayment key -> WitnessPaymentKey key

renderInclusion :: Inclusion -> String
renderInclusion = \case
  Optimistic -> "optimistic"
  Urgent -> "urgent"
