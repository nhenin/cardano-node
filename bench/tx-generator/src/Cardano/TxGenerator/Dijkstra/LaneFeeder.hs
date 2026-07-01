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
import Cardano.TxGenerator.Dijkstra.ActorPolicy

import Control.Concurrent (threadDelay)
import Control.Monad (foldM, when)
import qualified Data.Aeson as Aeson
import Data.Bifunctor (first)
import Data.Function ((&))
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
          =<< loadInitialFund networkId (takeDirectory fundsFile) selectedFundEntry
      runConflictLanes options connectInfo fee metadata initialFund
    else if lfoIndependentFunding
    then do
      initialFund <-
        overrideInitialFund options
          =<< loadInitialFund networkId (takeDirectory fundsFile) selectedFundEntry
      runIndependentFunding options connectInfo fee metadata initialFund
    else if lfoIndependentLanes
    then runIndependentLanes options connectInfo fee metadata fundsFile fundEntries
    else do
      initialFund <- loadInitialFund networkId (takeDirectory fundsFile) selectedFundEntry
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
  LaneFeederOptions{lfoOptimisticFirst, lfoUrgentAfter, lfoCycles, lfoDelayMs}
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

  optimisticFund <- loadInitialFund localNodeNetworkId fundsDir optimisticEntry
  urgentFund <- loadInitialFund localNodeNetworkId fundsDir urgentEntry

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
buildSplitTx LocalNodeConnectInfo{localNodeNetworkId} (Coin bidLovelace) SpendableFund{spendTxIn, spendValue = Coin available, spendPaymentKey, spendWitness} = do
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
      let splitTxId = getTxId body
          tx = signShelleyTransaction ShelleyBasedEraDijkstra body [signingWitness spendWitness]
          poolFunds =
            [ SpendableFund (TxIn splitTxId (TxIx (fromIntegral i))) (Coin perTx) spendPaymentKey (SpendPayment spendPaymentKey)
            | i <- [0 .. independentPoolSize - 1]
            ]
          nextChange =
            SpendableFund
              (TxIn splitTxId (TxIx (fromIntegral independentPoolSize)))
              (Coin change)
              spendPaymentKey
              (SpendPayment spendPaymentKey)
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
  LaneFeederOptions{lfoMetadataBytes, lfoCycles, lfoDelayMs, lfoFeeLovelace, lfoQuotesFile, lfoActorConfig, lfoInitialTxIn, lfoInitialValue, lfoInitialTxIn2, lfoInitialValue2}
  connectInfo@LocalNodeConnectInfo{localNodeNetworkId}
  metadata
  fundsFile
  fundEntries = do
    (optimisticEntry, urgentEntry) <-
      case fundEntries of
        optimisticEntry : urgentEntry : _ -> pure (optimisticEntry, urgentEntry)
        _ -> die "actor mode requires at least two entries in --funds"
    let fundsDir = takeDirectory fundsFile
        demandSize = lfoMetadataBytes + actorTxOverheadBytes
        -- The on-chain bid is a flat, generous cap (the --fee), not the actor's
        -- economic quote: our txs form a dependent chain, so an eviction (a rising
        -- quote overtaking a per-tx bid) would break the whole chain downstream.
        -- The elasticity in the demo comes from the actor walking away, not from
        -- mempool eviction.
        bid = Coin lfoFeeLovelace
    optimisticFund0 <-
      overrideFund lfoInitialTxIn lfoInitialValue
        =<< loadInitialFund localNodeNetworkId fundsDir optimisticEntry
    urgentFund0 <-
      overrideFund lfoInitialTxIn2 lfoInitialValue2
        =<< loadInitialFund localNodeNetworkId fundsDir urgentEntry
    putStrLn $
      "Actor mode: up to "
        <> show lfoCycles
        <> " demands, quotes from "
        <> lfoQuotesFile
        <> ", policy from "
        <> lfoActorConfig
    let step optimisticFund urgentFund counter
          | counter > lfoCycles = pure ()
          | otherwise = do
              quotes <- readPublishedQuotes lfoQuotesFile
              config <- readActorConfig lfoActorConfig
              let actorType = sampleActorType config counter
                  demand = sampleDemand config demandSize counter
                  choice = decide actorType (policyOf config) quotes demand
              (optimisticFund', urgentFund') <-
                case choice of
                  WalkAway -> do
                    logDecision counter actorType WalkAway demand quotes 0
                    pauseMs (max actorWalkAwayPauseMs lfoDelayMs)
                    pure (optimisticFund, urgentFund)
                  BuyUrgent -> do
                    logDecision counter actorType BuyUrgent demand quotes lfoFeeLovelace
                    spent <- submitLaneTx connectInfo bid metadata lfoDelayMs urgentFund (counter, Urgent)
                    pure (optimisticFund, spent)
                  BuyOptimistic -> do
                    logDecision counter actorType BuyOptimistic demand quotes lfoFeeLovelace
                    spent <- submitLaneTx connectInfo bid metadata lfoDelayMs optimisticFund (counter, Optimistic)
                    pure (spent, urgentFund)
              step optimisticFund' urgentFund' (counter + 1)
    step optimisticFund0 urgentFund0 1

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

-- | One line per actor decision, parseable by the dashboard aggregator.
logDecision :: Int -> ActorType -> LaneChoice -> Demand -> PublishedQuotes -> Integer -> IO ()
logDecision counter actorType choice demand quotes bid =
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

resolveSocket :: Maybe FilePath -> IO FilePath
resolveSocket = \case
  Just path -> pure path
  Nothing ->
    lookupEnv "CARDANO_NODE_SOCKET_PATH" >>= \case
      Just path -> pure path
      Nothing -> die "Missing --socket and CARDANO_NODE_SOCKET_PATH is not set"

loadInitialFund :: NetworkId -> FilePath -> FundEntry -> IO SpendableFund
loadInitialFund networkId fundsDir FundEntry{entrySigningKey, entryValue} = do
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
submitLaneTx connectInfo fee metadata delayMs fund (index, inclusion) =
  case buildLaneTx connectInfo fee metadata fund inclusion of
    Left err ->
      die $ "Failed to build tx " <> show index <> ": " <> err
    Right (tx, nextFund) -> do
      submitWithBackpressure submitRetryBudget tx
      pace delayMs
      pure nextFund
  where
    -- Under sustained load the mempool briefly fills, or the published quote
    -- momentarily overtakes the flat fee. Both clear once the next block forges,
    -- so we wait and resubmit the very same transaction (the chain is fixed)
    -- rather than giving up. A finite budget still backstops a stuck chain.
    submitWithBackpressure attemptsLeft tx = do
      result <- submitTxToNodeLocal connectInfo (TxInMode ShelleyBasedEraDijkstra tx)
      case result of
        TxSubmitSuccess ->
          putStrLn $
            show index
              <> ": accepted "
              <> renderInclusion inclusion
              <> " "
              <> show (getTxId (getTxBody tx))
        TxSubmitFail err
          | attemptsLeft > 0 -> retryAfterDrain attemptsLeft tx
          | otherwise ->
              die $ show index <> ": rejected " <> renderInclusion inclusion <> ": " <> show err
        TxSubmitError err
          | attemptsLeft > 0 -> retryAfterDrain attemptsLeft tx
          | otherwise ->
              die $ show index <> ": submit error " <> renderInclusion inclusion <> ": " <> show err

    retryAfterDrain attemptsLeft tx = do
      pace (max submitRetryDelayMs delayMs)
      submitWithBackpressure (attemptsLeft - 1) tx

    pace ms = when (ms > 0) (threadDelay (ms * 1000))

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

  let tx = signShelleyTransaction ShelleyBasedEraDijkstra body [signingWitness spendWitness]
      nextFund =
        SpendableFund
          { spendTxIn = TxIn (getTxId body) (TxIx 0)
          , spendValue = outputValue
          , spendPaymentKey = spendPaymentKey
          , spendWitness = SpendPayment spendPaymentKey
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
