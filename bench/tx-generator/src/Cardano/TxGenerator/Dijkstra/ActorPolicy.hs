-- | An approximation of how a transaction submitter ("actor") decides which
-- inclusion lane to buy, ported from Will Gould's mechanism-design simulator
-- (@tiered-pricing@, @abstract-sim-hs/src/Actor.hs@). The shape is his: an actor
-- weighs the value it still keeps after waiting for a lane against that lane's
-- fee, buys whichever lane keeps more (urgent if the retained value is better,
-- else optimistic), and walks away when neither lane leaves any surplus — so
-- demand is price-elastic and sheds as the published quotes climb.
--
-- This is the pure decision only; sampling demand and building the transaction
-- live in the feeder. It is deliberately small and total.
module Cardano.TxGenerator.Dijkstra.ActorPolicy
  ( PublishedQuotes (..)
  , Demand (..)
  , ActorPolicy (..)
  , ActorType (..)
  , LaneChoice (..)
  , decide
  , decideLane
  , bidFor
  , retainedValue
  ) where

-- | The two published inclusion quotes an actor reads, in lovelace per byte.
data PublishedQuotes = PublishedQuotes
  { urgentQuote :: !Integer
  , optimisticQuote :: !Integer
  }
  deriving (Eq, Show)

-- | A unit of demand an actor weighs: what the transaction is worth if it lands
-- now, how fast that worth decays while it waits, and how big it is.
data Demand = Demand
  { demandValue :: !Integer
  -- ^ Lovelace the transaction is worth to its submitter if included immediately.
  , demandUrgency :: !Double
  -- ^ Exponential decay rate of that worth per expected block of waiting.
  , demandSize :: !Int
  -- ^ Transaction size in bytes; multiplied by a quote to get the lane fee.
  }
  deriving (Eq, Show)

-- | An actor's fixed dispositions. @urgentLatency@/@optimisticLatency@ are the
-- expected blocks of wait on each lane (urgent rides the next regular block;
-- optimistic waits for its endorser block to be certified). @reservationMultiple@
-- raises the optimistic lane's effective fee so an honest actor only takes it
-- while comfortably in profit. @feeBuffer@ is how much over the quote it bids.
data ActorPolicy = ActorPolicy
  { urgentLatency :: !Double
  , optimisticLatency :: !Double
  , reservationMultiple :: !Double
  , feeBuffer :: !Double
  }
  deriving (Eq, Show)

-- | What the actor decides to buy — or that it walks away.
data LaneChoice = BuyUrgent | BuyOptimistic | WalkAway
  deriving (Eq, Show)

-- | Will's three fixed dispositions: Patient always takes the cheap optimistic
-- lane, Impatient always the fast urgent lane, Honest weighs the two by utility
-- (and is the only one that walks away — i.e. the price-elastic one).
data ActorType = Honest | Patient | Impatient
  deriving (Eq, Show)

-- | The lane an actor of the given type takes for a demand at the current quotes.
decide :: ActorType -> ActorPolicy -> PublishedQuotes -> Demand -> LaneChoice
decide actorType policy quotes demand =
  case actorType of
    Patient -> BuyOptimistic
    Impatient -> BuyUrgent
    Honest -> decideLane policy quotes demand

-- | Value still retained after waiting the given number of expected blocks: the
-- worth decays exponentially at the demand's urgency rate.
retainedValue :: Demand -> Double -> Double
retainedValue demand blocks =
  fromIntegral (demandValue demand) * exp (negate (demandUrgency demand) * max 0 blocks)

-- | The lane fee an actor must cover: the published quote times the size.
laneFee :: Integer -> Demand -> Double
laneFee quote demand = fromIntegral quote * fromIntegral (demandSize demand)

-- | Will's approximation of an honest actor's choice: buy whichever lane keeps
-- more value after its fee, provided that surplus is non-negative; otherwise walk
-- away. The urgent lane is cheaper to wait on (less decay) but dearer to buy; the
-- optimistic lane is the reverse, and is gated by the actor's reservation price.
decideLane :: ActorPolicy -> PublishedQuotes -> Demand -> LaneChoice
decideLane policy quotes demand
  | urgentSurplus > optimisticSurplus && urgentSurplus >= 0 = BuyUrgent
  | optimisticSurplus >= 0 = BuyOptimistic
  | urgentSurplus >= 0 = BuyUrgent
  | otherwise = WalkAway
 where
  urgentSurplus =
    retainedValue demand (urgentLatency policy)
      - laneFee (urgentQuote quotes) demand
  optimisticSurplus =
    retainedValue demand (optimisticLatency policy)
      - reservationMultiple policy * laneFee (optimisticQuote quotes) demand

-- | The lovelace fee the actor bids on a chosen lane: the lane fee plus its
-- buffer. 'WalkAway' has no bid.
bidFor :: ActorPolicy -> PublishedQuotes -> Demand -> LaneChoice -> Maybe Integer
bidFor policy quotes demand choice =
  case choice of
    BuyUrgent -> Just (bid (urgentQuote quotes))
    BuyOptimistic -> Just (bid (optimisticQuote quotes))
    WalkAway -> Nothing
 where
  bid quote = ceiling (feeBuffer policy * laneFee quote demand)
