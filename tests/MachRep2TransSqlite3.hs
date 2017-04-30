
-- It corresponds to model MachRep2 described in document 
-- Introduction to Discrete-Event Simulation and the SimPy Language
-- [http://heather.cs.ucdavis.edu/~matloff/156/PLN/DESimIntro.pdf]. 
-- SimPy is available on [http://simpy.sourceforge.net/].
--   
-- The model description is as follows.
--   
-- Two machines, but sometimes break down. Up time is exponentially 
-- distributed with mean 1.0, and repair time is exponentially distributed 
-- with mean 0.5. In this example, there is only one repairperson, so 
-- the two machines cannot be repaired simultaneously if they are down 
-- at the same time.
--
-- In addition to finding the long-run proportion of up time as in
-- model MachRep1, let’s also find the long-run proportion of the time 
-- that a given machine does not have immediate access to the repairperson 
-- when the machine breaks down. Output values should be about 0.6 and 0.67. 

import Control.Monad
import Control.Monad.Trans

import Database.HDBC
import Database.HDBC.Sqlite3

import Simulation.Aivika.Trans
import qualified Simulation.Aivika.Trans.Resource as R
import qualified Simulation.Aivika.Trans.Results.Transform as T
import Simulation.Aivika.Trans.Experiment
import Simulation.Aivika.Experiment.Entity
import Simulation.Aivika.Experiment.Entity.HDBC
import Simulation.Aivika.Experiment.Trans.Provider

import Simulation.Aivika.IO
import Simulation.Aivika.Experiment.IO.Provider

type DES = IO

meanUpTime = 1.0
meanRepairTime = 0.5

specs = Specs { spcStartTime = 0.0,
                spcStopTime = 1000.0,
                spcDT = 0.5,
                spcMethod = RungeKutta4,
                spcGeneratorType = SimpleGenerator }

description =
  "Model MachRep2. Two machines, but sometimes break down. Up time is exponentially " ++
  "distributed with mean 1.0, and repair time is exponentially distributed " ++ 
  "with mean 0.5. In this example, there is only one repairperson, so " ++
  "the two machines cannot be repaired simultaneously if they are down " ++
  "at the same time. "

experiment :: Experiment DES
experiment =
  defaultExperiment {
    experimentSpecs = specs,
    experimentRunCount = 3,
    experimentDescription = description }

x1 = resultByName "upTimeProp"
x2 = resultByName "immedProp"
r  = T.Resource $ resultByName "repairPerson"
qn = T.tr $ T.resourceQueueCountStats r

generators :: [ExperimentGenerator ExperimentProvider DES]
generators =
  [outputView $ defaultFinalTimingStatsView {
     finalTimingStatsKey = "final time-dependent statistics 1",
     finalTimingStatsSeries = qn },
   outputView $ defaultTimingStatsView {
     timingStatsKey = "time-dependent statistics 1",
     timingStatsSeries = qn }]

model :: Simulation DES (Results DES)
model =
  do -- number of times the machines have broken down
     nRep <- newRef 0 
     
     -- number of breakdowns in which the machine 
     -- started repair service right away
     nImmedRep <- newRef 0
     
     -- total up time for all machines
     totalUpTime <- newRef 0.0
     
     repairPerson <- runEventInStartTime $
                     R.newFCFSResource 1
     
     let machine :: Process DES ()
         machine =
           do upTime <-
                randomExponentialProcess meanUpTime
              liftEvent $
                modifyRef totalUpTime (+ upTime) 
              
              -- check the resource availability
              liftEvent $
                do modifyRef nRep (+ 1)
                   n <- R.resourceCount repairPerson
                   when (n == 1) $
                     modifyRef nImmedRep (+ 1)
                
              R.requestResource repairPerson
              repairTime <-
                randomExponentialProcess meanRepairTime
              R.releaseResource repairPerson
              
              machine

     runProcessInStartTime machine
     runProcessInStartTime machine

     let upTimeProp =
           do x <- readRef totalUpTime
              y <- liftDynamics time
              return $ x / (2 * y)

         immedProp :: Event DES Double
         immedProp =
           do n <- readRef nRep
              nImmed <- readRef nImmedRep
              return $
                fromIntegral nImmed /
                fromIntegral n

     return $
       results
       [resultSource
        "upTimeProp"
        "The long-run proportion of up time (~ 0.6)"
        upTimeProp,
        --
        resultSource
        "immedProp"
        "The proption of time of immediate access (~ 0.67)"
        immedProp,
        --
        resultSource
        "repairPerson"
        "The repair person"
        repairPerson]

executor = id

main =
  do conn  <- connectSqlite3 "test.db"
     agent <- newExperimentAgent conn
     aggregator <- newExperimentAggregator agent (experimentRunCount experiment)
     let provider = ExperimentProvider aggregator Nothing
     runExperiment executor experiment generators provider model
     disconnect conn
     