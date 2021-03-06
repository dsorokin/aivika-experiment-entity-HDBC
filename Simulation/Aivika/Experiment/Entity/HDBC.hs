
{-# LANGUAGE FlexibleInstances, UndecidableInstances, InstanceSigs #-}

-- |
-- Module     : Simulation.Aivika.Experiment.Entity.HDBC
-- Copyright  : Copyright (c) 2017, David Sorokin <david.sorokin@gmail.com>
-- License    : AllRightsReserved
-- Maintainer : David Sorokin <david.sorokin@gmail.com>
-- Stability  : experimental
-- Tested with: GHC 8.0.2
--
-- This module defines the HDBC database interface, where
-- 'IConnection' is an instance of 'ExperimentAgentConstructor'.
--

module Simulation.Aivika.Experiment.Entity.HDBC () where

import Control.Monad
import Control.Monad.Trans

import Database.HDBC

import Simulation.Aivika
import Simulation.Aivika.Experiment.Entity
import Simulation.Aivika.Experiment.Entity.Utils (divideBy)

-- | The batch insert size.
batchInsertSize :: Int
batchInsertSize = 100

-- | 'IConnection' allows constructing experiment agents.
instance IConnection c => ExperimentAgentConstructor c where

  newExperimentAgent :: c -> IO ExperimentAgent
  newExperimentAgent c =
    let a = ExperimentAgent {
          agentRetryCount = 5,
          agentRetryDelay = 100000,
          initialiseEntitySchema = initialiseHDBCEntitySchema c,
          updateExperimentEntity = updateHDBCExperimentEntity c,
          deleteExperimentEntity = deleteHDBCExperimentEntity c,
          tryWriteExperimentEntity = tryWriteHDBCExperimentEntity c,
          tryWriteSourceEntity = tryWriteHDBCSourceEntity c,
          tryWriteVarEntity = tryWriteHDBCVarEntity c,
          writeTimeSeriesEntity = writeHDBCTimeSeriesEntity c,
          writeLastValueEntities = writeHDBCLastValueEntities c,
          writeSamplingStatsEntity = writeHDBCSamplingStatsEntity c,
          writeFinalSamplingStatsEntities = writeHDBCFinalSamplingStatsEntities c,
          writeTimingStatsEntity = writeHDBCTimingStatsEntity c,
          writeFinalTimingStatsEntities = writeHDBCFinalTimingStatsEntities c,
          writeMultipleValueEntities = writeHDBCMultipleValueEntities c,
          writeDeviationEntity = writeHDBCDeviationEntity c,
          writeFinalDeviationEntities = writeHDBCFinalDeviationEntities c,
          readExperimentEntity = readHDBCExperimentEntity c,
          readExperimentEntities = readHDBCExperimentEntities c,
          readVarEntity = readHDBCVarEntity c,
          readVarEntityByName = readHDBCVarEntityByName c,
          readVarEntities = readHDBCVarEntities c,
          readSourceEntity = readHDBCSourceEntity c,
          readSourceEntityByKey = readHDBCSourceEntityByKey c,
          readSourceEntities = readHDBCSourceEntities c,
          readTimeSeriesEntities = readHDBCTimeSeriesEntities c,
          readLastValueEntities = readHDBCLastValueEntities c,
          readSamplingStatsEntities = readHDBCSamplingStatsEntities c,
          readFinalSamplingStatsEntities = readHDBCFinalSamplingStatsEntities c,
          readTimingStatsEntities = readHDBCTimingStatsEntities c,
          readFinalTimingStatsEntities = readHDBCFinalTimingStatsEntities c,
          readMultipleValueEntities = readHDBCMultipleValueEntities c,
          readDeviationEntities = readHDBCDeviationEntities c,
          readFinalDeviationEntities = readHDBCFinalDeviationEntities c }
    in return a

-- | A quick way to do a query. Similar to preparing, executing, and then fetching all rows.
hdbcQuery' :: IConnection c => c -> String -> [SqlValue] -> IO [[SqlValue]]
hdbcQuery' c queryString args =      
    do statement <- prepare c queryString  
       _ <- execute statement args
       rows <- fetchAllRows' statement
       finish statement
       return rows

-- | Implements 'initialiseEntitySchema'.
initialiseHDBCEntitySchema :: IConnection c => c -> IO ()
initialiseHDBCEntitySchema c =
  do initialiseExperimentEntity c
     initialiseVarEntity c
     initialiseSourceEntity c
     initialiseSourceVarEntity c
     initialiseDataEntity c
     initialiseMultipleDataEntity c
     initialiseValueDataItems c
     initialiseSamplingStatsDataItems c
     initialiseTimingStatsDataItems c

-- | Initialises the experiment entity.
initialiseExperimentEntity :: IConnection c => c -> IO ()
initialiseExperimentEntity c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createExperimentEntitySQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createExperimentEntityIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the experiment entity table.
createExperimentEntitySQL :: String
createExperimentEntitySQL =
  "CREATE TABLE experiments (\
   \  id BIGINT UNIQUE NOT NULL, \
   \  title VARCHAR(64) NOT NULL, \
   \  description VARCHAR(4096) NOT NULL, \
   \  starttime DOUBLE PRECISION NOT NULL, \
   \  stoptime DOUBLE PRECISION NOT NULL, \
   \  dt DOUBLE PRECISION NOT NULL, \
   \  integ_method INTEGER NOT NULL, \
   \  run_count INTEGER NOT NULL, \
   \  real_starttime VARCHAR(32) NOT NULL, \
   \  completed BOOLEAN NOT NULL, \
   \  error_message VARCHAR(4096), \
   \  PRIMARY KEY(id) \
   \)"

-- | Take the experiment entity title.
takeExperimentEntityTitle :: ExperimentEntity -> String
takeExperimentEntityTitle = take 64 . experimentEntityTitle 

-- | Take the experiment entity description.
takeExperimentEntityDescription :: ExperimentEntity -> String
takeExperimentEntityDescription = take 4096 . experimentEntityDescription

-- | Take the experiment entity real time.
takeExperimentEntityRealStartTime :: ExperimentEntity -> String
takeExperimentEntityRealStartTime = take 32 . experimentEntityRealStartTime

-- | Take the experiment entity error message.
takeExperimentEntityErrorMessage :: ExperimentEntity -> Maybe String
takeExperimentEntityErrorMessage = fmap (take 4096) . experimentEntityErrorMessage

-- | Return an SQL stament for creating the experiment entity indices.
createExperimentEntityIndexSQL :: [String]
createExperimentEntityIndexSQL =
  ["CREATE INDEX experiment_by_id ON experiments(id)",
   "CREATE INDEX experiment_by_real_starttime ON experiments(real_starttime)"]

-- | Implements 'tryWriteExperimentEntity'.
tryWriteHDBCExperimentEntity :: IConnection c => c -> ExperimentEntity -> IO Bool
tryWriteHDBCExperimentEntity c e =
  do n <- handleSql (const $ return 0) $
          withTransaction c $ \c ->
          do n <- run c insertExperimentEntitySQL
                  [toSql $ experimentEntityId e,
                   toSql $ takeExperimentEntityTitle e,
                   toSql $ takeExperimentEntityDescription e,
                   toSql $ experimentEntityStartTime e,
                   toSql $ experimentEntityStopTime e,
                   toSql $ experimentEntityDT e,
                   toSql $ experimentIntegMethodToInt $ experimentEntityIntegMethod e,
                   toSql $ experimentEntityRunCount e,
                   toSql $ takeExperimentEntityRealStartTime e,
                   toSql $ experimentEntityCompleted e,
                   toSql $ takeExperimentEntityErrorMessage e]
             return n
     return (n > 0)

-- | Return an SQL statement for inserting the experiment entity.
insertExperimentEntitySQL :: String
insertExperimentEntitySQL =
  "INSERT INTO experiments (id, title, description, starttime, stoptime, dt, integ_method, run_count, real_starttime, completed, error_message) \
  \ VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

-- | Return an SQL statement for reading the experiment entity.
selectExperimentEntitySQL :: String
selectExperimentEntitySQL =
  "SELECT id, title, description, starttime, stoptime, dt, integ_method, run_count, real_starttime, completed, error_message \
  \ FROM experiments WHERE id = ?"

-- | Return an SQL statement for reading the experiment entities.
selectExperimentEntitiesSQL :: String
selectExperimentEntitiesSQL =
  "SELECT id, title, description, starttime, stoptime, dt, integ_method, run_count, real_starttime, completed, error_message \
  \ FROM experiments ORDER BY real_starttime"

-- | Return an SQL statement for updating the experiment entity.
updateExperimentEntitySQL :: String
updateExperimentEntitySQL =
  "UPDATE experiments SET title = ?, description = ?, starttime = ?, stoptime = ?, dt = ?, \
  \ integ_method = ?, run_count = ?, real_starttime = ?, completed = ?, error_message = ? \
  \ WHERE id = ?"

-- | Implements 'readExperimentEntity'.
readHDBCExperimentEntity :: IConnection c => c -> ExperimentUUID -> IO (Maybe ExperimentEntity)
readHDBCExperimentEntity c expId =
  do rs <- handleSqlError $
           hdbcQuery' c selectExperimentEntitySQL [toSql expId]
     case rs of
       [] -> return Nothing
       [[expId, title, description, starttime, stoptime, dt,
         integMethod, runCount, realStartTime, completed, errorMessage]] ->
         let e = ExperimentEntity { experimentEntityId = fromSql expId,
                                    experimentEntityTitle = fromSql title,
                                    experimentEntityDescription = fromSql description,
                                    experimentEntityStartTime = fromSql starttime,
                                    experimentEntityStopTime = fromSql stoptime,
                                    experimentEntityDT = fromSql dt,
                                    experimentEntityIntegMethod =
                                      experimentIntegMethodFromInt (fromSql integMethod),
                                    experimentEntityRunCount = fromSql runCount,
                                    experimentEntityRealStartTime = fromSql realStartTime,
                                    experimentEntityCompleted = fromSql completed,
                                    experimentEntityErrorMessage = fromSql errorMessage }
         in return (Just e)

-- | Implements 'readExperimentEntities'.
readHDBCExperimentEntities :: IConnection c => c -> IO [ExperimentEntity]
readHDBCExperimentEntities c =
  do rs <- handleSqlError $
           hdbcQuery' c selectExperimentEntitiesSQL []
     forM rs $ \[expId, title, description, starttime, stoptime, dt,
                 integMethod, runCount, realStartTime, completed, errorMessage] ->
       return ExperimentEntity { experimentEntityId = fromSql expId,
                                 experimentEntityTitle = fromSql title,
                                 experimentEntityDescription = fromSql description,
                                 experimentEntityStartTime = fromSql starttime,
                                 experimentEntityStopTime = fromSql stoptime,
                                 experimentEntityDT = fromSql dt,
                                 experimentEntityIntegMethod =
                                   experimentIntegMethodFromInt (fromSql integMethod),
                                 experimentEntityRunCount = fromSql runCount,
                                 experimentEntityRealStartTime = fromSql realStartTime,
                                 experimentEntityCompleted = fromSql completed,
                                 experimentEntityErrorMessage = fromSql errorMessage }

-- | Implements 'updateExperimentEntity'.
updateHDBCExperimentEntity :: IConnection c => c -> ExperimentEntity -> IO Bool
updateHDBCExperimentEntity c e =
  do n <- handleSqlError $
          withTransaction c $ \c ->
          do n <- run c updateExperimentEntitySQL
                  [toSql $ takeExperimentEntityTitle e,
                   toSql $ takeExperimentEntityDescription e,
                   toSql $ experimentEntityStartTime e,
                   toSql $ experimentEntityStopTime e,
                   toSql $ experimentEntityDT e,
                   toSql $ experimentIntegMethodToInt $ experimentEntityIntegMethod e,
                   toSql $ experimentEntityRunCount e,
                   toSql $ takeExperimentEntityRealStartTime e,
                   toSql $ experimentEntityCompleted e,
                   toSql $ takeExperimentEntityErrorMessage e,
                   toSql $ experimentEntityId e]
             return n
     return (n > 0)

-- | Initialises the variable entity.
initialiseVarEntity :: IConnection c => c -> IO ()
initialiseVarEntity c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createVarEntitySQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createVarEntityIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the variable entity table.
createVarEntitySQL :: String
createVarEntitySQL =
  "CREATE TABLE variables (\
   \  id BIGINT UNIQUE NOT NULL, \
   \  experiment_id BIGINT NOT NULL, \
   \  name VARCHAR(64) NOT NULL, \
   \  description VARCHAR(256) NOT NULL, \
   \  PRIMARY KEY(experiment_id, name), \
   \  FOREIGN KEY(experiment_id) REFERENCES experiments(id) \
   \)"

-- | Return an SQL stament for creating the variable entity indices.
createVarEntityIndexSQL :: [String]
createVarEntityIndexSQL =
  ["CREATE INDEX variable_by_id ON variables(id)",
   "CREATE INDEX variable_by_experiment_id ON variables(experiment_id)",
   "CREATE INDEX variable_by_name ON variables(name)"]

-- | Take the variable entity name.
takeVarEntityName :: VarEntity -> String
takeVarEntityName = take 64 . varEntityName

-- | Take the variable entity description.
takeVarEntityDescription :: VarEntity -> String
takeVarEntityDescription = take 256 . varEntityDescription

-- | Implements 'tryWriteVarEntity'.
tryWriteHDBCVarEntity :: IConnection c => c -> VarEntity -> IO Bool
tryWriteHDBCVarEntity c e =
  do n <- handleSql (const $ return 0) $
          withTransaction c $ \c ->
          do n <- run c insertVarEntitySQL
                  [toSql $ varEntityId e,
                   toSql $ varEntityExperimentId e,
                   toSql $ takeVarEntityName e,
                   toSql $ takeVarEntityDescription e]
             return n
     return (n > 0)

-- | Return an SQL statement for inserting the variable entity.
insertVarEntitySQL :: String
insertVarEntitySQL = "INSERT INTO variables (id, experiment_id, name, description) VALUES (?, ?, ?, ?)"

-- | Return an SQL statement for reading the variable entity.
selectVarEntitySQL :: String
selectVarEntitySQL = "SELECT id, experiment_id, name, description FROM variables WHERE id = ? AND experiment_id = ?"

-- | Return an SQL statement for reading the variable entities by the specified experiment identifier.
selectVarEntitiesSQL :: String
selectVarEntitiesSQL = "SELECT id, experiment_id, name, description FROM variables WHERE experiment_id = ? ORDER BY name"

-- | Return an SQL statement for reading the variable entity by name.
selectVarEntityByNameSQL :: String
selectVarEntityByNameSQL = "SELECT id, experiment_id, name, description FROM variables WHERE experiment_id = ? AND name = ?"

-- | Implements 'readVarEntity'.
readHDBCVarEntity :: IConnection c => c -> ExperimentUUID -> VarUUID -> IO (Maybe VarEntity)
readHDBCVarEntity c expId varId =
  do rs <- handleSqlError $
           hdbcQuery' c selectVarEntitySQL [toSql varId, toSql expId]
     case rs of
       [] -> return Nothing
       [[varId, expId, name, description]] ->
         let e = VarEntity { varEntityId = fromSql varId,
                             varEntityExperimentId = fromSql expId,
                             varEntityName = fromSql name,
                             varEntityDescription = fromSql description }
         in return (Just e)

-- | Implements 'readVarEntityByName'.
readHDBCVarEntityByName :: IConnection c => c -> ExperimentUUID -> String -> IO (Maybe VarEntity)
readHDBCVarEntityByName c expId name =
  do rs <- handleSqlError $
           hdbcQuery' c selectVarEntityByNameSQL [toSql expId, toSql name]
     case rs of
       [] -> return Nothing
       [[varId, expId, name, description]] ->
         let e = VarEntity { varEntityId = fromSql varId,
                             varEntityExperimentId = fromSql expId,
                             varEntityName = fromSql name,
                             varEntityDescription = fromSql description }
         in return (Just e)

-- | Implements 'readVarEntities'.
readHDBCVarEntities :: IConnection c => c -> ExperimentUUID -> IO [VarEntity]
readHDBCVarEntities c expId =
  do rs <- handleSqlError $
           hdbcQuery' c selectVarEntitiesSQL [toSql expId]
     forM rs $ \[varId, expId, name, description] ->
       return VarEntity { varEntityId = fromSql varId,
                          varEntityExperimentId = fromSql expId,
                          varEntityName = fromSql name,
                          varEntityDescription = fromSql description }

-- | Initialises the source entity.
initialiseSourceEntity :: IConnection c => c -> IO ()
initialiseSourceEntity c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createSourceEntitySQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createSourceEntityIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the source entity table.
createSourceEntitySQL :: String
createSourceEntitySQL =
  "CREATE TABLE sources (\
   \  id BIGINT UNIQUE NOT NULL, \
   \  experiment_id BIGINT NOT NULL, \
   \  source_key VARCHAR(64) NOT NULL, \
   \  title VARCHAR(64) NOT NULL, \
   \  description VARCHAR(256) NOT NULL, \
   \  source_type INTEGER NOT NULL, \
   \  PRIMARY KEY(experiment_id, source_key), \
   \  FOREIGN KEY(experiment_id) REFERENCES experiments(id) \
   \)"

-- | Take the source entity key.
takeSourceEntityKey :: SourceEntity -> String
takeSourceEntityKey = take 64 . sourceEntityKey

-- | Take the source entity title.
takeSourceEntityTitle :: SourceEntity -> String
takeSourceEntityTitle = take 64 . sourceEntityTitle

-- | Take the source entity description.
takeSourceEntityDescription :: SourceEntity -> String
takeSourceEntityDescription = take 256 . sourceEntityDescription

-- | Return an SQL stament for creating the source entity indices.
createSourceEntityIndexSQL :: [String]
createSourceEntityIndexSQL =
  ["CREATE INDEX source_by_id ON sources(id)",
   "CREATE INDEX source_by_experiment_id_and_source_key ON sources(experiment_id, source_key)"]

-- | Initialises the source-variable entity.
initialiseSourceVarEntity :: IConnection c => c -> IO ()
initialiseSourceVarEntity c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createSourceVarEntitySQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createSourceVarEntityIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the source-variable entity table.
createSourceVarEntitySQL :: String
createSourceVarEntitySQL =
  "CREATE TABLE sources_to_variables (\
   \  source_id BIGINT NOT NULL, \
   \  variable_id BIGINT NOT NULL, \
   \  PRIMARY KEY(source_id, variable_id), \
   \  FOREIGN KEY(source_id) REFERENCES sources(id), \
   \  FOREIGN KEY(variable_id) REFERENCES variables(id) \
   \)"

-- | Return an SQL stament for creating the source-variable entity indices.
createSourceVarEntityIndexSQL :: [String]
createSourceVarEntityIndexSQL =
  ["CREATE INDEX source_to_variable_by_source_id ON sources_to_variables(source_id)"]

-- | Return an SQL statement for reading the source entity.
selectSourceEntitySQL :: String
selectSourceEntitySQL = "SELECT id, experiment_id, source_key, title, description, source_type FROM sources WHERE id = ? AND experiment_id = ?"

-- | Return an SQL statement for reading the source entities by the specified experiment identifier.
selectSourceEntitiesSQL :: String
selectSourceEntitiesSQL = "SELECT id, experiment_id, source_key, title, description, source_type FROM sources WHERE experiment_id = ? ORDER BY source_key"

-- | Return an SQL statement for reading the source entity by key.
selectSourceEntityByKeySQL :: String
selectSourceEntityByKeySQL = "SELECT id, experiment_id, source_key, title, description, source_type FROM sources WHERE experiment_id = ? AND source_key = ?"

-- | Implements 'readSourceEntity'.
readHDBCSourceEntity :: IConnection c => c -> ExperimentUUID -> SourceUUID -> IO (Maybe SourceEntity)
readHDBCSourceEntity c expId srcId =
  do rs <- handleSqlError $
           hdbcQuery' c selectSourceEntitySQL [toSql srcId, toSql expId]
     case rs of
       [] -> return Nothing
       [[srcId, expId, key, title, description, srcType]] ->
         do varEntities <- readHDBCSourceVarEntities c (fromSql srcId)
            let e = SourceEntity { sourceEntityId = fromSql srcId,
                                   sourceEntityExperimentId = fromSql expId,
                                   sourceEntityKey = fromSql key,
                                   sourceEntityTitle = fromSql title,
                                   sourceEntityDescription = fromSql description,
                                   sourceEntityType = sourceEntityTypeFromInt $ fromSql srcType,
                                   sourceEntityVarEntities = varEntities }
            return (Just e)

-- | Implements 'readSourceEntityByKey'.
readHDBCSourceEntityByKey :: IConnection c => c -> ExperimentUUID -> String -> IO (Maybe SourceEntity)
readHDBCSourceEntityByKey c expId key =
  do rs <- handleSqlError $
           hdbcQuery' c selectSourceEntityByKeySQL [toSql expId, toSql key]
     case rs of
       [] -> return Nothing
       [[srcId, expId, key, title, description, srcType]] ->
         do varEntities <- readHDBCSourceVarEntities c (fromSql srcId)
            let e = SourceEntity { sourceEntityId = fromSql srcId,
                                   sourceEntityExperimentId = fromSql expId,
                                   sourceEntityKey = fromSql key,
                                   sourceEntityTitle = fromSql title,
                                   sourceEntityDescription = fromSql description,
                                   sourceEntityType = sourceEntityTypeFromInt $ fromSql srcType,
                                   sourceEntityVarEntities = varEntities }
            return (Just e)

-- | Implements 'readSourceEntities'.
readHDBCSourceEntities :: IConnection c => c -> ExperimentUUID -> IO [SourceEntity]
readHDBCSourceEntities c expId =
  do rs <- handleSqlError $
           hdbcQuery' c selectSourceEntitiesSQL [toSql expId]
     forM rs $ \[srcId, expId, key, title, description, srcType] ->
       do varEntities <- readHDBCSourceVarEntities c (fromSql srcId)
          return SourceEntity { sourceEntityId = fromSql srcId,
                                sourceEntityExperimentId = fromSql expId,
                                sourceEntityKey = fromSql key,
                                sourceEntityTitle = fromSql title,
                                sourceEntityDescription = fromSql description,
                                sourceEntityType = sourceEntityTypeFromInt $ fromSql srcType,
                                sourceEntityVarEntities = varEntities }

-- | A query for selecting the variables associated with the specified source.
selectSourceVarEntitiesSQL :: String
selectSourceVarEntitiesSQL =
  "SELECT variables.id, variables.experiment_id, variables.name, variables.description FROM variables \
  \ INNER JOIN sources_to_variables ON variables.id = sources_to_variables.variable_id \
  \ WHERE sources_to_variables.source_id = ? \
  \ ORDER BY variables.name"

-- | Read the variable entities associated with the specified source.
readHDBCSourceVarEntities :: IConnection c => c -> SourceUUID -> IO [VarEntity]
readHDBCSourceVarEntities c srcId =
  do rs <- handleSqlError $
           hdbcQuery' c selectSourceVarEntitiesSQL [toSql srcId]
     forM rs $ \[varId, expId, name, description] ->
       let e = VarEntity { varEntityId = fromSql varId,
                           varEntityExperimentId = fromSql expId,
                           varEntityName = fromSql name,
                           varEntityDescription = fromSql description }
       in return e

-- | Implements 'tryWriteSourceEntity'.
tryWriteHDBCSourceEntity :: IConnection c => c -> SourceEntity -> IO Bool
tryWriteHDBCSourceEntity c e =
  handleSql (const $ return False) $
  withTransaction c $ \c ->
  do n <- run c insertSourceEntitySQL
          [toSql $ sourceEntityId e,
           toSql $ sourceEntityExperimentId e,
           toSql $ takeSourceEntityKey e,
           toSql $ takeSourceEntityTitle e,
           toSql $ takeSourceEntityDescription e,
           toSql $ sourceEntityTypeToInt $ sourceEntityType e]
     ns' <-
       forM (sourceEntityVarEntities e) $ \varEntity ->
       do n' <- run c insertSourceVarEntitySQL
                [toSql $ sourceEntityId e,
                 toSql $ varEntityId varEntity]
          return n'
     return $ (n > 0) && (all (> 0) ns')

-- | Return an SQL statement for inserting the source entity.
insertSourceEntitySQL :: String
insertSourceEntitySQL = "INSERT INTO sources (id, experiment_id, source_key, title, description, source_type) VALUES (?, ?, ?, ?, ?, ?)"

-- | Return an SQL statement for inserting the source-variable entity.
insertSourceVarEntitySQL :: String
insertSourceVarEntitySQL = "INSERT INTO sources_to_variables (source_id, variable_id) VALUES (?, ?)"

-- | Initialises the data entity.
initialiseDataEntity :: IConnection c => c -> IO ()
initialiseDataEntity c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createDataEntitySQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createDataEntityIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the data entity table.
createDataEntitySQL :: String
createDataEntitySQL =
  "CREATE TABLE data (\
   \  id BIGINT UNIQUE NOT NULL, \
   \  experiment_id BIGINT NOT NULL, \
   \  run_index INTEGER NOT NULL, \
   \  variable_id BIGINT NOT NULL, \
   \  source_id BIGINT NOT NULL, \
   \  PRIMARY KEY(id), \
   \  FOREIGN KEY(experiment_id) REFERENCES experiments(id), \
   \  FOREIGN KEY(variable_id) REFERENCES variables(id), \
   \  FOREIGN KEY(source_id) REFERENCES sources(id) \
   \)"

-- | Return an SQL stament for creating the data entity indices.
createDataEntityIndexSQL :: [String]
createDataEntityIndexSQL =
  ["CREATE INDEX data_by_id ON data(id)",
   "CREATE INDEX data_by_source_id ON data(source_id)",
   "CREATE INDEX data_by_run_index ON data(run_index)"]

-- | Return an SQL statement for inserting the data entity.
insertDataEntitySQL :: String
insertDataEntitySQL = "INSERT INTO data (id, experiment_id, run_index, variable_id, source_id) VALUES (?, ?, ?, ?, ?)"

-- | Initialises the value data items.
initialiseValueDataItems :: IConnection c => c -> IO ()
initialiseValueDataItems c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createValueDataItemsSQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createValueDataItemIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the value data item table.
createValueDataItemsSQL :: String
createValueDataItemsSQL =
  "CREATE TABLE value_data_items (\
   \  data_id BIGINT NOT NULL, \
   \  iteration INTEGER NOT NULL, \
   \  time DOUBLE PRECISION NOT NULL, \
   \  order_index INTEGER NOT NULL, \
   \  value DOUBLE PRECISION NOT NULL \
   \)"

-- | Return an SQL stament for creating the value data item indices.
createValueDataItemIndexSQL :: [String]
createValueDataItemIndexSQL =
  ["CREATE INDEX value_data_item_by_data_id ON value_data_items(data_id)",
   "CREATE INDEX value_data_item_by_iteration ON value_data_items(iteration)",
   "CREATE INDEX value_data_item_by_time ON value_data_items(time)",
   "CREATE INDEX value_data_item_by_order_index ON value_data_items(order_index)"]

-- | Return an SQL statement for inserting the value data item.
insertValueDataItemSQL :: String
insertValueDataItemSQL = "INSERT INTO value_data_items (data_id, iteration, time, order_index, value) VALUES (?, ?, ?, ?, ?)"

-- | Implements 'writeLastValueEntities'.
writeHDBCLastValueEntities :: IConnection c => c -> [LastValueEntity] -> IO ()
writeHDBCLastValueEntities c es =
  handleSqlError $
  forM_ es $ \e ->
  withTransaction c $ \c ->
  do run c insertDataEntitySQL
       [toSql $ dataEntityId e,
        toSql $ dataEntityExperimentId e,
        toSql $ dataEntityRunIndex e,
        toSql $ dataEntityVarId e,
        toSql $ dataEntitySourceId e]
     let i = dataEntityItem e
     run c insertValueDataItemSQL
       [toSql $ dataEntityId e,
        toSql $ dataItemIteration i,
        toSql $ dataItemTime i,
        toSql $ dataEntityRunIndex e,
        toSql $ dataItemValue i]
     return ()

-- | Implements 'writeTimeSeriesEntity'.
writeHDBCTimeSeriesEntity :: IConnection c => c -> TimeSeriesEntity -> IO ()
writeHDBCTimeSeriesEntity c e =
  handleSqlError $
  withTransaction c $ \c ->
  do run c insertDataEntitySQL
       [toSql $ dataEntityId e,
        toSql $ dataEntityExperimentId e,
        toSql $ dataEntityRunIndex e,
        toSql $ dataEntityVarId e,
        toSql $ dataEntitySourceId e]
     forM_ (divideBy batchInsertSize $ (zip [(1 :: Int) ..] $ dataEntityItem e)) $ \nis ->
       do sth <- prepare c insertValueDataItemSQL
          executeMany sth $
            flip map nis $ \(n, i) ->
            [toSql $ dataEntityId e,
             toSql $ dataItemIteration i,
             toSql $ dataItemTime i,
             toSql n,
             toSql $ dataItemValue i]

-- | Select the data by the specified source identifier and run index.
selectDataEntitySQL :: String
selectDataEntitySQL =
  "SELECT data.id, data.experiment_id, data.run_index, data.variable_id, data.source_id FROM data \
   \ INNER JOIN variables ON variables.id = data.variable_id \
   \ WHERE data.source_id = ? AND data.run_index = ? \
   \ ORDER BY variables.name"

-- | Select all value data items by the specified data identifier.
selectValueDataItemsSQL :: String
selectValueDataItemsSQL =
  "SELECT iteration, time, value FROM value_data_items WHERE data_id = ? \
   \ ORDER BY iteration, time, order_index"

-- | Select the data and value data items by the specified source identifier and run index.
selectValueDataItemsInnerJoinSQL :: String
selectValueDataItemsInnerJoinSQL =
  "SELECT data.id, data.experiment_id, data.run_index, data.variable_id, data.source_id, \
   \ value_data_items.iteration, value_data_items.time, value_data_items.value \
   \ FROM value_data_items \
   \ INNER JOIN data ON data.id = value_data_items.data_id \
   \ WHERE data.source_id = ? AND run_index = ? \
   \ ORDER BY value_data_items.iteration, value_data_items.time, value_data_items.order_index"

-- | Implements 'readLastValueEntities'.
readHDBCLastValueEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> Int -> IO [LastValueEntity]
readHDBCLastValueEntities c expId srcId runIndex =
  do rs <- handleSqlError $
           hdbcQuery' c selectValueDataItemsInnerJoinSQL [toSql srcId, toSql runIndex]
     forM rs $ \[dataId, expId, runIndex, varId, srcId, iteration, time, value] ->
       let i = DataItem { dataItemIteration = fromSql iteration,
                          dataItemTime = fromSql time,
                          dataItemValue = fromSql value }
           e = DataEntity { dataEntityId = fromSql dataId,
                            dataEntityExperimentId = fromSql expId,
                            dataEntityRunIndex = fromSql runIndex,
                            dataEntityVarId = fromSql varId,
                            dataEntitySourceId = fromSql srcId,
                            dataEntityItem = i }
       in return e

-- | Implements 'readTimeSeriesEntities'.
readHDBCTimeSeriesEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> Int -> IO [IO TimeSeriesEntity]
readHDBCTimeSeriesEntities c expId srcId runIndex =
  do rs <- handleSqlError $
           hdbcQuery' c selectDataEntitySQL [toSql srcId, toSql runIndex]
     forM rs $ \[dataId, expId, runIndex, varId, srcId] ->
       return $
       do rs' <- handleSqlError $
                 hdbcQuery' c selectValueDataItemsSQL [dataId]
          let items = flip map rs' $ \[iteration, time, value] ->
                DataItem { dataItemIteration = fromSql iteration,
                           dataItemTime = fromSql time,
                           dataItemValue = fromSql value }
          return DataEntity { dataEntityId = fromSql dataId,
                              dataEntityExperimentId = fromSql expId,
                              dataEntityRunIndex = fromSql runIndex,
                              dataEntityVarId = fromSql varId,
                              dataEntitySourceId = fromSql srcId,
                              dataEntityItem = items }

-- | Initialises the multiple data entity.
initialiseMultipleDataEntity :: IConnection c => c -> IO ()
initialiseMultipleDataEntity c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createMultipleDataEntitySQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createMultipleDataEntityIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the multiple data entity table.
createMultipleDataEntitySQL :: String
createMultipleDataEntitySQL =
  "CREATE TABLE multiple_data (\
   \  id BIGINT UNIQUE NOT NULL, \
   \  experiment_id BIGINT NOT NULL, \
   \  variable_id BIGINT NOT NULL, \
   \  source_id BIGINT NOT NULL, \
   \  PRIMARY KEY(id), \
   \  FOREIGN KEY(experiment_id) REFERENCES experiments(id), \
   \  FOREIGN KEY(variable_id) REFERENCES variables(id), \
   \  FOREIGN KEY(source_id) REFERENCES sources(id) \
   \)"

-- | Return an SQL stament for creating the multiple data entity indices.
createMultipleDataEntityIndexSQL :: [String]
createMultipleDataEntityIndexSQL =
  ["CREATE INDEX multiple_data_by_id ON multiple_data(id)",
   "CREATE INDEX multiple_data_by_source_id ON multiple_data(source_id)"]

-- | Select the multiple data by the specified source identifier.
selectMultipleDataEntitySQL :: String
selectMultipleDataEntitySQL =
  "SELECT multiple_data.id, multiple_data.experiment_id, multiple_data.variable_id, multiple_data.source_id FROM multiple_data \
   \ INNER JOIN variables ON variables.id = multiple_data.variable_id \
   \ WHERE multiple_data.source_id = ? \
   \ ORDER BY variables.name"

-- | Return an SQL statement for inserting the multiple data entity.
insertMultipleDataEntitySQL :: String
insertMultipleDataEntitySQL =
  "INSERT INTO multiple_data (id, experiment_id, variable_id, source_id) VALUES (?, ?, ?, ?)"

-- | Initialises the sampling stats data items.
initialiseSamplingStatsDataItems :: IConnection c => c -> IO ()
initialiseSamplingStatsDataItems c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createSamplingStatsDataItemsSQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createSamplingStatsDataItemIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the sample-based statistics data item table.
createSamplingStatsDataItemsSQL :: String
createSamplingStatsDataItemsSQL =
  "CREATE TABLE sampling_stats_data_items (\
   \  data_id BIGINT NOT NULL, \
   \  iteration INTEGER NOT NULL, \
   \  time DOUBLE PRECISION NOT NULL, \
   \  order_index INTEGER NOT NULL, \
   \  count INTEGER NOT NULL, \
   \  min_value DOUBLE PRECISION NOT NULL, \
   \  max_value DOUBLE PRECISION NOT NULL, \
   \  mean_value DOUBLE PRECISION NOT NULL, \
   \  mean2_value DOUBLE PRECISION NOT NULL \
   \)"

-- | Return an SQL stament for creating the sample-based statistics data item indices.
createSamplingStatsDataItemIndexSQL :: [String]
createSamplingStatsDataItemIndexSQL =
  ["CREATE INDEX sampling_stats_data_item_by_data_id ON sampling_stats_data_items(data_id)",
   "CREATE INDEX sampling_stats_data_item_by_iteration ON sampling_stats_data_items(iteration)",
   "CREATE INDEX sampling_stats_data_item_by_time ON sampling_stats_data_items(time)",
   "CREATE INDEX sampling_stats_data_item_by_order_index ON sampling_stats_data_items(order_index)"]

-- | Return an SQL statement for inserting the sample-based statistics data item.
insertSamplingStatsDataItemSQL :: String
insertSamplingStatsDataItemSQL =
  "INSERT INTO sampling_stats_data_items (data_id, iteration, time, order_index, count, min_value, max_value, mean_value, mean2_value) \
  \ VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

-- | Implements 'writeFinalDeviationEntities'.
writeHDBCFinalDeviationEntities :: IConnection c => c -> [FinalDeviationEntity] -> IO ()
writeHDBCFinalDeviationEntities c es =
  handleSqlError $
  forM_ es $ \e ->
  withTransaction c $ \c ->
  do run c insertMultipleDataEntitySQL
       [toSql $ multipleDataEntityId e,
        toSql $ multipleDataEntityExperimentId e,
        toSql $ multipleDataEntityVarId e,
        toSql $ multipleDataEntitySourceId e]
     let i = multipleDataEntityItem e
         stats = dataItemValue i
     run c insertSamplingStatsDataItemSQL
       [toSql $ multipleDataEntityId e,
        toSql $ dataItemIteration i,
        toSql $ dataItemTime i,
        toSql (1 :: Int),
        toSql $ samplingStatsCount stats,
        toSql $ samplingStatsMin stats,
        toSql $ samplingStatsMax stats,
        toSql $ samplingStatsMean stats,
        toSql $ samplingStatsMean2 stats]
     return ()

-- | Implements 'writeDeviationEntity'.
writeHDBCDeviationEntity :: IConnection c => c -> DeviationEntity -> IO ()
writeHDBCDeviationEntity c e =
  handleSqlError $
  withTransaction c $ \c ->
  do run c insertMultipleDataEntitySQL
       [toSql $ multipleDataEntityId e,
        toSql $ multipleDataEntityExperimentId e,
        toSql $ multipleDataEntityVarId e,
        toSql $ multipleDataEntitySourceId e]
     forM_ (divideBy batchInsertSize $ (zip [(1 :: Int) ..] $ multipleDataEntityItem e)) $ \nis ->
       do sth <- prepare c insertSamplingStatsDataItemSQL
          executeMany sth $
            flip map nis $ \(n, i) ->
            let stats = dataItemValue i
            in [toSql $ multipleDataEntityId e,
                toSql $ dataItemIteration i,
                toSql $ dataItemTime i,
                toSql n,
                toSql $ samplingStatsCount stats,
                toSql $ samplingStatsMin stats,
                toSql $ samplingStatsMax stats,
                toSql $ samplingStatsMean stats,
                toSql $ samplingStatsMean2 stats]

-- | Select all sample-based statistics data items by the specified data identifier.
selectSamplingStatsDataItemsSQL :: String
selectSamplingStatsDataItemsSQL =
  "SELECT iteration, time, count, min_value, max_value, mean_value, mean2_value FROM sampling_stats_data_items \
  \ WHERE data_id = ? \
  \ ORDER BY iteration, time, order_index"

-- | Select the multiple data and sample-based statistcs data items by the specified source identifier.
selectMultipleSamplingStatsDataItemsInnerJoinSQL :: String
selectMultipleSamplingStatsDataItemsInnerJoinSQL =
  "SELECT multiple_data.id, multiple_data.experiment_id, multiple_data.variable_id, multiple_data.source_id, \
   \ sampling_stats_data_items.iteration, sampling_stats_data_items.time, \
   \ sampling_stats_data_items.count, \
   \ sampling_stats_data_items.min_value, sampling_stats_data_items.max_value, \
   \ sampling_stats_data_items.mean_value, sampling_stats_data_items.mean2_value \
   \ FROM sampling_stats_data_items \
   \ INNER JOIN multiple_data ON multiple_data.id = sampling_stats_data_items.data_id \
   \ WHERE multiple_data.source_id = ? \
   \ ORDER BY sampling_stats_data_items.iteration, sampling_stats_data_items.time, sampling_stats_data_items.order_index"

-- | Select the data and sample-based statistcs data items by the specified source identifier and run index.
selectSamplingStatsDataItemsInnerJoinSQL :: String
selectSamplingStatsDataItemsInnerJoinSQL =
  "SELECT data.id, data.experiment_id, data.run_index, data.variable_id, data.source_id, \
   \ sampling_stats_data_items.iteration, sampling_stats_data_items.time, \
   \ sampling_stats_data_items.count, \
   \ sampling_stats_data_items.min_value, sampling_stats_data_items.max_value, \
   \ sampling_stats_data_items.mean_value, sampling_stats_data_items.mean2_value \
   \ FROM sampling_stats_data_items \
   \ INNER JOIN data ON data.id = sampling_stats_data_items.data_id \
   \ WHERE data.source_id = ? AND data.run_index = ? \
   \ ORDER BY sampling_stats_data_items.iteration, sampling_stats_data_items.time, sampling_stats_data_items.order_index"

-- | Implements 'readFinalDeviationEntities'.
readHDBCFinalDeviationEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> IO [FinalDeviationEntity]
readHDBCFinalDeviationEntities c expId srcId =
  do rs <- handleSqlError $
           hdbcQuery' c selectMultipleSamplingStatsDataItemsInnerJoinSQL [toSql srcId]
     forM rs $ \[multipleDataId, expId, varId, srcId, iteration, time, count, minValue, maxValue, meanValue, mean2Value] ->
       let stats = SamplingStats { samplingStatsCount = fromSql count,
                                   samplingStatsMin = fromSql minValue,
                                   samplingStatsMax = fromSql maxValue,
                                   samplingStatsMean = fromSql meanValue,
                                   samplingStatsMean2 = fromSql mean2Value }
           i = DataItem { dataItemIteration = fromSql iteration,
                          dataItemTime = fromSql time,
                          dataItemValue = stats }
           e = MultipleDataEntity { multipleDataEntityId = fromSql multipleDataId,
                                    multipleDataEntityExperimentId = fromSql expId,
                                    multipleDataEntityVarId = fromSql varId,
                                    multipleDataEntitySourceId = fromSql srcId,
                                    multipleDataEntityItem = i }
       in return e

-- | Implements 'readDeviationEntities'.
readHDBCDeviationEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> IO [IO DeviationEntity]
readHDBCDeviationEntities c expId srcId =
  do rs <- handleSqlError $
           hdbcQuery' c selectMultipleDataEntitySQL [toSql srcId]
     forM rs $ \[multipleDataId, expId, varId, srcId] ->
       return $
       do rs' <- handleSqlError $
                 hdbcQuery' c selectSamplingStatsDataItemsSQL [multipleDataId]
          let items = flip map rs' $ \[iteration, time, count, minValue, maxValue, meanValue, mean2Value] ->
                let stats = SamplingStats { samplingStatsCount = fromSql count,
                                            samplingStatsMin = fromSql minValue,
                                            samplingStatsMax = fromSql maxValue,
                                            samplingStatsMean = fromSql meanValue,
                                            samplingStatsMean2 = fromSql mean2Value }
                in DataItem { dataItemIteration = fromSql iteration,
                              dataItemTime = fromSql time,
                              dataItemValue = stats }
          return MultipleDataEntity { multipleDataEntityId = fromSql multipleDataId,
                                      multipleDataEntityExperimentId = fromSql expId,
                                      multipleDataEntityVarId = fromSql varId,
                                      multipleDataEntitySourceId = fromSql srcId,
                                      multipleDataEntityItem = items }

-- | Implements 'writeMultipleValueEntities'.
writeHDBCMultipleValueEntities :: IConnection c => c -> [MultipleValueEntity] -> IO ()
writeHDBCMultipleValueEntities c es =
  handleSqlError $
  forM_ es $ \e ->
  withTransaction c $ \c ->
  do run c insertMultipleDataEntitySQL
       [toSql $ multipleDataEntityId e,
        toSql $ multipleDataEntityExperimentId e,
        toSql $ multipleDataEntityVarId e,
        toSql $ multipleDataEntitySourceId e]
     forM_ (divideBy batchInsertSize $ (zip [(1 :: Int) ..] $ multipleDataEntityItem e)) $ \nis ->
       do sth <- prepare c insertValueDataItemSQL
          executeMany sth $
            flip map nis $ \(n, i) ->
            [toSql $ multipleDataEntityId e,
             toSql $ dataItemIteration i,
             toSql $ dataItemTime i,
             toSql n,
             toSql $ dataItemValue i]

-- | Implements 'readMultipleValueEntities'.
readHDBCMultipleValueEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> IO [IO MultipleValueEntity]
readHDBCMultipleValueEntities c expId srcId =
  do rs <- handleSqlError $
           hdbcQuery' c selectMultipleDataEntitySQL [toSql srcId]
     forM rs $ \[multipleDataId, expId, varId, srcId] ->
       return $
       do rs' <- handleSqlError $
                 hdbcQuery' c selectValueDataItemsSQL [multipleDataId]
          items <-
            forM rs' $ \[iteration, time, value] ->
            return DataItem { dataItemIteration = fromSql iteration,
                              dataItemTime = fromSql time,
                              dataItemValue = fromSql value }
          return MultipleDataEntity { multipleDataEntityId = fromSql multipleDataId,
                                      multipleDataEntityExperimentId = fromSql expId,
                                      multipleDataEntityVarId = fromSql varId,
                                      multipleDataEntitySourceId = fromSql srcId,
                                      multipleDataEntityItem = items }

-- | Implements 'writeFinalSamplingStatsEntities'.
writeHDBCFinalSamplingStatsEntities :: IConnection c => c -> [FinalSamplingStatsEntity] -> IO ()
writeHDBCFinalSamplingStatsEntities c es =
  handleSqlError $
  forM_ es $ \e ->
  withTransaction c $ \c ->
  do run c insertDataEntitySQL
       [toSql $ dataEntityId e,
        toSql $ dataEntityExperimentId e,
        toSql $ dataEntityRunIndex e,
        toSql $ dataEntityVarId e,
        toSql $ dataEntitySourceId e]
     let i = dataEntityItem e
         stats = dataItemValue i
     run c insertSamplingStatsDataItemSQL
       [toSql $ dataEntityId e,
        toSql $ dataItemIteration i,
        toSql $ dataItemTime i,
        toSql (1 :: Int),
        toSql $ samplingStatsCount stats,
        toSql $ samplingStatsMin stats,
        toSql $ samplingStatsMax stats,
        toSql $ samplingStatsMean stats,
        toSql $ samplingStatsMean2 stats]
     return ()

-- | Implements 'writeSamplingStatsEntity'.
writeHDBCSamplingStatsEntity :: IConnection c => c -> SamplingStatsEntity -> IO ()
writeHDBCSamplingStatsEntity c e =
  handleSqlError $
  withTransaction c $ \c ->
  do run c insertDataEntitySQL
       [toSql $ dataEntityId e,
        toSql $ dataEntityExperimentId e,
        toSql $ dataEntityRunIndex e,
        toSql $ dataEntityVarId e,
        toSql $ dataEntitySourceId e]
     forM_ (divideBy batchInsertSize $ (zip [(1 :: Int) ..] $ dataEntityItem e)) $ \nis ->
       do sth <- prepare c insertSamplingStatsDataItemSQL
          executeMany sth $
            flip map nis $ \(n, i) ->
            let stats = dataItemValue i
            in [toSql $ dataEntityId e,
                toSql $ dataItemIteration i,
                toSql $ dataItemTime i,
                toSql n,
                toSql $ samplingStatsCount stats,
                toSql $ samplingStatsMin stats,
                toSql $ samplingStatsMax stats,
                toSql $ samplingStatsMean stats,
                toSql $ samplingStatsMean2 stats]

-- | Implements 'readFinalSamplingStatsEntities'.
readHDBCFinalSamplingStatsEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> Int -> IO [FinalSamplingStatsEntity]
readHDBCFinalSamplingStatsEntities c expId srcId runIndex =
  do rs <- handleSqlError $
           hdbcQuery' c selectSamplingStatsDataItemsInnerJoinSQL [toSql srcId, toSql runIndex]
     forM rs $ \[dataId, expId, runIndex, varId, srcId, iteration, time, count, minValue, maxValue, meanValue, mean2Value] ->
       let stats = SamplingStats { samplingStatsCount = fromSql count,
                                   samplingStatsMin = fromSql minValue,
                                   samplingStatsMax = fromSql maxValue,
                                   samplingStatsMean = fromSql meanValue,
                                   samplingStatsMean2 = fromSql mean2Value }
           i = DataItem { dataItemIteration = fromSql iteration,
                          dataItemTime = fromSql time,
                          dataItemValue = stats }
           e = DataEntity { dataEntityId = fromSql dataId,
                            dataEntityExperimentId = fromSql expId,
                            dataEntityRunIndex = fromSql runIndex,
                            dataEntityVarId = fromSql varId,
                            dataEntitySourceId = fromSql srcId,
                            dataEntityItem = i }
       in return e

-- | Implements 'readSamplingStatsEntities'.
readHDBCSamplingStatsEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> Int -> IO [IO SamplingStatsEntity]
readHDBCSamplingStatsEntities c expId srcId runIndex =
  do rs <- handleSqlError $
           hdbcQuery' c selectDataEntitySQL [toSql srcId, toSql runIndex]
     forM rs $ \[dataId, expId, runIndex, varId, srcId] ->
       return $
       do rs' <- handleSqlError $
                 hdbcQuery' c selectSamplingStatsDataItemsSQL [dataId]
          let items = flip map rs' $ \[iteration, time, count, minValue, maxValue, meanValue, mean2Value] ->
                let stats = SamplingStats { samplingStatsCount = fromSql count,
                                            samplingStatsMin = fromSql minValue,
                                            samplingStatsMax = fromSql maxValue,
                                            samplingStatsMean = fromSql meanValue,
                                            samplingStatsMean2 = fromSql mean2Value }
                in DataItem { dataItemIteration = fromSql iteration,
                              dataItemTime = fromSql time,
                              dataItemValue = stats }
          return DataEntity { dataEntityId = fromSql dataId,
                              dataEntityExperimentId = fromSql expId,
                              dataEntityRunIndex = fromSql runIndex,
                              dataEntityVarId = fromSql varId,
                              dataEntitySourceId = fromSql srcId,
                              dataEntityItem = items }

-- | Initialises the timing stats data items.
initialiseTimingStatsDataItems :: IConnection c => c -> IO ()
initialiseTimingStatsDataItems c =
  do f <- handleSql (const $ return False) $
          withTransaction c $ \c ->
          do run c createTimingStatsDataItemsSQL []
             return True
     when f $
       handleSqlError $
       withTransaction c $ \c ->
       forM_ createTimingStatsDataItemIndexSQL $ \sql ->
       do run c sql []
          return ()

-- | Return an SQL stament for creating the time-dependent statistics data item table.
createTimingStatsDataItemsSQL :: String
createTimingStatsDataItemsSQL =
  "CREATE TABLE timing_stats_data_items (\
   \  data_id BIGINT NOT NULL, \
   \  iteration INTEGER NOT NULL, \
   \  time DOUBLE PRECISION NOT NULL, \
   \  order_index INTEGER NOT NULL, \
   \  count INTEGER NOT NULL, \
   \  min_value DOUBLE PRECISION NOT NULL, \
   \  max_value DOUBLE PRECISION NOT NULL, \
   \  last_value DOUBLE PRECISION NOT NULL, \
   \  min_time DOUBLE PRECISION NOT NULL, \
   \  max_time DOUBLE PRECISION NOT NULL, \
   \  start_time DOUBLE PRECISION NOT NULL, \
   \  last_time DOUBLE PRECISION NOT NULL, \
   \  sum_value DOUBLE PRECISION NOT NULL, \
   \  sum2_value DOUBLE PRECISION NOT NULL \
   \)"

-- | Return an SQL stament for creating the time-dependent statistics data item indices.
createTimingStatsDataItemIndexSQL :: [String]
createTimingStatsDataItemIndexSQL =
  ["CREATE INDEX timing_stats_data_item_by_data_id ON timing_stats_data_items(data_id)",
   "CREATE INDEX timing_stats_data_item_by_iteration ON timing_stats_data_items(iteration)",
   "CREATE INDEX timing_stats_data_item_by_time ON timing_stats_data_items(time)",
   "CREATE INDEX timing_stats_data_item_by_order_index ON timing_stats_data_items(order_index)"]

-- | Return an SQL statement for inserting the time-dependent statistics data item.
insertTimingStatsDataItemSQL :: String
insertTimingStatsDataItemSQL =
  "INSERT INTO timing_stats_data_items (data_id, iteration, time, order_index, \
  \ count, min_value, max_value, last_value, \
  \ min_time, max_time, start_time, last_time, sum_value, sum2_value) \
  \ VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

-- | Select all time-dependent statistics data items by the specified data identifier.
selectTimingStatsDataItemsSQL :: String
selectTimingStatsDataItemsSQL =
  "SELECT iteration, time, count, min_value, max_value, last_value, \
  \ min_time, max_time, start_time, last_time, sum_value, sum2_value \
  \ FROM timing_stats_data_items \
  \ WHERE data_id = ? \
  \ ORDER BY iteration, time, order_index"

-- | Select the data and time-dependent statistcs data items by the specified source identifier and run index.
selectTimingStatsDataItemsInnerJoinSQL :: String
selectTimingStatsDataItemsInnerJoinSQL =
  "SELECT data.id, data.experiment_id, data.run_index, data.variable_id, data.source_id, \
   \ timing_stats_data_items.iteration, timing_stats_data_items.time, \
   \ timing_stats_data_items.count, \
   \ timing_stats_data_items.min_value, timing_stats_data_items.max_value, \
   \ timing_stats_data_items.last_value, \
   \ timing_stats_data_items.min_time, timing_stats_data_items.max_time, \
   \ timing_stats_data_items.start_time, timing_stats_data_items.last_time, \
   \ timing_stats_data_items.sum_value, timing_stats_data_items.sum2_value \
   \ FROM timing_stats_data_items \
   \ INNER JOIN data ON data.id = timing_stats_data_items.data_id \
   \ WHERE data.source_id = ? AND data.run_index = ? \
   \ ORDER BY timing_stats_data_items.iteration, timing_stats_data_items.time, timing_stats_data_items.order_index"

-- | Implements 'writeFinalTimingStatsEntities'.
writeHDBCFinalTimingStatsEntities :: IConnection c => c -> [FinalTimingStatsEntity] -> IO ()
writeHDBCFinalTimingStatsEntities c es =
  handleSqlError $
  forM_ es $ \e ->
  withTransaction c $ \c ->
  do run c insertDataEntitySQL
       [toSql $ dataEntityId e,
        toSql $ dataEntityExperimentId e,
        toSql $ dataEntityRunIndex e,
        toSql $ dataEntityVarId e,
        toSql $ dataEntitySourceId e]
     let i = dataEntityItem e
         stats = dataItemValue i
     run c insertTimingStatsDataItemSQL
       [toSql $ dataEntityId e,
        toSql $ dataItemIteration i,
        toSql $ dataItemTime i,
        toSql (1 :: Int),
        toSql $ timingStatsCount stats,
        toSql $ timingStatsMin stats,
        toSql $ timingStatsMax stats,
        toSql $ timingStatsLast stats,
        toSql $ timingStatsMinTime stats,
        toSql $ timingStatsMaxTime stats,
        toSql $ timingStatsStartTime stats,
        toSql $ timingStatsLastTime stats,
        toSql $ timingStatsSum stats,
        toSql $ timingStatsSum2 stats]

-- | Implements 'writeTimingStatsEntity'.
writeHDBCTimingStatsEntity :: IConnection c => c -> TimingStatsEntity -> IO ()
writeHDBCTimingStatsEntity c e =
  handleSqlError $
  withTransaction c $ \c ->
  do run c insertDataEntitySQL
       [toSql $ dataEntityId e,
        toSql $ dataEntityExperimentId e,
        toSql $ dataEntityRunIndex e,
        toSql $ dataEntityVarId e,
        toSql $ dataEntitySourceId e]
     forM_ (divideBy batchInsertSize $ (zip [(1 :: Int) ..] $ dataEntityItem e)) $ \nis ->
       do sth <- prepare c insertTimingStatsDataItemSQL
          executeMany sth $
            flip map nis $ \(n, i) ->
            let stats = dataItemValue i
            in [toSql $ dataEntityId e,
                toSql $ dataItemIteration i,
                toSql $ dataItemTime i,
                toSql n,
                toSql $ timingStatsCount stats,
                toSql $ timingStatsMin stats,
                toSql $ timingStatsMax stats,
                toSql $ timingStatsLast stats,
                toSql $ timingStatsMinTime stats,
                toSql $ timingStatsMaxTime stats,
                toSql $ timingStatsStartTime stats,
                toSql $ timingStatsLastTime stats,
                toSql $ timingStatsSum stats,
                toSql $ timingStatsSum2 stats]

-- | Implements 'readFinalTimingStatsEntities'.
readHDBCFinalTimingStatsEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> Int -> IO [FinalTimingStatsEntity]
readHDBCFinalTimingStatsEntities c expId srcId runIndex =
  do rs <- handleSqlError $
           hdbcQuery' c selectTimingStatsDataItemsInnerJoinSQL [toSql srcId, toSql runIndex]
     forM rs $ \[dataId, expId, runIndex, varId, srcId, iteration, time, count, minValue, maxValue, lastValue,
                 minTime, maxTime, startTime, lastTime, sumValue, sum2Value] ->
       let stats = TimingStats { timingStatsCount = fromSql count,
                                 timingStatsMin = fromSql minValue,
                                 timingStatsMax = fromSql maxValue,
                                 timingStatsLast = fromSql lastValue,
                                 timingStatsMinTime = fromSql minTime,
                                 timingStatsMaxTime = fromSql maxTime,
                                 timingStatsStartTime = fromSql startTime,
                                 timingStatsLastTime = fromSql lastTime,
                                 timingStatsSum = fromSql sumValue,
                                 timingStatsSum2 = fromSql sum2Value }
           i = DataItem { dataItemIteration = fromSql iteration,
                          dataItemTime = fromSql time,
                          dataItemValue = stats }
           e = DataEntity { dataEntityId = fromSql dataId,
                            dataEntityExperimentId = fromSql expId,
                            dataEntityRunIndex = fromSql runIndex,
                            dataEntityVarId = fromSql varId,
                            dataEntitySourceId = fromSql srcId,
                            dataEntityItem = i }
       in return e

-- | Implements 'readTimingStatsEntities'.
readHDBCTimingStatsEntities :: IConnection c => c -> ExperimentUUID -> SourceUUID -> Int -> IO [IO TimingStatsEntity]
readHDBCTimingStatsEntities c expId srcId runIndex =
  do rs <- handleSqlError $
           hdbcQuery' c selectDataEntitySQL [toSql srcId, toSql runIndex]
     forM rs $ \[dataId, expId, runIndex, varId, srcId] ->
       return $
       do rs' <- handleSqlError $
                 hdbcQuery' c selectTimingStatsDataItemsSQL [dataId]
          let items = flip map rs' $ \[iteration, time, count, minValue, maxValue, lastValue,
                                       minTime, maxTime, startTime, lastTime, sumValue, sum2Value] ->
                let stats = TimingStats { timingStatsCount = fromSql count,
                                          timingStatsMin = fromSql minValue,
                                          timingStatsMax = fromSql maxValue,
                                          timingStatsLast = fromSql lastValue,
                                          timingStatsMinTime = fromSql minTime,
                                          timingStatsMaxTime = fromSql maxTime,
                                          timingStatsStartTime = fromSql startTime,
                                          timingStatsLastTime = fromSql lastTime,
                                          timingStatsSum = fromSql sumValue,
                                          timingStatsSum2 = fromSql sum2Value }
                in DataItem { dataItemIteration = fromSql iteration,
                              dataItemTime = fromSql time,
                              dataItemValue = stats }
          return DataEntity { dataEntityId = fromSql dataId,
                              dataEntityExperimentId = fromSql expId,
                              dataEntityRunIndex = fromSql runIndex,
                              dataEntityVarId = fromSql varId,
                              dataEntitySourceId = fromSql srcId,
                              dataEntityItem = items }

-- | Select data entity identifiers by the specified experiment identifier.
selectDataEntityIdByExperimentIdSQL :: String
selectDataEntityIdByExperimentIdSQL = "SELECT id FROM data WHERE experiment_id = ?"

-- | Select multiple data entity identifiers by the specified experiment identifier.
selectMultipleDataEntityIdByExperimentIdSQL :: String
selectMultipleDataEntityIdByExperimentIdSQL = "SELECT id FROM multiple_data WHERE experiment_id = ?"

-- | Delete the data entities by the specified experiment identifier.
deleteDataEntitySQL :: String
deleteDataEntitySQL = "DELETE FROM data WHERE experiment_id = ?"

-- | Delete the multiple data entities by the specified experiment identifier.
deleteMultipleDataEntitySQL :: String
deleteMultipleDataEntitySQL = "DELETE FROM multiple_data WHERE experiment_id = ?"

-- | Delete the value data items by the specified data identifier.
deleteValueDataItemsSQL :: String
deleteValueDataItemsSQL = "DELETE FROM value_data_items WHERE data_id = ?"

-- | Delete the sample-based statistics data items by the specified data identifier.
deleteSamplingStatsDataItemsSQL :: String
deleteSamplingStatsDataItemsSQL = "DELETE FROM sampling_stats_data_items WHERE data_id = ?"

-- | Delete the time-dependent statistics data items by the specified data identifier.
deleteTimingStatsDataItemsSQL :: String
deleteTimingStatsDataItemsSQL = "DELETE FROM timing_stats_data_items WHERE data_id = ?"

-- | Delete the data entities.
deleteHDBCDataEntities :: IConnection c => c -> ExperimentUUID -> IO Bool
deleteHDBCDataEntities c expId =
  handleSqlError $
  do rs <- hdbcQuery' c selectDataEntityIdByExperimentIdSQL [toSql expId]
     forM rs $ \[dataId] ->
       do run c deleteValueDataItemsSQL [dataId]
          run c deleteSamplingStatsDataItemsSQL [dataId]
          run c deleteTimingStatsDataItemsSQL [dataId]
     n <- run c deleteDataEntitySQL [toSql expId]
     return (n < 0)

-- | Delete the multiple data entities.
deleteHDBCMultipleDataEntities :: IConnection c => c -> ExperimentUUID -> IO Bool
deleteHDBCMultipleDataEntities c expId =
  handleSqlError $
  do rs <- hdbcQuery' c selectMultipleDataEntityIdByExperimentIdSQL [toSql expId]
     forM rs $ \[dataId] ->
       do run c deleteValueDataItemsSQL [dataId]
          run c deleteSamplingStatsDataItemsSQL [dataId]
          run c deleteTimingStatsDataItemsSQL [dataId]
     n <- run c deleteMultipleDataEntitySQL [toSql expId]
     return (n > 0)

-- | Select the source entity identifiers by the experiment identifier.
selectSourceEntityIdByExperimentIdSQL :: String
selectSourceEntityIdByExperimentIdSQL = "SELECT id FROM sources WHERE experiment_id = ?"

-- | Delete the source-to-variable mapping entities by the specified source identifier.
deleteSourceVarEntitySQL :: String
deleteSourceVarEntitySQL = "DELETE FROM sources_to_variables WHERE source_id = ?"

-- | Delete the source entities by the specified experiment identifier.
deleteSourceEntitySQL :: String
deleteSourceEntitySQL = "DELETE FROM sources WHERE experiment_id = ?"

-- | Delete the source entities.
deleteHDBCSourceEntities :: IConnection c => c -> ExperimentUUID -> IO Bool
deleteHDBCSourceEntities c expId =
  handleSqlError $
  do rs <- hdbcQuery' c selectSourceEntityIdByExperimentIdSQL [toSql expId]
     forM rs $ \[srcId] ->
       run c deleteSourceVarEntitySQL [srcId]
     n <- run c deleteSourceEntitySQL [toSql expId]
     return (n > 0)

-- | Delete the variable entities by the specified experiment identifier.
deleteVarEntitySQL :: String
deleteVarEntitySQL = "DELETE FROM variables WHERE experiment_id = ?"

-- | Delete the variable entities.
deleteHDBCVarEntities :: IConnection c => c -> ExperimentUUID -> IO Bool
deleteHDBCVarEntities c expId =
  handleSqlError $
  do n <- run c deleteVarEntitySQL [toSql expId]
     return (n > 0)

-- | Delete the specified experiment entity.
deleteExperimentEntitySQL :: String
deleteExperimentEntitySQL = "DELETE FROM experiments WHERE id = ?"

-- | Delete the experiment entity.
deleteHDBCExperimentEntity :: IConnection c => c -> ExperimentUUID -> IO Bool
deleteHDBCExperimentEntity c expId =
  handleSqlError $
  withTransaction c $ \c ->
  do deleteHDBCDataEntities c expId
     deleteHDBCMultipleDataEntities c expId
     deleteHDBCSourceEntities c expId
     deleteHDBCVarEntities c expId
     n <- run c deleteExperimentEntitySQL [toSql expId]
     return (n > 0)
