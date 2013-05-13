<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
use Cubex\Cli\Shell;
use Cubex\Events\EventManager;
use Cubex\Facade\Cassandra;
use Cubex\KvStore\Cassandra\CassandraException;
use Cubex\KvStore\Cassandra\ColumnFamily;
use Cubex\KvStore\Cassandra\DataType\BytesType;
use Cubex\KvStore\Cassandra\DataType\CassandraType;
use Cubex\Log\Log;
use Cubex\Mapper\Database\RecordCollection;
use Cubex\Mapper\Database\SearchObject;
use Cubex\Sprintf\ParseQuery;
use Cubex\Text\TextTable;
use cassandra\ConsistencyLevel;

class RangeManager
{
  /**
   * @var ColumnFamily
   */
  private $_cf;
  private $_cassandraServiceName;
  private $_columnFamily;
  private $_processor;
  private $_minToken;
  private $_maxToken;
  private $_readConsistencyLevel;
  private $_cassReceiveTimeout;
  private $_cassSendTimeout;
  private $_columnDataType;

  private $_scriptProgress;
  private $_instanceName;
  private $_hostname;
  private $_statsReporter;
  private $_batchSizeTuner;

  public function __construct(
    $cassandraServiceName, $columnFamily,
    ItemProcessor $processor, $instanceName = "", $displayReport = true
  )
  {
    $this->_cassandraServiceName = $cassandraServiceName;
    $this->_columnFamily         = $columnFamily;
    $this->_cf                   = null;
    $this->_processor            = $processor;
    $this->_scriptProgress       = new ScriptProgress();
    $this->_instanceName         = $instanceName;
    $this->_hostname             = gethostname();
    $this->_readConsistencyLevel = ConsistencyLevel::QUORUM;
    $this->_cassSendTimeout      = 2000;
    $this->_cassReceiveTimeout   = 2000;
    $this->_columnDataType       = new BytesType();
    if($this->_instanceName != "")
    {
      $this->_hostname .= "|" . $this->_instanceName;
    }

    $this->_processor->sourceColumnFamily = $this->_getCF();

    // Work out the max token for this CF
    $this->_calcMinMaxTokens();

    $this->_statsReporter = new StatsReporter($this->_instanceName);
    $this->_statsReporter->displayPrettyReport = $displayReport;

    $this->_batchSizeTuner        = new BatchSizeTuner();
    $this->_batchSizeTuner->setBatchSizeLimitsArr(
      $this->_processor->getBatchSize()
    );
  }

  private function _calcMinMaxTokens()
  {
    $partitionerType = $this->_getCF()->connection()->partitioner();
    switch($partitionerType)
    {
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        $this->_minToken = bcadd(bcpow("-2", "63"), "1");
        $this->_maxToken = bcsub(bcpow("2", "63"), "1");
        break;
      case "org.apache.cassandra.dht.RandomPartitioner":
        $this->_minToken = "0";
        $this->_maxToken = bcpow("2", "127");
        break;
      default:
        throw new \Exception('Unknown partitioner type: ' . $partitionerType);
    }
  }


  private function _getCF($refresh = false)
  {
    if($refresh || (! $this->_cf))
    {
      EventManager::trigger(Events::CASS_CONNECT_START);

      $cass = Cassandra::getAccessor($this->_cassandraServiceName);
      if($refresh)
      {
        $cass->disconnect();

        $tries = 0;
        while(true)
        {
          try
          {
            $cass->connect();
            break;
          }
          catch(\Exception $e)
          {
            $tries++;
            if($tries >= 5)
            {
              throw $e;
            }
            Log::info('Connect failed. Retrying...');
            usleep($tries * 10000);
          }
        }
      }

      $this->_cf = $cass->cf($this->_columnFamily, false);
      $this->setColumnDataType($this->_columnDataType);
      $this->setReadConsistencyLevel($this->_readConsistencyLevel);
      $this->setCassTimeout(
        $this->_cassSendTimeout,
        $this->_cassReceiveTimeout
      );

      EventManager::trigger(Events::CASS_CONNECT_END);
    }
    return $this->_cf;
  }

  public function setColumnDataType(CassandraType $type)
  {
    $this->_columnDataType = $type;
    if($this->_cf)
    {
      $this->_cf->setColumnDataType($type);
    }
  }

  public function setCassTimeout($sendTimeout, $receiveTimeout)
  {
    $this->_cassSendTimeout = $sendTimeout;
    $this->_cassReceiveTimeout = $receiveTimeout;
    if($this->_cf)
    {
      $this->_cf->connection()->setReceiveTimeout($this->_cassReceiveTimeout);
      $this->_cf->connection()->setSendTimeout($this->_cassSendTimeout);
    }
  }

  public function setReadConsistencyLevel($consistencyLevel)
  {
    $this->_readConsistencyLevel = $consistencyLevel;
    if($this->_cf)
    {
      $this->_cf->setReadConsistencyLevel($consistencyLevel);
    }
  }

  public function buildRanges($numRanges)
  {
    echo "Creating ranges... ";
    $firstToken = $this->_minToken;
    $lastToken  = $this->_maxToken;

    // Delete all ranges from the DB
    $db = TokenRange::conn();
    $tableName = (new TokenRange())->getTableName();
    $db->query(ParseQuery::parse($db, 'DELETE FROM %T', $tableName));
    $db->query(
      ParseQuery::parse($db, 'ALTER TABLE %T AUTO_INCREMENT=0', $tableName)
    );

    $interval = bcdiv(bcsub($lastToken, $firstToken), $numRanges);

    $numCreated = 0;
    $prevToken  = "";
    $lastRange = null;
    for(
      $tok = $firstToken;
      bccomp($tok, $lastToken) < 1;
      $tok = bcadd($tok, $interval)
    )
    {
      if($prevToken !== "")
      {
        $range             = new TokenRange();
        $range->startToken = $prevToken;
        $range->endToken   = $tok;
        $range->randomKey  = rand(1, 10000);
        $range->saveChanges();
        $lastRange = $range;

        $numCreated++;
      }

      Shell::clearLine();
      echo "Creating ranges... " .
        number_format($numCreated) . " / " . number_format($numRanges);

      $prevToken = $tok;
    }

    // Catch left over tokens and add them to the last range
    if(bccomp(bcsub($lastToken, $prevToken), 0) == 1)
    {
      if($lastRange)
      {
        $range = $lastRange;
      }
      else
      {
        $range = new TokenRange();
        $range->startToken = $prevToken;
        $range->randomKey  = rand(1, 10000);
      }
      $range->endToken   = $lastToken;
      $range->saveChanges();
    }

    echo "\nFinished creating ranges.\n";
  }

  public function resetRanges()
  {
    $tableName = (new TokenRange())->getTableName();
    $conn      = TokenRange::conn();
    $conn->query(
      "UPDATE `" . $tableName . "` SET firstKey='', lastKey='', processing=0," .
      "hostname=NULL, processed=0, failed=0, processingTime=0, " .
      "totalItems=0, processedItems=0, errorCount=0, error=NULL"
    );
  }

  public function resetRange($rangeId)
  {
    $range = new TokenRange($rangeId);
    if(!$range->exists())
    {
      throw new \Exception('Range does not exist: ' . $rangeId);
    }

    $range->firstKey       = '';
    $range->lastKey        = '';
    $range->processing     = 0;
    $range->hostname       = null;
    $range->processed      = 0;
    $range->failed         = 0;
    $range->processingTime = 0;
    $range->totalItems     = 0;
    $range->processedItems = 0;
    $range->errorCount     = 0;
    $range->error          = null;

    $range->saveChanges();
  }

  /**
   * For testing only: refresh the keys for all ranges
   */
  public function refreshKeysForAllRanges()
  {
    $ranges = (new RecordCollection(new TokenRange()))->loadAll();
    $total = count($ranges);
    $processed = 0;
    foreach($ranges as $range)
    {
      $this->refreshKeysForRange($range);
      $processed++;

      Shell::clearLine();
      echo "Processed: " . number_format($processed) .
        ' / ' . number_format($total);
    }
    echo "\n";
  }

  public function refreshKeysForRange(TokenRange $range)
  {
    EventManager::trigger(Events::REFRESH_KEYS_START);

    $cf = $this->_getCF(true);

    $range->firstKey = '';
    $range->lastKey = '';

    $firstKey = '';
    $lastKey = '';
    $gotFirst = false;
    $gotLast = false;

    // get the first and last keys in the CF
    $firstItemInCF = $this->_getTokensWithRetry(
      $cf, $this->_minToken, $this->_minToken, 1
    );
    $firstKeyInCF = key($firstItemInCF);
    $lastItemInCF = $this->_getTokensWithRetry(
      $cf, $this->_maxToken, $this->_maxToken, 1
    );
    $lastKeyInCF = key($lastItemInCF);


    $firstItem = $this->_getTokensWithRetry(
      $cf, $range->startToken, $range->startToken, 1
    );
    if($firstItem)
    {
      $firstKey = key($firstItem);
      if($firstKey == $firstKeyInCF)
      {
        $firstKey = "";
      }
      $gotFirst = true;
    }

    $lastItem = $this->_getTokensWithRetry(
      $cf, $range->endToken, $range->endToken, 1
    );
    if($lastItem)
    {
      $lastKey = key($lastItem);
      if($lastKey == $lastKeyInCF)
      {
        $lastKey = "";
      }
      $gotLast = true;
    }

    if($gotFirst && $gotLast)
    {
      $range->firstKey = $firstKey;
      $range->lastKey = $lastKey;
      $range->saveChanges();
    }

    EventManager::trigger(Events::REFRESH_KEYS_END);
  }


  private function _getTokensWithRetry(
    ColumnFamily &$cf, $startToken, $endToken, $count
  )
  {
    $tries = 0;
    $res = null;
    while(true)
    {
      try
      {
        $res = $cf->getTokens($startToken, $endToken, $count);
        break;
      }
      catch(\Exception $e)
      {
        $tries++;
        if($tries >= 5)
        {
          throw $e;
        }
      }

      Log::info('getTokens failed. Retrying...');
      usleep($tries * 100000);
      $cf = $this->_getCF(true);
    }

    return $res;
  }

  private function _getKeysWithRetry(
    ColumnFamily &$cf, $lastKey, $rangeLastKey, $batchSize, $cols
  )
  {
    EventManager::trigger(Events::GET_KEYS_START);

    $items = null;
    $tries = 0;
    while(true)
    {
      try
      {
        $items = $cf->getKeys($lastKey, $rangeLastKey, $batchSize, $cols);
        break;
      }
      catch(\Exception $e)
      {
        $tries++;
        if($tries >= 5)
        {
          throw $e;
        }

        Log::info('getKeys failed. Retrying...');
        usleep($tries * 100000);
        $cf = $this->_getCF(true);
      }
    }

    EventManager::trigger(Events::GET_KEYS_END);
    return $items;
  }


  /**
   * @return TokenRange
   */
  public function claimNextFreeRange()
  {
    EventManager::trigger(Events::CLAIM_RANGE_START);

    $range = false;
    $db    = TokenRange::conn();

    // Check for an already-flagged range
    $coll = new RecordCollection(new TokenRange());
    $coll->loadWhere(['processing' => 1, 'hostname' => $this->_hostname])
    ->limit(1);

    if($coll->count() > 0)
    {
      $range = $coll->first();
    }
    else
    {
      $res = $db->query(
        ParseQuery::parse(
          $db,
          "UPDATE %T SET processing=1, hostname=%s " .
          "WHERE processing=0 AND processed=0 ORDER BY randomKey LIMIT 1",
          (new TokenRange())->getTableName(), $this->_hostname
        )
      );

      if($res)
      {
        $range = TokenRange::loadWhere(
          ['processing' => 1, 'hostname' => $this->_hostname]
        );
      }
    }

    EventManager::trigger(Events::CLAIM_RANGE_END);
    return $range;
  }

  public function processAll()
  {
    $this->_statsReporter->resetCounters();
    while(true)
    {
      $range = $this->claimNextFreeRange();
      if(!$range)
      {
        Log::notice('Ran out of ranges to process');
        break;
      }

      // Refresh the keys for this range
      try
      {
        $this->refreshKeysForRange($range);
        $success = true;
      }
      catch(CassandraException $e)
      {
        $success = false;
      }

      if(
        $success &&
        (($range->firstKey != "") || ($range->lastKey != "")) &&
        (! starts_with($range->lastKey, 'empty:')) &&
        ($range->firstKey != $range->lastKey)
      )
      {
        $this->processRange($range);
      }
      else
      {
        // Re-queue the range if there was an error getting the keys
        Log::error('Error getting the keys for range ' . $range->id());
        $this->_requeueRange($range);
      }
    }
  }

  /**
   * Re-queue a range with a new random key so it can be processed later
   *
   * @param TokenRange $range
   */
  private function _requeueRange(TokenRange $range)
  {
    EventManager::trigger(Events::REQUEUE_RANGE_START);
    Log::info('Re-queueing range ' . $range->id() . ' to process later');

    $range->processing     = 0;
    $range->processed      = 0;
    $range->processingTime = 0;
    $range->totalItems     = 0;
    $range->processedItems = 0;
    $range->errorCount     = 0;
    $range->randomKey      = rand(0, 10000);
    $range->hostname       = "";
    $range->saveChanges();
    EventManager::trigger(Events::REQUEUE_RANGE_END);
  }

  public function processRange(TokenRange $range)
  {
    Log::info(
      "Processing range ID " . $range->id(
      ) . " from '" . $range->firstKey . "' to '" . $range->lastKey . "'..."
    );

    $this->_batchSizeTuner->reset();
    $totalItems     = 0;
    $processedItems = 0;
    $errors         = 0;
    $rangeStartTime = microtime(true);
    $okToSave       = true;
    try
    {
      $cf   = $this->_getCF();
      $cols = $this->_processor->requiredColumns();

      $lastKey      = $range->firstKey ? $range->firstKey : "";
      $rangeLastKey = $range->lastKey ? $range->lastKey : "";
      $finished     = false;
      while(!$finished)
      {
        $this->_batchSizeTuner->nextBatch();
        $batchSize = $this->_batchSizeTuner->getBatchSize();
        $items = $this->_getKeysWithRetry(
          $cf, $lastKey, $rangeLastKey, $batchSize, $cols
        );
        //$items = $cf->getKeys($lastKey, $rangeLastKey, $batchSize, $cols);

        if(!$items)
        {
          Log::info("Found no more items in range");
          break;
        }

        // Skip the last item in the range because this will be the
        // first item in the next range
        if(($rangeLastKey != "") && (last_key($items) == $rangeLastKey))
        {
          array_pop($items);
        }

        if($this->_processor->supportsBatchProcessing())
        {
          try
          {
            $batchProcessed = $this->_processor->processBatch($items);
            $processedItems += $batchProcessed;
            $totalItems += count($items);
            $this->_statsReporter->processedItems += $batchProcessed;
            $this->_statsReporter->totalItems += count($items);
          }
          catch(BatchException $e)
          {
            $errors += $e->getErrorCount();
            $this->_statsReporter->errors += $e->getErrorCount();
            $msg = $e->getMessage();
            if($msg != "")
            {
              $msg = 'Error processing batch: ' . $msg;
            }

            Log::error($msg);
            if($this->_processor->stopOnErrors())
            {
              die();
            }
          }
        }
        else
        {
          foreach($items as $key => $itemData)
          {
            try
            {
              if($this->_processor->processItem($key, $itemData))
              {
                $processedItems++;
                $this->_statsReporter->processedItems++;
              }
            }
            catch(ItemException $e)
            {
              $errors++;
              $this->_statsReporter->errors++;
              Log::error(
                'Error processing item ' . $key . ' : ' . $e->getMessage()
              );
              if($this->_processor->stopOnErrors())
              {
                die();
              }
            }
            $totalItems++;
            $this->_statsReporter->totalItems++;
          }
        }

        $lastKey = last_key($items);

        if($this->_processor->shouldSaveProgress())
        {
          $this->_scriptProgress->save($range->firstKey, $lastKey);
        }

        if(($lastKey == $rangeLastKey) || (count($items) < $batchSize))
        {
          $finished = true;
        }

        $this->_statsReporter->displayReport(
          $finished,
          $range,
          $totalItems,
          $processedItems,
          $errors,
          $rangeStartTime,
          $lastKey
        );
      }

      $range->failed = $errors > 0 ? 1 : 0;
      $range->error  = "";
    }
    catch(\Exception $e)
    {
      $msg = "Code " . $e->getCode();
      $exMsg = $e->getMessage();
      if($exMsg != "")
      {
        $msg .= ": " . $exMsg;
      }
      Log::error(
        'Error processing range: ' . $msg . "\n\nBacktrace:\n" .
        $e->getTraceAsString()
      );

      // Check for non-fatal errors (i.e. Cassandra timeouts)
      if(
        ($e instanceof CassandraException)
        && (
          ($e->getCode() == 408) ||
          ($e->getCode() == 503) ||
          starts_with($e->getMessage(), 'TSocket: timed out')
        )
      )
      {
        $okToSave = false;
        $this->_requeueRange($range);
      }
      else
      {
        $range->error = $msg;
        $range->failed = 1;
      }
    }

    if($okToSave)
    {
      $range->processing     = 0;
      $range->processed      = 1;
      $range->processingTime = microtime(true) - $rangeStartTime;
      $range->totalItems     = $totalItems;
      $range->processedItems = $processedItems;
      $range->errorCount     = $errors;

      $range->saveChanges();
    }
  }

  public function listFailedRanges($limit)
  {
    $db = TokenRange::conn();
    $total = $db->getField(
      ParseQuery::parse(
        $db,
        'SELECT COUNT(*) FROM %T WHERE failed=1',
        TokenRange::tableName()
      )
    );

    if($total > 0)
    {
      $coll = new RecordCollection(new TokenRange());
      $coll->loadWhere(['failed' => 1])->setLimit(0, $limit);

      $table = new TextTable();
      $table->setColumnHeaders('id', 'hostname', 'error');

      foreach($coll as $range)
      {
        $table->appendRow([$range->id, $range->hostname, $range->error]);
      }
      echo "Displaying " . $coll->count() . " of " . $total . " failed ranges\n";
      echo $table;
    }
    else
    {
      echo "No failed ranges were found.\n";
    }
  }

  public function resetFailedRanges()
  {
    $db = TokenRange::conn();
    $tableName = TokenRange::tableName();

    echo "Resetting failed ranges...\n";

    $db->query(
      ParseQuery::parse(
        $db,
        "UPDATE %T SET processed=0, failed=0, hostname='' WHERE failed=1",
        $tableName
      )
    );

    echo "Finished. " . $db->affectedRows() . " ranges were reset.\n";
  }
}

