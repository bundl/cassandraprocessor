<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
use cassandra\SlicePredicate;
use Cubex\Cli\Shell;
use Cubex\Database\IDatabaseService;
use Cubex\Events\EventManager;
use Cubex\Facade\Cassandra;
use Cubex\Cassandra\CassandraException;
use Cubex\Cassandra\ColumnFamily;
use Cubex\Cassandra\DataType\BytesType;
use Cubex\Cassandra\DataType\CassandraType;
use Cubex\Helpers\DateTimeHelper;
use Cubex\Log\Log;
use Cubex\Mapper\Database\RecordCollection;
use Cubex\Mapper\Database\SearchObject;
use Cubex\Sprintf\ParseQuery;
use Cubex\Text\TextTable;
use cassandra\ConsistencyLevel;

class RangeManager
{
  const MAX_REQUEUES = 50;

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
  private $_writeConsistencyLevel;
  private $_cassReceiveTimeout;
  private $_cassSendTimeout;
  private $_columnDataType;

  private $_scriptProgress;
  private $_instanceName;
  private $_hostname;
  private $_statsReporter;
  private $_batchSizeTuner;
  private $_lastStartKey;

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
    $this->_writeConsistencyLevel = ConsistencyLevel::QUORUM;
    $this->_cassSendTimeout      = 2000;
    $this->_cassReceiveTimeout   = 2000;
    $this->_columnDataType       = new BytesType();
    if($this->_instanceName != "")
    {
      $this->_hostname .= "|" . $this->_instanceName;
    }

    $this->_minToken = null;
    $this->_maxToken = null;

    $this->_statsReporter = new StatsReporter($this->_instanceName);
    $this->_statsReporter->displayPrettyReport = $displayReport;

    $this->_batchSizeTuner        = new BatchSizeTuner();
    $this->_batchSizeTuner->setBatchSizeLimitsArr(
      $this->_processor->getBatchSize()
    );

    $this->_lastStartKey = 0;
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

  private function _minToken()
  {
    if($this->_minToken === null)
    {
      $this->_calcMinMaxTokens();
    }
    return $this->_minToken;
  }

  private function _maxToken()
  {
    if($this->_maxToken === null)
    {
      $this->_calcMinMaxTokens();
    }
    return $this->_maxToken;
  }

  public function getCF()
  {
    return $this->_getCF();
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
      $this->setWriteConsistencyLevel($this->_writeConsistencyLevel);
      $this->setCassTimeout(
        $this->_cassSendTimeout,
        $this->_cassReceiveTimeout
      );
      $this->_processor->sourceColumnFamily = $this->_cf;

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

  public function setWriteConsistencyLevel($consistencyLevel)
  {
    $this->_writeConsistencyLevel = $consistencyLevel;
    if($this->_cf)
    {
      $this->_cf->setWriteConsistencyLevel($consistencyLevel);
    }
  }

  public function buildRanges($numRanges)
  {
    echo "Creating ranges... ";
    $firstToken = $this->_minToken();
    $lastToken  = $this->_maxToken();

    // Delete all ranges from the DB
    $db = TokenRange::conn();
    $tableName = (new TokenRange())->getTableName();
    if((new TokenRange())->tableExists())
    {
      $db->query(ParseQuery::parse($db, 'DELETE FROM %T', $tableName));
      $db->query(
        ParseQuery::parse($db, 'ALTER TABLE %T AUTO_INCREMENT=0', $tableName)
      );
    }
    else
    {
      (new TokenRange())->createTable();
    }

    $interval = bcdiv(bcsub($lastToken, $firstToken), $numRanges);

    $batchSize = 1000;
    $numCreated = 0;
    $prevToken  = "";
    $lastRange = null;
    $batchCount = 0;
    $batchData = [];
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
        $batchData[] = $range;
        $lastRange = $range;

        $batchCount++;
        $numCreated++;
      }

      if($batchCount >= $batchSize)
      {
        $this->_insertMultipleRanges($batchData);
        $batchData = [];
        $batchCount = 0;

        Shell::clearLine();
        echo "Creating ranges... " .
          number_format($numCreated) . " / " . number_format($numRanges);
      }

      $prevToken = $tok;
    }
    if(count($batchData) > 0)
    {
      $this->_insertMultipleRanges($batchData);

      Shell::clearLine();
      echo "Creating ranges... " .
        number_format($numCreated) . " / " . number_format($numRanges);
    }

    // Catch left over tokens and add them to the last range
    if(bccomp(bcsub($lastToken, $prevToken), 0) == 1)
    {
      if($lastRange)
      {
        $range = (new RecordCollection(new TokenRange()))
          ->loadOneWhere(['startToken' => $lastRange->startToken]);
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

  /**
   * @param TokenRange[] $ranges
   */
  private function _insertMultipleRanges($ranges)
  {
    $db = TokenRange::conn();
    $tableName = (new TokenRange())->getTableName();

    $query = ParseQuery::parse(
      $db,
      'INSERT INTO %T (startToken, endToken, randomKey, createdAt, updatedAt) VALUES ',
      $tableName
    );

    $data = [];
    foreach($ranges as $range)
    {
      $nowStr = DateTimeHelper::formattedDateFromAnything(time());
      $data[] = ParseQuery::parse(
        $db,
        "(%s, %s, %d, %s, %s)",
        $range->startToken,
        $range->endToken,
        $range->randomKey,
        $nowStr,
        $nowStr
      );
    }

    $query .= implode(", ", $data);
    Log::debug($query);

    $db->query($query);
  }

  public function resetRanges()
  {
    $count = $this->_multiQuery(
      TokenRange::conn(),
      "UPDATE %T SET firstKey='', lastKey='', processing=0, requeueCount=0," .
      "hostname=NULL, processed=0, failed=0, processingTime=0, " .
      "totalItems=0, processedItems=0, errorCount=0, error=NULL",
      $this->listAllRangeTables()
    );

    echo "Finished. Reset " . $count . " ranges\n";
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
    $range->requeueCount   = 0;

    $range->saveChanges();
  }

  /**
   * For testing only: refresh the keys for all ranges
   */
  public function refreshKeysForAllRanges()
  {
    $result = $this->_multiGetRows(
      TokenRange::conn(),
      'SELECT id FROM %T',
      $this->listAllRangeTables()
    );

    $ids = [];
    foreach($result as $row)
    {
      $ids[] = $row->id;
    }

    $total = count($ids);
    $processed = 0;
    foreach($ids as $id)
    {
      $range = new TokenRange($id);
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
      $cf, $this->_minToken(), $this->_minToken(), 1
    );
    $firstKeyInCF = key($firstItemInCF);
    $lastItemInCF = $this->_getTokensWithRetry(
      $cf, $this->_maxToken(), $this->_maxToken(), 1
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
      $startKey = $this->_lastStartKey;
      $res = false;
      while(true)
      {
        $endKey = $startKey + 100;

        $res = $db->query(
          ParseQuery::parse(
            $db,
            "UPDATE %T SET processing=1, hostname=%s " .
            "WHERE processing=0 AND processed=0 AND " .
            "randomKey BETWEEN %d AND %d " .
            "ORDER BY randomKey LIMIT 1",
            (new TokenRange())->getTableName(), $this->_hostname,
            $startKey, $endKey
          )
        );

        if(($res && ($db->affectedRows() > 0)) || ($startKey > 10000))
        {
          $this->_lastStartKey = $startKey;
          break;
        }
        $startKey += 100;
      }

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
    Log::info('Cassandra Processor started');
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
        $range->error = 'Error getting keys for range';
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
    $range->processing     = 0;
    $range->processingTime = 0;

    if($range->requeueCount > self::MAX_REQUEUES)
    {
      $range->failed    = 1;
      $range->processed = 1;
      $range->saveChanges();
    }
    else
    {
      EventManager::trigger(Events::REQUEUE_RANGE_START);
      Log::info('Re-queueing range ' . $range->id() . ' to process later');

      $range->processed      = 0;
      $range->totalItems     = 0;
      $range->processedItems = 0;
      $range->errorCount     = 0;
      $range->randomKey      = rand(0, 10000);
      $range->requeueCount   = $range->requeueCount + 1;
      $range->saveChanges();
      EventManager::trigger(Events::REQUEUE_RANGE_END);
    }
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
    $this->_processor->resetRangeData();
    try
    {
      $cf   = $this->_getCF();
      $cols = $this->_processor->requiredColumns();
      if(is_numeric($cols))
      {
        $cols = new SlicePredicate(
          ['slice_range' => $cf->makeSlice('', '', false, $cols)]
        );
      }

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
            EventManager::trigger(Events::PROCESS_BATCH_START);
            $batchProcessed = $this->_processor->processBatch($items);
            EventManager::trigger(Events::PROCESS_BATCH_END);

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

        if($errors > 0)
        {
          break;
        }
      }

      $rangeData = $this->_processor->getRangeData();
      $range->rangeData = $rangeData ? json_encode($rangeData) : '';
      $range->failed = 0;
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

      $range->error = $msg;

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
        $errors++;
      }
      else
      {
        $range->failed = 1;
      }
    }

    if($errors > 0)
    {
      $this->_requeueRange($range);
    }
    else
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
    $result = $this->_multiGetRows(
      TokenRange::conn(),
      "SELECT id, updatedAt, hostname, error FROM %T WHERE failed=1",
      $this->listAllRangeTables(),
      $limit
    );

    if(count($result) > 0)
    {
      $this->_displayRangeList($result);
    }
    else
    {
      echo "No failed ranges were found.\n";
    }
  }

  public function resetFailedRanges()
  {
    echo "Resetting failed ranges...\n";

    $affectedRows = $this->_multiQuery(
      TokenRange::conn(),
      "UPDATE %T SET processed=0, failed=0, hostname='', requeueCount=0 WHERE failed=1",
      $this->listAllRangeTables()
    );

    echo "Finished. " . $affectedRows . " ranges were reset.\n";
  }

  /**
   * List the ranges that were requeued within the last $mins minutes
   *
   * @param int $mins
   */
  public function listRequeuedRanges($mins = 30)
  {
    $since = date('Y-m-d H:i:s', time() - ($mins * 60));

    $result = $this->_multiGetRows(
      TokenRange::conn(),
      "SELECT id, updatedAt, hostname, error FROM %T " .
      "WHERE requeueCount>0 AND processed=0 AND failed=0 AND updatedAt >= '" . $since . "'",
      $this->listAllRangeTables()
    );

    if(count($result) > 0)
    {
      $this->_displayRangeList($result);
    }
    else
    {
      echo "No requeued ranges were found.\n";
    }
  }

  /**
   * @param TokenRange[] $ranges
   */
  private function _displayRangeList(array $ranges)
  {
    $table = new TextTable();
    $table->setColumnHeaders('id', 'updatedAt', 'hostname', 'error');

    foreach($ranges as $range)
    {
      $table->appendRow(
        [$range->id, $range->updatedAt, $range->hostname, $range->error]
      );
    }
    echo $table;
  }

  public function resetProcessingRanges()
  {
    echo "Resetting processing ranges...\n";

    $affectedRows = $this->_multiQuery(
      TokenRange::conn(),
      "UPDATE %T SET processing=0, processed=0, failed=0, hostname='' " .
      "WHERE processing=1",
      $this->listAllRangeTables()
    );

    echo "Finished. " . $affectedRows . " ranges were reset.\n";
  }

  protected function _multiQuery(IDatabaseService $db, $query, $tables)
  {
    $affectedRows = 0;
    foreach($tables as $table)
    {
      $db->query(ParseQuery::parse($db, $query, $table));
      $affectedRows += $db->affectedRows();
    }
    return $affectedRows;
  }

  protected function _multiGetRows(
    IDatabaseService $db, $query, $tables, $limit = 0
  )
  {
    $result = [];
    foreach($tables as $table)
    {
      $res = $db->getRows(ParseQuery::parse($db, $query, $table));
      $result = array_merge($result, $res);
      $numResults = count($result);
      if(($limit > 0) && ($numResults >= $limit))
      {
        if($numResults > $limit)
        {
          $result = array_slice($result, 0, $limit);
        }
        break;
      }
    }
    return $result;
  }

  /**
   * Get a list of all Token Ranges tables that could be used by this script.
   * In most cases this will only return 1 table but if the script is using
   * sharded tables then this will return all of them.
   *
   * @return string[]
   */
  public function listAllRangeTables()
  {
    $tables = [];

    $db = TokenRange::conn();
    $tableName = TokenRange::tableName();
    if(strpos($tableName, '_') !== false)
    {
      $parts = explode("_", $tableName);
      $last = array_pop($parts);
      if(is_numeric($last))
      {
        $tableBase = implode("_", $parts) . '_';

        $i = 1;
        while(true)
        {
          $table = $tableBase . $i;
          $res = $db->numRows("SHOW TABLES LIKE '" . $table . "'");
          if($res > 0)
          {
            $tables[] = $table;
          }
          else
          {
            break;
          }
          $i++;
        }
      }
    }

    if(count($tables) == 0)
    {
      $tables = [$tableName];
    }

    return $tables;
  }
}

