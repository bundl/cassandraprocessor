<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
use Cubex\Cli\Shell;
use Cubex\Facade\Cassandra;
use Cubex\Log\Log;
use Cubex\Mapper\Database\RecordCollection;
use Cubex\Mapper\Database\SearchObject;
use Cubex\Sprintf\ParseQuery;

class RangeManager
{
  private $_cassandraServiceName;
  private $_columnFamily;
  private $_cf;
  private $_processor;
  private $_minToken;
  private $_maxToken;

  private $_scriptProgress;
  private $_instanceName;
  private $_hostname;

  private $_statsReporter;

  public $batchSize;

  public function __construct(
    $cassandraServiceName, $columnFamily, ItemProcessor $processor,
    $instanceName = "", $displayReport = true
  )
  {
    $this->_cassandraServiceName = $cassandraServiceName;
    $this->_columnFamily         = $columnFamily;
    $this->_cf                   = null;
    $this->_processor            = $processor;
    $this->batchSize             = 50;
    $this->_scriptProgress       = new ScriptProgress();
    $this->_instanceName         = $instanceName;
    $this->_hostname             = gethostname();
    if($this->_instanceName != "")
    {
      $this->_hostname .= "-" . $this->_instanceName;
    }

    $this->_processor->sourceColumnFamily = $this->_getCF();

    // Work out the max token for this CF
    $this->_calcMinMaxTokens();

    $this->_statsReporter = new StatsReporter($this->_instanceName);
    $this->_statsReporter->displayPrettyReport = $displayReport;
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


  private function _getCF()
  {
    if(!$this->_cf)
    {
      $cass      = Cassandra::getAccessor($this->_cassandraServiceName);
      $this->_cf = $cass->cf($this->_columnFamily, false);
    }
    return $this->_cf;
  }

  public function buildRanges($numRanges)
  {
    echo "Creating ranges... ";
    $firstToken = $this->_minToken;
    $lastToken  = $this->_maxToken;

    // Delete all ranges from the DB
    $tableName = (new TokenRange())->getTableName();
    TokenRange::conn()->query('DELETE FROM ' . $tableName);
    TokenRange::conn()->query(
      'ALTER TABLE ' . $tableName . ' AUTO_INCREMENT=0'
    );

    $interval = bcdiv(bcsub($lastToken, $firstToken), $numRanges);

    $numCreated = 0;
    $prevToken  = "";
    for($tok = $firstToken; bccomp($tok, $lastToken) < 1; $tok = bcadd(
      $tok,
      $interval
    ))
    {
      if($prevToken !== "")
      {
        $range             = new TokenRange();
        $range->startToken = $prevToken;
        $range->endToken   = $tok;
        $range->saveChanges();
      }

      $numCreated++;
      Shell::clearLine();
      echo "Creating ranges... " . number_format(
        $numCreated
      ) . " / " . number_format($numRanges);

      $prevToken = $tok;
    }

    // Catch left over tokens
    if(bccomp(bcsub($lastToken, $prevToken), 0) == 1)
    {
      $range             = new TokenRange();
      $range->startToken = $prevToken;
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
      "UPDATE `" . $tableName . "` SET firstKey='', lastKey='', processing=0, hostname=NULL, processed=0, failed=0, " .
      "processingTime=0, totalItems=0, processedItems=0, errorCount=0, error=NULL"
    );
  }

  public function refreshKeysForRange(TokenRange $range)
  {
    $cf = $this->_getCF();

    $firstItem = $cf->getTokens($range->startToken, $range->startToken, 1);
    if($firstItem)
    {
      $range->firstKey = key($firstItem);

      $lastItem = $cf->getTokens($range->endToken, $range->endToken, 1);
      if($lastItem)
      {
        $range->lastKey = key($lastItem);
      }
      $range->saveChanges();
    }
  }


  /**
   * @return TokenRange
   */
  public function claimNextFreeRange()
  {
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
          "UPDATE token_ranges SET processing=1, hostname=%s " .
          "WHERE processing=0 AND processed=0 ORDER BY RAND() LIMIT 1",
          $this->_hostname
        )
      );

      if($res)
      {
        $range = TokenRange::loadWhere(
          ['processing' => 1, 'hostname' => $this->_hostname]
        );
      }
    }

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

      // Try a few times to refresh the keys if it fails
      for($i = 0; $i < 3; $i++)
      {
        $this->refreshKeysForRange($range);
        if(($range->firstKey != "") && ($range->lastKey != ""))
        {
          break;
        }
      }

      if($range->firstKey != "")
      {
        $this->processRange($range);
      }
      else
      {
        $range->processing     = 0;
        $range->processed      = 0;
        $range->processingTime = 0;
        $range->totalItems     = 0;
        $range->processedItems = 0;
        $range->errorCount     = 0;
        $range->hostname       = "";

        $range->saveChanges();
      }
    }
  }

  public function processRange(TokenRange $range)
  {
    Log::info(
      "Processing range ID " . $range->id(
      ) . " from '" . $range->firstKey . "' to '" . $range->lastKey . "'..."
    );

    $totalItems     = 0;
    $processedItems = 0;
    $errors         = 0;
    $rangeStartTime = microtime(true);
    try
    {
      $cf   = $this->_getCF();
      $cols = $this->_processor->requiredColumns();

      $lastKey      = $range->firstKey;
      $rangeLastKey = $range->lastKey ? $range->lastKey : "";
      $finished     = false;
      while(!$finished)
      {
        $items = $cf->getKeys($lastKey, $rangeLastKey, $this->batchSize, $cols);

        if(!$items)
        {
          Log::info("Found no more items in range");
          break;
        }

        // Skip the last item in the range because this will be the first item in the next range
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

        if(($lastKey == $rangeLastKey) || (count($items) < $this->batchSize))
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
      $range->failed = 1;
      $msg           = $e->getMessage();
      if($msg == "")
      {
        $msg = 'Exception code ' . $e->getCode();
      }
      $range->error = $msg;
      Log::error(
        'Error processing range: ' . $msg . "\n\nBacktrace:\n" . $e->getTraceAsString(
        )
      );
    }

    $range->processing     = 0;
    $range->processed      = 1;
    $range->processingTime = microtime(true) - $rangeStartTime;
    $range->totalItems     = $totalItems;
    $range->processedItems = $processedItems;
    $range->errorCount     = $errors;

    $range->saveChanges();
  }
}

