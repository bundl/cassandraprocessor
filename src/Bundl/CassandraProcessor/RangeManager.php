<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
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

  private $_startTime;
  private $_totalItems;
  private $_processedItems;
  private $_errors;
  private $_scriptProgress;
  private $_hostname;

  public $batchSize;
  public $reportInterval;

  public function __construct($cassandraServiceName, $columnFamily, ItemProcessor $processor, $hostname = "")
  {
    $this->_cassandraServiceName = $cassandraServiceName;
    $this->_columnFamily         = $columnFamily;
    $this->_cf                   = null;
    $this->_processor            = $processor;
    $this->batchSize             = 200;
    $this->reportInterval        = 10;
    $this->_scriptProgress       = new ScriptProgress();
    $this->_hostname             = $hostname == "" ? gethostname() : $hostname;

    $this->resetCounters();
  }

  public function resetCounters()
  {
    $this->_startTime      = time();
    $this->_totalItems     = 0;
    $this->_processedItems = 0;
    $this->_errors         = 0;
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
    echo "Creating ranges...\n";
    $partitionerType = $this->_getCF()->connection()->partitioner();
    switch($partitionerType)
    {
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        $firstToken = bcadd(bcpow("-2", "63"), "1");
        $lastToken  = bcsub(bcpow("2", "63"), "1");
        break;
      case "org.apache.cassandra.dht.RandomPartitioner":
        $firstToken = "0";
        $lastToken  = bcpow("2", "127");
        break;
      default:
        throw new \Exception('Unknown partitioner type: ' . $partitionerType);
    }

    // Delete all ranges from the DB
    $tableName = (new TokenRange())->getTableName();
    TokenRange::conn()->query('DELETE FROM ' . $tableName);
    TokenRange::conn()->query('ALTER TABLE ' . $tableName . ' AUTO_INCREMENT=0');

    $interval = bcdiv(bcsub($lastToken, $firstToken), $numRanges);

    $prevToken = "";
    for($tok = $firstToken; bccomp($tok, $lastToken) < 1; $tok = bcadd($tok, $interval))
    {
      if($prevToken !== "")
      {
        $range             = new TokenRange();
        $range->startToken = $prevToken;
        $range->endToken   = $tok;
        $range->saveChanges();
      }

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

    echo "Finished creating ranges.\n";
  }

  public function refreshKeysForRange(TokenRange $range)
  {
    $cf = $this->_getCF();

    $firstItem = $cf->getTokens($range->startToken, $range->endToken, 1);
    if($firstItem)
    {
      $range->firstKey = key($firstItem);
    }

    $otherRange = TokenRange::loadWhere(['startToken' => $range->endToken]);
    if($otherRange)
    {
      $lastItem = $cf->getTokens($otherRange->startToken, $otherRange->endToken, 1);
      if($lastItem)
      {
        $range->lastKey = key($lastItem);
      }
    }

    $range->saveChanges();
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
    $coll->loadWhere(['processing' => 1, 'hostname' => $this->_hostname])->limit(1);

    if($coll->count() > 0)
    {
      $range = $coll->first();
    }
    else
    {
      $res = $db->query(
        ParseQuery::parse(
          $db,
          "UPDATE token_ranges SET processing=1, hostname=%s WHERE processing=0 AND processed=0 " .
          "AND firstKey<>'' AND firstKey IS NOT NULL LIMIT 1",
          $this->_hostname
        )
      );

      if($res)
      {
        $range = TokenRange::loadWhere(['processing' => 1, 'hostname' => $this->_hostname]);
      }
    }

    return $range;
  }

  public function processAll()
  {
    $this->resetCounters();
    while(true)
    {
      $range = $this->claimNextFreeRange();
      if(!$range)
      {
        Log::notice('Ran out of ranges to process');
        break;
      }

      $this->refreshKeysForRange($range);
      $this->processRange($range);
    }
  }

  public function processRange(TokenRange $range)
  {
    Log::info("Processing range from '" . $range->firstKey . "' to '" . $range->lastKey . "'...");

    $totalItems     = 0;
    $processedItems = 0;
    $errors         = 0;
    $lastReportTime = $rangeStartTime = time();
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
            $this->_processedItems += $batchProcessed;
            $totalItems += count($items);
            $this->_totalItems += count($items);
          }
          catch(BatchException $e)
          {
            $errors += $e->getErrorCount();
            $this->_errors += $e->getErrorCount();
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
                $this->_processedItems++;
              }
            }
            catch(ItemException $e)
            {
              $errors++;
              $this->_errors++;
              Log::error('Error processing item ' . $key . ' : ' . $e->getMessage());
              if($this->_processor->stopOnErrors())
              {
                die();
              }
            }
            $totalItems++;
            $this->_totalItems++;
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

        $now = time();
        if($finished || (($now - $lastReportTime) >= $this->reportInterval))
        {
          $lastReportTime = $now;
          $this->displayReport($totalItems, $processedItems, $errors, $rangeStartTime, $lastKey);
        }
      }

      $range->failed = $errors > 0 ? 1 : 0;
      $range->error  = "";
    }
    catch(\Exception $e)
    {
      $range->failed = 1;
      $range->error  = $e->getMessage();
      Log::error('Error processing range: ' . $e->getMessage());
    }

    $range->processing     = 0;
    $range->processed      = 1;
    $range->processingTime = time() - $rangeStartTime;
    $range->totalItems     = $totalItems;
    $range->processedItems = $processedItems;
    $range->errorCount     = $errors;

    $range->saveChanges();
  }

  public function displayReport($rangeTotal, $rangeProcessed, $rangeErrors, $rangeStartTime, $lastKey)
  {
    static $lastRangeTotal = 0;
    static $lastReportTime = 0;

    $now = time();

    $batchTotal = $rangeTotal - $lastRangeTotal;
    if($lastReportTime == 0)
    {
      $lastReportTime = $rangeStartTime;
    }
    $batchDuration = $now - $lastReportTime;

    $totalDuration = $now - $this->_startTime;
    if($totalDuration > 0)
    {
      $averageRate = round($this->_totalItems / $totalDuration);
    }
    else
    {
      $averageRate = 0;
    }
    if($batchDuration > 0)
    {
      $currentRate = round($batchTotal / $batchDuration);
    }
    else
    {
      $currentRate = 0;
    }

    $lastReportTime = $now;

    Log::info(
      "CURRENT RANGE: Run time " . $this->_secsToTime($now - $rangeStartTime) .
      ", Processed " . $rangeProcessed . " of " . $rangeTotal . " items, " . $rangeErrors . " errors"
    );
    Log::info(
      "OVERALL: Run time " . $this->_secsToTime($totalDuration) .
      ", Processed " . $this->_processedItems . " of " . $this->_totalItems . " items, " .
      $this->_errors . " errors"
    );
    Log::info(
      "Current rate: " . $currentRate . " items/second, Average rate: " . $averageRate . " items/second"
    );
    Log::info("Last key: " . $lastKey);
  }


  private function _secsToTime($secs)
  {
    $hours = floor($secs / 3600);
    $secs -= $hours * 3600;
    $mins = floor($secs / 60);
    $secs -= $mins * 60;

    return sprintf("%d:%02d:%02d", $hours, $mins, $secs);
  }
}

