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
    $this->batchSize             = 50;
    $this->reportInterval        = 10;
    $this->_scriptProgress       = new ScriptProgress();
    $this->_hostname             = $hostname == "" ? gethostname() : $hostname;

    // Work out the max token for this CF
    $this->_calcMinMaxTokens();

    $this->resetCounters();
  }

  private function _calcMinMaxTokens()
  {
    $partitionerType = $this->_getCF()->connection()->partitioner();
    switch($partitionerType)
    {
      case "org.apache.cassandra.dht.Murmur3Partitioner":
        $this->_minToken = bcadd(bcpow("-2", "63"), "1");
        $this->_maxToken  = bcsub(bcpow("2", "63"), "1");
        break;
      case "org.apache.cassandra.dht.RandomPartitioner":
        $this->_minToken = "0";
        $this->_maxToken  = bcpow("2", "127");
        break;
      default:
        throw new \Exception('Unknown partitioner type: ' . $partitionerType);
    }
  }

  public function resetCounters()
  {
    $this->_startTime      = microtime(true);
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
    echo "Creating ranges... ";
    $firstToken = $this->_minToken;
    $lastToken = $this->_maxToken;

    // Delete all ranges from the DB
    $tableName = (new TokenRange())->getTableName();
    TokenRange::conn()->query('DELETE FROM ' . $tableName);
    TokenRange::conn()->query('ALTER TABLE ' . $tableName . ' AUTO_INCREMENT=0');

    $interval = bcdiv(bcsub($lastToken, $firstToken), $numRanges);

    $numCreated = 0;
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

      $numCreated++;
      Shell::clearLine();
      echo "Creating ranges... " . number_format($numCreated) . " / " . number_format($numRanges);

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
    $conn = TokenRange::conn();
    $conn->query(
      "UPDATE `" . $tableName . "` SET firstKey='', lastKey='', processing=0, hostname=NULL, processed=0, failed=0, " .
      "processingTime=0, totalItems=0, processedItems=0, errorCount=0, error=NULL"
    );
  }

  public function refreshKeysForRange(TokenRange $range)
  {
    $cf = $this->_getCF();

    $firstItem = $cf->getTokens($range->startToken, $range->endToken, 1);
    if($firstItem)
    {
      $range->firstKey = key($firstItem);
    }

    $foundLastKey = false;
    $otherRangeStart = $range->endToken;
    while(! $foundLastKey)
    {
      $otherRange = TokenRange::loadWhere(['startToken' => $otherRangeStart]);
      if($otherRange)
      {
        $lastItem = $cf->getTokens($otherRange->startToken, $otherRange->endToken, 1);
        if($lastItem)
        {
          $lastKey = key($lastItem);
          if($lastKey || ($otherRange->endToken == $this->_maxToken))
          {
            $range->lastKey = $lastKey;
            $foundLastKey = true;
          }
        }
        $otherRangeStart = $otherRange->endToken;
      }
      else if(($range->endToken == $this->_maxToken) || ($otherRangeStart == $this->_maxToken))
      {
        $range->lastKey = "";
        $foundLastKey = true;
      }
      else
      {
        echo "Error getting next token range (looking for range starting with token " . $otherRangeStart . ")\n";
        echo "\nThis range:\n";
        var_dump_json($range);
        die;
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
          "UPDATE token_ranges SET processing=1, hostname=%s " .
          "WHERE processing=0 AND processed=0 ORDER BY RAND() LIMIT 1",
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
      if($range->firstKey != "")
      {
        $this->processRange($range);
      }
      else
      {
        $range->processing     = 0;
        $range->processed      = 1;
        $range->processingTime = 0;
        $range->totalItems     = 0;
        $range->processedItems = 0;
        $range->errorCount     = 0;

        $range->saveChanges();
      }
    }
  }

  public function processRange(TokenRange $range)
  {
    Log::info(
      "Processing range ID " . $range->id() . " from '" . $range->firstKey . "' to '" . $range->lastKey . "'..."
    );

    $totalItems     = 0;
    $processedItems = 0;
    $errors         = 0;
    $lastReportTime = $rangeStartTime = microtime(true);
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

        $this->displayReport($finished, $range, $totalItems, $processedItems, $errors, $rangeStartTime, $lastKey);
      }

      $range->failed = $errors > 0 ? 1 : 0;
      $range->error  = "";
    }
    catch(\Exception $e)
    {
      $range->failed = 1;
      $msg = $e->getMessage();
      if($msg == "")
      {
        $msg = 'Exception code ' . $e->getCode();
      }
      $range->error  = $msg;
      Log::error('Error processing range: ' . $msg . "\n\nBacktrace:\n" . $e->getTraceAsString());
    }

    $range->processing     = 0;
    $range->processed      = 1;
    $range->processingTime = microtime(true) - $rangeStartTime;
    $range->totalItems     = $totalItems;
    $range->processedItems = $processedItems;
    $range->errorCount     = $errors;

    $range->saveChanges();
  }

  public function displayReport(
    $forceLog, TokenRange $currentRange, $rangeTotal, $rangeProcessed, $rangeErrors, $rangeStartTime, $lastKey
  )
  {
    static $lastRangeTotal = 0;
    static $lastReportTime = 0;

    $now = microtime(true);

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

    if($forceLog || (($now - $lastReportTime) >= $this->reportInterval))
    {
      $lastReportTime = $now;
      // Log the stats
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

    // Display a nice report...
    Shell::clear();
    $this->_displayReportHeader('Current Range', false);
    echo Shell::colourText(" (" . $currentRange->id() . ")", Shell::COLOUR_FOREGROUND_LIGHT_GREY) . "\n";
    $this->_displayReportLine('Start token', $currentRange->startToken);
    $this->_displayReportLine('End token', $currentRange->endToken);
    $this->_displayReportLine('First key', $currentRange->firstKey);
    $this->_displayReportLine('Last key', $currentRange->lastKey);
    $this->_displayReportHeader('Range statistics');
    $this->_displayReportLine('Processing time', $this->_secsToTime($now - $rangeStartTime));
    $this->_displayReportLine('Total items', $rangeTotal);
    $this->_displayReportLine('Processed items', $rangeProcessed);
    $this->_displayReportLine('Skipped', ($rangeTotal - ($rangeProcessed + $rangeErrors)));
    $this->_displayReportLine('Errors', $rangeErrors);
    $this->_displayReportLine('Processing rate', $currentRate . ' items/second');
    $this->_displayReportHeader('Total');
    $this->_displayReportLine('Processing time', $this->_secsToTime($totalDuration));
    $this->_displayReportLine('Total items', $this->_totalItems);
    $this->_displayReportLine('Processed items', $this->_processedItems);
    $this->_displayReportLine('Skipped', ($this->_totalItems - ($this->_processedItems + $this->_errors)));
    $this->_displayReportLine('Errors', $this->_errors);
    $this->_displayReportLine('Processing rate', $averageRate . ' items/second');
    echo "\n";
    $this->_displayReportLine('Last key processed', $lastKey);
  }


  private function _displayReportHeader($text, $newLine = true)
  {
    echo "\n ";
    echo Shell::colourText($text, Shell::COLOUR_FOREGROUND_LIGHT_RED);
    if($newLine) echo "\n";
  }

  private function _displayReportLine($label, $value)
  {
    $labelSize = 18;
    $labelColour = Shell::COLOUR_FOREGROUND_LIGHT_GREEN;
    $colonColour = Shell::COLOUR_FOREGROUND_YELLOW;
    $valueColour = Shell::COLOUR_FOREGROUND_WHITE;

    $labelTxt = $label . str_repeat(" ", $labelSize - strlen($label));
    echo "  " . Shell::colourText($labelTxt, $labelColour);
    echo Shell::colourText(" : ", $colonColour);
    echo Shell::colourText($value, $valueColour);
    echo "\n";
  }


  private function _secsToTime($secs)
  {
    $secs = round($secs);
    $hours = floor($secs / 3600);
    $secs -= $hours * 3600;
    $mins = floor($secs / 60);
    $secs -= $mins * 60;

    return sprintf("%d:%02d:%02d", $hours, $mins, $secs);
  }
}

