<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
use Cubex\Cli\CliLogger;
use Cubex\Cli\Shell;
use Cubex\Events\EventManager;
use Cubex\Helpers\DateTimeHelper;
use Cubex\Log\Log;
use Cubex\Text\ReportTableDecorator;
use Cubex\Text\TextTable;

class StatsReporter
{
  private $_reportInterval = 15;
  private $_instanceName;

  private $_startTime;
  public $totalItems;
  public $processedItems;
  public $errors;
  public $displayPrettyReport;

  public function __construct($instanceName = "")
  {
    $this->_instanceName = $instanceName;
    $this->displayPrettyReport = true;
    $this->resetCounters();
  }

  public function resetCounters()
  {
    $this->_startTime     = microtime(true);
    $this->totalItems     = 0;
    $this->processedItems = 0;
    $this->errors         = 0;
  }


  public function displayReport(
    $forceLog, TokenRange $currentRange, $rangeTotal, $rangeProcessed,
    $rangeErrors, $rangeStartTime, $lastKey
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
      $averageRate = round($this->totalItems / $totalDuration);
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

    $rangeSkipped = $rangeTotal - ($rangeProcessed + $rangeErrors);
    $totalSkipped = $this->totalItems - ($this->processedItems + $this->errors);

    // Log the stats periodically
    if($forceLog || (($now - $lastReportTime) >= $this->_reportInterval))
    {
      $lastReportTime = $now;
      $this->_logReport(
        $now,
        $rangeStartTime,
        $rangeProcessed,
        $rangeTotal,
        $rangeErrors,
        $totalDuration,
        $currentRate,
        $averageRate,
        $lastKey
      );
    }

    // Display a nice report
    $prettyReport = $this->_generatePrettyReport(
      $currentRange,
      $now,
      $rangeStartTime,
      $rangeTotal,
      $rangeProcessed,
      $rangeErrors,
      $currentRate,
      $rangeSkipped,
      $totalSkipped,
      $totalDuration,
      $averageRate,
      $lastKey
    );

    if($this->displayPrettyReport)
    {
      Shell::redrawScreen($prettyReport);
    }

    // Store the pretty report in a file
    $logsDir = CliLogger::getDefaultLogPath($this->_instanceName);
    $reportFile = $logsDir . DS . 'report.txt';
    file_put_contents($reportFile, $prettyReport);


    // Store the raw stats
    $rawStats = json_encode(
      [
        'hostname' => $currentRange->hostname,
        'rangeId' => $currentRange->id(),
        'startToken' => $currentRange->startToken,
        'endToken' => $currentRange->endToken,
        'rangeFirstKey' => $currentRange->firstKey,
        'rangeLastKey' => $currentRange->lastKey,
        'timestamp' => $now,
        'rangeStartTime' => $rangeStartTime,
        'rangeTotal' => $rangeTotal,
        'rangeProcessed' => $rangeProcessed,
        'rangeErrors' => $rangeErrors,
        'rangeSkipped' => $rangeSkipped,
        'currentRate' => $currentRate,
        'totalItems' => $this->totalItems,
        'totalProcessed' => $this->processedItems,
        'totalErrors' => $this->errors,
        'totalSkipped' => $totalSkipped,
        'totalDuration' => $totalDuration,
        'averageRate' => $averageRate,
        'lastKey' => $lastKey
      ]
    );

    $rawStatsFile = $logsDir . DS . 'stats.txt';
    file_put_contents($rawStatsFile, $rawStats);
  }

  private function _logReport(
    $now, $rangeStartTime, $rangeProcessed, $rangeTotal, $rangeErrors,
    $totalDuration, $currentRate, $averageRate, $lastKey
  )
  {
    // Log the stats
    Log::info(
      "CURRENT RANGE: Run time " . DateTimeHelper::secondsToTime(
        $now - $rangeStartTime
      ) .
      ", Processed " . $rangeProcessed . " of " .
      $rangeTotal . " items, " . $rangeErrors . " errors"
    );
    Log::info(
      "OVERALL: Run time " . DateTimeHelper::secondsToTime($totalDuration) .
      ", Processed " . $this->processedItems . " of " .
      $this->totalItems . " items, " .
      $this->errors . " errors"
    );
    Log::info(
      "Current rate: " . $currentRate . " items/second, Average rate: " .
      $averageRate . " items/second"
    );
    Log::info("Last key: " . $lastKey);
  }


  private function _generatePrettyReport(
    TokenRange $currentRange, $now, $rangeStartTime, $rangeTotal,
    $rangeProcessed,
    $rangeErrors, $currentRate, $rangeSkipped, $totalSkipped, $totalDuration,
    $averageRate, $lastKey
  )
  {
    $t = new TextTable(new ReportTableDecorator());

    $t->appendSubHeading(
      'Current Range ' . Shell::colourText(
        "(" . $currentRange->id() . ")",
        Shell::COLOUR_FOREGROUND_LIGHT_GREY
      )
    );
    $t->appendRows(
      [
        ['Start token', $currentRange->startToken],
        ['End token', $currentRange->endToken],
        ['First key', $currentRange->firstKey],
        ['Last key', $currentRange->lastKey]
      ]
    );
    $t->appendSubHeading('Range statistics');
    $t->appendRows(
      [
        [
          'Processing time',
          DateTimeHelper::secondsToTime($now - $rangeStartTime)
        ],
        ['Total items', number_format($rangeTotal)],
        ['Processed items', number_format($rangeProcessed)],
        ['Skipped', number_format($rangeSkipped)],
        ['Errors', number_format($rangeErrors)],
        ['Processing rate', number_format($currentRate) . ' items/second']
      ]
    );
    $t->appendSubHeading('Total');
    $t->appendRows(
      [
        ['Processing time', DateTimeHelper::secondsToTime($totalDuration)],
        ['Total items', number_format($this->totalItems)],
        ['Processed items', number_format($this->processedItems)],
        ['Skipped', number_format($totalSkipped)],
        ['Errors', number_format($this->errors)],
        ['Processing rate', number_format($averageRate) . ' items/second']
      ]
    );
    $t->appendSpacer();
    $t->appendRow(['Last key processed', $lastKey]);

    ob_start();
    echo $t;
    EventManager::trigger(Events::DISPLAY_REPORT_END);
    return ob_get_clean();
  }
}
