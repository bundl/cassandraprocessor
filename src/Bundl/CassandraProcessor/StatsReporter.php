<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
use Cubex\Cli\Shell;
use Cubex\Events\EventManager;
use Cubex\Log\Log;

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
    $logsDir = realpath(dirname(WEB_ROOT)) . DS . 'logs';
    if($this->_instanceName != "")
    {
      $logsDir .= DS . $this->_instanceName;
    }
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
      "CURRENT RANGE: Run time " . $this->_secsToTime(
        $now - $rangeStartTime
      ) .
      ", Processed " . $rangeProcessed . " of " . $rangeTotal . " items, " . $rangeErrors . " errors"
    );
    Log::info(
      "OVERALL: Run time " . $this->_secsToTime($totalDuration) .
      ", Processed " . $this->processedItems . " of " . $this->totalItems . " items, " .
      $this->errors . " errors"
    );
    Log::info(
      "Current rate: " . $currentRate . " items/second, Average rate: " . $averageRate . " items/second"
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
    ob_start();
    EventManager::trigger(Events::DISPLAY_REPORT_START);

    $this->_displayReportHeader('Current Range', false);
    echo Shell::colourText(
      " (" . $currentRange->id() . ")",
      Shell::COLOUR_FOREGROUND_LIGHT_GREY
    ) . "\n";
    $this->_displayReportLine('Start token', $currentRange->startToken);
    $this->_displayReportLine('End token', $currentRange->endToken);
    $this->_displayReportLine('First key', $currentRange->firstKey);
    $this->_displayReportLine('Last key', $currentRange->lastKey);
    $this->_displayReportHeader('Range statistics');
    $this->_displayReportLine(
      'Processing time',
      $this->_secsToTime($now - $rangeStartTime)
    );
    $this->_displayReportLine('Total items', $rangeTotal);
    $this->_displayReportLine('Processed items', $rangeProcessed);
    $this->_displayReportLine('Skipped', $rangeSkipped);
    $this->_displayReportLine('Errors', $rangeErrors);
    $this->_displayReportLine(
      'Processing rate',
      $currentRate . ' items/second'
    );
    $this->_displayReportHeader('Total');
    $this->_displayReportLine(
      'Processing time',
      $this->_secsToTime($totalDuration)
    );
    $this->_displayReportLine('Total items', $this->totalItems);
    $this->_displayReportLine('Processed items', $this->processedItems);
    $this->_displayReportLine('Skipped', $totalSkipped);
    $this->_displayReportLine('Errors', $this->errors);
    $this->_displayReportLine(
      'Processing rate',
      $averageRate . ' items/second'
    );
    echo "\n";
    $this->_displayReportLine('Last key processed', $lastKey);

    EventManager::trigger(Events::DISPLAY_REPORT_END);

    // Store the pretty report
    return ob_get_clean();
  }


  private function _displayReportHeader($text, $newLine = true)
  {
    echo "\n ";
    echo Shell::colourText($text, Shell::COLOUR_FOREGROUND_LIGHT_RED);
    if($newLine)
    {
      echo "\n";
    }
  }

  private function _displayReportLine($label, $value)
  {
    $labelSize   = 18;
    $labelColour = Shell::COLOUR_FOREGROUND_LIGHT_GREEN;
    $colonColour = Shell::COLOUR_FOREGROUND_YELLOW;
    $valueColour = Shell::COLOUR_FOREGROUND_WHITE;

    $labelTxt = $label . str_repeat(" ", $labelSize - strlen($label));
    echo "  " . Shell::colourText($labelTxt, $labelColour);
    echo Shell::colourText(" : ", $colonColour);
    echo Shell::colourText($value, $valueColour);
    Shell::clearToEol();
    echo "\n";
  }


  private function _secsToTime($secs)
  {
    $secs  = round($secs);
    $hours = floor($secs / 3600);
    $secs -= $hours * 3600;
    $mins = floor($secs / 60);
    $secs -= $mins * 60;

    return sprintf("%d:%02d:%02d", $hours, $mins, $secs);
  }
}
