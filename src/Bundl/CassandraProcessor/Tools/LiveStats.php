<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor\Tools;

use Bundl\CassandraProcessor\CassProcessorTask;
use Bundl\CassandraProcessor\Tools\Lib\RangeStats;
use Bundl\CassandraProcessor\Tools\Lib\StatsCollector;
use Cubex\Cli\Shell;

/**
 * Show live stats for a CassandraProcessor task
 */
class LiveStats
{
  /**
   * @var RangeStats
   */
  private $_rangeStats;

  /**
   * @param RangeStats $rangeStats
   */
  public function __construct(RangeStats $rangeStats)
  {
    $this->_rangeStats = $rangeStats;
  }

  /**
   * @throws \Exception
   */
  public function execute()
  {
    Shell::clear();
    ob_start();
    Shell::clearToEol();
    $eolClear = ob_get_clean();
    $eol = $eolClear . "\n";
    $colSeparator = '  | ';

    $lastNumProcessed = 0;
    $firstNumProcessed = 0;
    $lastRangeRateTime = $startTime = time();
    $avgRangeRate = 0;
    $curRangeRate = 0;

    while(true)
    {
      $loopStartTime = microtime(true);

      $hostMaxLen = 0;
      $hosts = $this->_rangeStats->getProcessingHosts();
      foreach($hosts as $hostInfo)
      {
        $len = strlen(
          self::hostDisplayName($hostInfo['hostname'], $hostInfo['instance'])
        );
        if($len > $hostMaxLen)
        {
          $hostMaxLen = $len;
        }
      }

      $colWidth = $hostMaxLen + 14 + strlen($colSeparator);
      $winWidth = Shell::columns();

      $numCols = floor($winWidth / $colWidth);
      if($numCols < 1)
      {
        $numCols = 1;
      }

      $curCol = 1;

      $allReports = StatsCollector::getReports($hosts);

      Shell::home();
      echo $eol;

      echo " Stats for " . $_REQUEST['__path__'] . $eol . $eol;

      $totalItems = 0;
      $processed  = 0;
      $errors     = 0;
      $skipped    = 0;
      $totalRate       = 0;
      $numNodes   = 0;
      foreach($hosts as $hostInfo)
      {
        $hostname = $hostInfo['hostname'];
        $instance = $hostInfo['instance'];

        $reportId = $hostname . '|' . $instance;
        $report = isset($allReports[$reportId]) ? $allReports[$reportId] : false;

        echo " " . str_pad(
            self::hostDisplayName($hostname, $instance),
            $hostMaxLen,
            ' ',
            STR_PAD_RIGHT
          );

        if($report)
        {
          $totalItems += $report->totalItems;
          $processed += $report->totalProcessed;
          $errors += $report->totalErrors;
          $skipped += $report->totalSkipped;
          $totalRate += $report->averageRate;
          $numNodes++;
          $rateNum = $report->averageRate;

          printf(" %4s items/s", $rateNum);
        }
        else
        {
          echo "         \033[0;31mDOWN\033[0m";
        }

        if($curCol >= $numCols)
        {
          echo $eol;
          $curCol = 1;
        }
        else
        {
          echo $colSeparator;
          $curCol++;
        }
      }
      if($curCol != 1)
      {
        echo $eol;
      }

      echo $eol;
      $avgRate = $numNodes > 0 ? round($totalRate / $numNodes) : 0;

      echo " Active Nodes : " . $numNodes . $eol;
      /*echo " Total Items  : " . $totalItems . $eol;
      echo " Processed    : " . $processed . $eol;
      echo " Skipped      : " . $skipped . $eol;
      echo " Errors       : " . $errors . $eol;*/
      echo " Total Rate   : " . number_format($totalRate) . " items/second" . $eol;
      echo " Average rate : " . number_format($avgRate) . " items/second" . $eol;

      echo $eol . $eol . " Range Statistics:" . $eol;
      $stats     = $this->_rangeStats->getStats();
      $processedStats = isset($stats['processed']) ? $stats['processed'] : array();

      $numProcessed = empty($processedStats['Ranges']) ? 0 :
        $processedStats['Ranges'];
      $numUnprocessed = empty($stats['unprocessed']['Ranges']) ?
        0 : $stats['unprocessed']['Ranges'];
      $numProcessing  = empty($stats['processing']['Ranges']) ?
        0 : $stats['processing']['Ranges'];
      $numFailed      = empty($stats['failed']['Ranges']) ?
        0 : $stats['failed']['Ranges'];

      $totalRanges = $numProcessed + $numUnprocessed + $numProcessing + $numFailed;
      if($totalRanges > 0)
      {
        $processedPct = round($numProcessed * 100 / $totalRanges, 2);
        $unprocessedPct = round($numUnprocessed * 100 / $totalRanges, 2);
        $processingPct = round($numProcessing * 100 / $totalRanges, 2);
        $failedPct = round($numFailed * 100 / $totalRanges, 2);
      }
      else
      {
        $processedPct = 0;
        $unprocessedPct = 0;
        $processingPct = 0;
        $failedPct = 0;
      }

      $len = strlen(number_format($totalRanges));
      $fmt = "%-" . $len . "s (%0.2f%%)" . $eol;

      echo "  Processed   : " .
        sprintf($fmt, number_format($numProcessed), $processedPct);
      echo "  Unprocessed : " .
        sprintf($fmt, number_format($numUnprocessed), $unprocessedPct);
      echo "  Processing  : " .
        sprintf($fmt, number_format($numProcessing), $processingPct);
      $failedStr = "  Failed      : " .
        sprintf($fmt, number_format($numFailed), $failedPct);
      if($numFailed > 0)
      {
        $failedStr = "\033[0;31m" . $failedStr . "\033[0m";
      }
      echo $failedStr;
      echo "  Requeued    : " . $this->_rangeStats->getRequeuedRangesDelta() . $eol;
      echo $eol;

      $fields = array(
        'Total Items' => 'num',
        'Processed Items' => 'num',
        '' => 'blank',
        'Avg Range Time' => 'time',
        'Max Range Time' => 'time',
        'Avg Proc Items' => 'num',
        'Max Proc Items' => 'num',
        'Avg Total Items' => 'num',
        'Max Total Items' => 'num'
      );

      $labelWidth = 0;
      foreach($fields as $field => $type)
      {
        if($type != 'blank')
        {
          $len = strlen($field);
          if($len > $labelWidth)
          {
            $labelWidth = $len;
          }
        }
      }

      foreach($fields as $field => $type)
      {
        if($type == 'blank')
        {
          echo $eol;
        }
        else
        {
          $value = empty($processedStats[$field]) ? 0: $processedStats[$field];

          echo '  ' . str_pad($field, $labelWidth, ' ', STR_PAD_RIGHT) . ' : ';
          switch($type)
          {
            case 'num':
              echo number_format($value);
              break;
            case 'rate':
              echo number_format($value) . ' items/second';
              break;
            case 'time':
              echo number_format($value) . ' seconds';
              break;
            default:
              echo $processedStats[$field];
              break;
          }

          echo $eol;
        }
      }

      // Calculate time remaining
      $avgRangeTime = $this->_rangeStats->getAvgRangeTime(30);

      $remainingMachineSeconds = $avgRangeTime * ($numUnprocessed + $numProcessing);
      $remainingSeconds = $numNodes > 0 ?
        round($remainingMachineSeconds / $numNodes) : 0;

      echo $eol;
      echo " Remaining time: " . self::formatTime($remainingSeconds) . $eol;

      // Calculate time remaining (method 2)
      $statsTotalItems = empty($processedStats['Total Items']) ?
        0 : $processedStats['Total Items'];
      $estimatedTotalItems = (($numProcessed == 0) || ($totalRanges == 0)) ?
        0 : $statsTotalItems / ($numProcessed / $totalRanges);
      $estimatedRemainingItems = $estimatedTotalItems - $statsTotalItems;
      $remainingSeconds2 = $totalRate == 0 ?
        0 : round($estimatedRemainingItems / $totalRate);
      echo $eol;
      echo " Estimated total items: " . number_format($estimatedTotalItems) . $eol;
      echo " Estimated remaining items: " .
        number_format($estimatedRemainingItems) . $eol;
      echo " Remaining time: " . self::formatTime($remainingSeconds2) . $eol;

      // Estimate number of items still to process
      $processedRatio = $statsTotalItems == 0 ?
        0 : $processedStats['Processed Items'] / $statsTotalItems;
      $estimatedProcessedItems = $estimatedRemainingItems * $processedRatio;

      echo $eol;
      echo " Estimated remaining items to process: " .
        number_format($estimatedProcessedItems) . $eol;

      Shell::clearToEndOfScreen();

      // make sure it takes at least 1 second to go round the loop
      if(microtime(true) < $loopStartTime + 1)
      {
        time_sleep_until($loopStartTime + 1);
      }
    }
  }

  public static function formatTime($secs)
  {
    $days = floor($secs / 86400);
    $secs -= $days * 86400;
    $hours = floor($secs / 3600);
    $secs -= $hours * 3600;
    $mins = floor($secs / 60);
    $secs -= $mins * 60;

    $args = array();
    $fmt  = "";
    if($days == 1)
    {
      $fmt .= '1 day ';
    }
    else if($days > 1)
    {
      $fmt .= '%d days ';
      $args[] = $days;
    }
    $fmt .= '%d:%02d:%02d';
    $args[] = $hours;
    $args[] = $mins;
    $args[] = $secs;

    return vsprintf($fmt, $args);
  }

  public static function hostDisplayName($hostname, $instance)
  {
    return $instance == "" ? $hostname : $hostname . ' ' . $instance;
  }
}
