<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
use Bundl\Debugger\DebuggerBundle;
use Cubex\Cli\CliLogger;
use Cubex\Cli\CliTask;
use Cubex\Cli\PidFile;
use Cubex\Cli\Shell;
use Cubex\Facade\Cassandra;
use Cubex\Foundation\Config\ConfigTrait;

abstract class CassProcessorTask implements CliTask
{
  use ConfigTrait;

  const RUNMODE_NORMAL       = 1;
  const RUNMODE_RESET_RANGES = 2;
  const RUNMODE_BUILD_RANGES = 3;
  const RUNMODE_REFRESH_KEYS = 4;
  const RUNMODE_COUNT_RANGE  = 5;
  const RUNMODE_GET_KEYS     = 6;

  protected $_instanceName = "";
  protected $_enableDebug = false;
  protected $_displayReport;
  protected $_runMode;

  private $_rangeCount;
  private $_countStartKey;
  private $_countEndKey;
  private $_getKeysStartToken;
  private $_getKeysEndToken;
  private $_getKeysCount;

  private $_rangeManager = null;

  public function __construct($loader = null, $args = null)
  {
    $this->_displayReport = true;
    $this->_runMode = self::RUNMODE_NORMAL;

    foreach($args as $arg => $value)
    {
      switch($arg)
      {
        case 'instance':
          $this->_instanceName = $value;
          break;
        case 'resetRanges':
          $this->_runMode = self::RUNMODE_RESET_RANGES;
          break;
        case 'buildRanges':
          if((! $value) || (! is_numeric($value)))
          {
            echo "You must specify a number of ranges to build (e.g. buildRanges=10000)\n";
            die();
          }
          $this->_runMode = self::RUNMODE_BUILD_RANGES;
          $this->_rangeCount = $value;
          break;
        case 'debugger':
          $this->_enableDebug = true;
          break;
        case 'displayReport':
          $this->_displayReport = $value != 0;
          break;
        case 'refreshKeys':
          $this->_runMode = self::RUNMODE_REFRESH_KEYS;
          break;
        case 'countRange':
          $this->_runMode = self::RUNMODE_COUNT_RANGE;
          if(isset($args['start']) && isset($args['end']))
          {
            $this->_countStartKey = $args['start'];
            $this->_countEndKey = $args['end'];
          }
          else
          {
            echo "Usage: countRange start=startKey end=endKey\n";
            die();
          }
          break;
        case 'getKeys':
          $this->_runMode = self::RUNMODE_GET_KEYS;
          if(isset($args['start']) && isset($args['end']))
          {
            $this->_getKeysStartToken = $args['start'];
            $this->_getKeysEndToken = $args['end'];
            $this->_getKeysCount = isset($args['count']) ? $args['count'] : 5;
          }
          else
          {
            echo "Usage: getKeys start=startToken end=endToken [count=5]\n";
            die();
          }
          break;
        case 'start':
        case 'end':
        case 'count':
          break;
        default:
          die('Invalid argument: ' . $arg . "\n");
      }
    }
  }

  protected function _getRangeManager()
  {
    if($this->_rangeManager == null)
    {
      $this->_rangeManager = new RangeManager(
        $this->_getCassServiceName(), $this->_getColumnFamilyName(),
        $this->_getProcessor(), $this->_instanceName, $this->_displayReport
      );
    }
    return $this->_rangeManager;
  }

  public function init()
  {
    TokenRange::setTableName($this->_getTokenRangesTableName());

    if($this->_enableDebug)
    {
      $debugger = new DebuggerBundle();
      $debugger->init();
    }

    switch($this->_runMode)
    {
      case self::RUNMODE_BUILD_RANGES:
        $this->_getRangeManager()->buildRanges($this->_rangeCount);
        break;
      case self::RUNMODE_RESET_RANGES:
        $this->_getRangeManager()->resetRanges();
        break;
      case self::RUNMODE_REFRESH_KEYS:
        // For testing only: Refresh the keys in all ranges
        $this->_getRangeManager()->refreshKeysForAllRanges();
        break;
      case self::RUNMODE_COUNT_RANGE:
        $this->_countRange($this->_countStartKey, $this->_countEndKey);
        break;
      case self::RUNMODE_GET_KEYS:
        $this->_getKeys(
          $this->_getKeysStartToken,
          $this->_getKeysEndToken,
          $this->_getKeysCount
        );
        break;
      default:
        $logger  = new CliLogger(
          $this->_getEchoLevel(), $this->_getLogLevel(), "", $this->_instanceName
        );
        $pidFile = new PidFile("", $this->_instanceName);
        $this->_getRangeManager()->processAll();
        break;
    }
  }

  private function _countRange($startKey, $endKey)
  {
    if(($startKey == "") && ($endKey == ""))
    {
      echo "Start key and end key are both blank\n";
      die;
    }

    $cass = Cassandra::getAccessor($this->_getCassServiceName());
    $cf   = $cass->cf($this->_getColumnFamilyName(), false);

    echo "Counting range from '" . $startKey . "' to '" . $endKey . "'\n";

    $batchSize = 1000;
    $totalKeys = 1;
    $finished = false;
    $lastKey = $startKey;
    while(! $finished)
    {
      // ignore the duplicate key from each time around
      $totalKeys--;

      $items = $cf->getKeys($lastKey, $endKey, $batchSize, array());

      $cnt = count($items);

      $totalKeys += $cnt;
      $lastKey = last_key($items);

      if(($cnt < $batchSize) || ($lastKey == $endKey))
      {
        $finished = true;
      }

      Shell::clearLine();
      echo "Found " . number_format($totalKeys) . " keys";
    }

    echo "\n";
  }

  private function _getKeys($startToken, $endToken, $count)
  {
    $cass = Cassandra::getAccessor($this->_getCassServiceName());
    $cf   = $cass->cf($this->_getColumnFamilyName(), false);

    echo "Start token: " . $startToken . "\n";
    $tokens = $cf->getTokens($startToken, $startToken, $count);
    foreach(array_keys($tokens) as $key)
    {
      echo md5($key) . " - " . $key . "\n";
    }
    echo "\n";

    echo "End token: " . $endToken . "\n";
    $tokens = $cf->getTokens($endToken, $endToken, $count);
    foreach(array_keys($tokens) as $key)
    {
      echo md5($key) . " - " . $key . "\n";
    }
    echo "\n";
  }

  /**
   * Return the name of the MySQL table used to store token ranges
   *
   * @return string
   */
  protected function _getTokenRangesTableName()
  {
    return 'token_ranges';
  }

  /**
   * Create the ItemProcessor for this task
   *
   * @return ItemProcessor
   */
  protected abstract function _getProcessor();

  /**
   * Get the name of the service to use when connecting to Cassandra
   *
   * @return string
   */
  protected abstract function _getCassServiceName();

  /**
   * Get the name of the column family to process
   *
   * @return string
   */
  protected abstract function _getColumnFamilyName();

  /**
   * Get the logging level (should be one of the LogLevel constants)
   *
   * @return string
   */
  protected abstract function _getLogLevel();

  /**
   * Get the console output log level (should be one of the LogLevel constants)
   *
   * @return string
   */
  protected abstract function _getEchoLevel();
}
