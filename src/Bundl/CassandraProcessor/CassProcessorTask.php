<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
use Bundl\Debugger\DebuggerBundle;
use Cubex\Cli\CliArgument;
use Cubex\Cli\CliCommand;
use Cubex\Cli\CliLogger;
use Cubex\Cli\PidFile;
use Cubex\Cli\Shell;
use Cubex\Data\Validator\Validator;
use Cubex\Facade\Cassandra;
use cassandra\ConsistencyLevel;

abstract class CassProcessorTask extends CliCommand
{
  protected $_instanceName = "";
  protected $_enableDebug = false;
  protected $_displayReport = true;
  private $_rangeManager = null;
  private $_logger = null;
  private $_pidFile = null;

  protected function _argumentsList()
  {
    return [
      new CliArgument(
        'instance',
        'A name to give this instance of the script. Required when running ' .
        'multiple instances of the same script on one machine.',
        "i", CliArgument::VALUE_REQUIRED, 'name'
      ),
      new CliArgument('reset-ranges', 'Reset the status of all ranges'),
      new CliArgument(
        'reset-range',
        'Reset a range to be reprocessed',
        "", CliArgument::VALUE_REQUIRED, 'rangeId', false, null,
        Validator::VALIDATE_INT
      ),
      new CliArgument(
        'build-ranges',
        'Delete all existing ranges and rebuild with the specified number of ranges',
        "", CliArgument::VALUE_REQUIRED, 'count', false, null,
        Validator::VALIDATE_INT
      ),
      new CliArgument('debug', 'If set then include the DebuggerBundle'),
      new CliArgument('no-report', 'Don\'t show the processing report'),
      new CliArgument('refresh-keys', 'For testing only: Refresh the keys in all ranges'),
      new CliArgument(
        'count-range',
        'Count the number of keys in a range',
        '',
        CliArgument::VALUE_REQUIRED,
        'startkey,endkey'
      ),
      new CliArgument(
        'get-keys',
        'List the keys at the start and end of a token range',
        '',
        CliArgument::VALUE_REQUIRED,
        'startToken,endToken,count'
      )
    ];
  }

  public function init()
  {
  }

  public function execute()
  {
    TokenRange::setTableName($this->_getTokenRangesTableName());

    // Process options
    if($this->argumentValue('debug'))
    {
      $debugger = new DebuggerBundle();
      $debugger->init();
    }

    $this->_instanceName = $this->argumentValue('instance', '');
    if($this->argumentValue('no-report'))
    {
      $this->_displayReport = false;
    }

    // Run in the appropriate mode

    $resetRangeId = $this->argumentValue('reset-range');
    $buildRanges  = $this->argumentValue('build-ranges');
    $countRange   = $this->argumentValue('count-range');
    $getKeys      = $this->argumentValue('get-keys');
    if($resetRangeId)
    {
      echo "Resetting range " . $resetRangeId . "...\n";
      $this->_getRangeManager()->resetRange($resetRangeId);
      echo "Finished.\n";
    }
    else if($buildRanges)
    {
      $this->_getRangeManager()->buildRanges($buildRanges);
    }
    else if($this->argumentValue('reset-ranges'))
    {
      echo "Resetting all ranges...\n";
      $this->_getRangeManager()->resetRanges();
      echo "Finished.\n";
    }
    else if($this->argumentValue('refresh-keys'))
    {
      // For testing only: Refresh the keys in all ranges
      $this->_getRangeManager()->refreshKeysForAllRanges();
    }
    else if($countRange)
    {
      list($startKey, $endKey) = explode(",", $countRange, 2);
      $this->_countRange($startKey, $endKey);
    }
    else if($getKeys)
    {
      list($startToken, $endToken, $count) = explode(",", $getKeys);
      $this->_getKeys(
        $startToken,
        $endToken,
        $count
      );
    }
    else
    {
      // Default run mode - process the ranges...
      $this->_logger  = new CliLogger(
        $this->_getEchoLevel(), $this->_getLogLevel(),
        "", $this->_instanceName
      );
      $this->_pidFile = new PidFile("", $this->_instanceName);
      $this->_getRangeManager()->processAll();
    }
  }

  protected function _getRangeManager()
  {
    if($this->_rangeManager == null)
    {
      $this->_rangeManager = new RangeManager(
        $this->_getCassServiceName(), $this->_getColumnFamilyName(),
        $this->_getReadConsistencyLevel(), $this->_getProcessor(),
        $this->_instanceName, $this->_displayReport
      );
    }
    return $this->_rangeManager;
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
    $finished  = false;
    $lastKey   = $startKey;
    while(!$finished)
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
   * Return the consistency level to use when reading from the CF
   */
  protected function _getReadConsistencyLevel()
  {
    return ConsistencyLevel::QUORUM;
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
