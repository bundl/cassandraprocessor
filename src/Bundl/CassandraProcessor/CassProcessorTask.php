<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Bundl\CassandraProcessor\Mappers\TokenRange;
use Bundl\CassandraProcessor\Tools\Lib\RangeStats;
use Bundl\CassandraProcessor\Tools\LiveStats;
use Bundl\Debugger\DebuggerBundle;
use Cubex\Cli\CliArgument;
use Cubex\Cli\CliCommand;
use Cubex\Cli\CliLogger;
use Cubex\Cli\PidFile;
use Cubex\Cli\Shell;
use Cubex\Data\Validator\Validator;
use Cubex\Facade\Cassandra;
use cassandra\ConsistencyLevel;
use Cubex\Foundation\Container;
use Cubex\Loader;
use Cubex\Mapper\Database\RecordCollection;
use Psr\Log\LogLevel;

abstract class CassProcessorTask extends CliCommand
{
  protected $_instanceName = "";
  protected $_enableDebug = false;
  protected $_displayReport = true;
  private $_rangeManager = null;
  /**
   * @var PidFile
   */
  private $_pidFile = null;
  protected $_defaultLogLevel = LogLevel::INFO;

  protected function _argumentsList()
  {
    return [
      new CliArgument(
        'instance',
        'A name to give this instance of the script. Required when running ' .
        'multiple instances of the same script on one machine.',
        "i", CliArgument::VALUE_REQUIRED, 'name'
      ),
      new CliArgument(
        'instances',
        'Launch this many instances of the script. Each instance will be ' .
        'given the name "instX" where X is the number of the instance.',
        "", CliArgument::VALUE_REQUIRED, 'count', false, null,
        Validator::VALIDATE_INT
      ),
      new CliArgument(
        'dry-run',
        'Run in dry run mode, no writing or deleting will be performed'
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
      new CliArgument(
        'refresh-keys', 'For testing only: Refresh the keys in all ranges'
      ),
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
      ),
      new CliArgument(
        'list-failed',
        'List failed ranges, defaults to 100',
        '',
        CliArgument::VALUE_OPTIONAL,
        'limit',
        false,
        100,
        Validator::VALIDATE_INT
      ),
      new CliArgument(
        'list-requeued',
        'List ranges that were requeued within the last "mins" minutes',
        '',
        CliArgument::VALUE_OPTIONAL,
        'mins',
        false,
        30,
        Validator::VALIDATE_INT
      ),
      new CliArgument(
        'reset-failed',
        'Reset all failed ranges'
      ),
      new CliArgument(
        'reset-processing',
        'Reset all ranges that are flagged as processing. USE WITH CARE.'
      ),
      new CliArgument(
        'live-stats',
        'Show the stats for this script on all nodes'
      ),
    ];
  }

  public function init()
  {
    $this->_instanceName = $this->argumentValue('instance', '');
    $this->_logger->setInstanceName($this->_instanceName);

    if($this->argumentIsSet('dry-run'))
    {
      ProcessorOptions::setDryRun(true);
    }
    TokenRange::setOverrideTableName($this->_getTokenRangesTableName());
    TokenRange::setOverrideServiceName($this->_getTokenRangesDBServiceName());
  }

  public function execute()
  {
    // Process options
    if($this->argumentValue('debug'))
    {
      $debugger = new DebuggerBundle();
      $debugger->init();
    }

    if($this->argumentValue('no-report'))
    {
      $this->_displayReport = false;
    }

    // Run in the appropriate mode

    $resetProcessing = $this->argumentValue('reset-processing');
    $resetRangeId    = $this->argumentValue('reset-range');
    $buildRanges     = $this->argumentValue('build-ranges');
    $countRange      = $this->argumentValue('count-range');
    $getKeys         = $this->argumentValue('get-keys');
    $resetFailed     = $this->argumentIsSet('reset-failed');
    $listFailed      = $this->argumentIsSet('list-failed') ?
      $this->argumentValue('list-failed') : false;
    $listRequeued    = $this->argumentIsSet('list-requeued') ?
      $this->argumentValue('list-requeued') : false;
    $instanceCount   = $this->argumentValue('instances', 1);
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
    else if($listFailed)
    {
      $this->_getRangeManager()->listFailedRanges($listFailed);
    }
    else if($resetFailed)
    {
      $this->_getRangeManager()->resetFailedRanges();
    }
    else if($listRequeued)
    {
      $this->_getRangeManager()->listRequeuedRanges($listRequeued);
    }
    else if($resetProcessing)
    {
      $this->_getRangeManager()->resetProcessingRanges();
    }
    else if($this->argumentValue('live-stats'))
    {
      (
      new LiveStats(
        new RangeStats(
          $this->_getTokenRangesDBServiceName(),
          $this->_getTokenRangesTableName()
        )
      )
      )->execute();
    }
    else
    {
      // Default run mode - process the ranges...
      if($instanceCount > 1)
      {
        $this->_forkInstances($instanceCount);
      }

      $this->_pidFile = new PidFile("", $this->_instanceName);
      $this->_initProcessingRun();
      $this->_getRangeManager()->processAll();
    }
  }

  public function showRangeData()
  {
    $col = new RecordCollection(new TokenRange());
    $col->whereNeq('rangeData', '');
    foreach($col as $range)
    {
      /** @var TokenRange $range */
      echo $range->id() . ' : ' . $range->rangeData . "\n";
    }
  }

  protected function _forkInstances($count)
  {
    $firstInstanceName = $this->_instanceName;
    for($i = 2; $i <= $count; $i++)
    {
      $childPid = pcntl_fork();
      if($childPid == -1)
      {
        throw new \Exception('Unable to fork instance ' . $i);
      }
      if($childPid == 0)
      {
        // Child process
        $this->_instanceName = 'inst' . $i;
        $this->_logger->setInstanceName($this->_instanceName);
        return;
      }
    }
    $this->_instanceName = $firstInstanceName;
  }

  /**
   * Override this to perform any operations that need to be done before
   * starting the processing run
   */
  protected function _initProcessingRun()
  {
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
    $count = (int)$count;
    if($count < 1)
    {
      $count = 1;
    }
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
   * Return the name of the DB service that contains the token ranges table
   *
   * @return string
   */
  protected function _getTokenRangesDBServiceName()
  {
    return 'db';
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
}
