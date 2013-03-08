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
use Cubex\Foundation\Config\ConfigTrait;

abstract class CassProcessorTask implements CliTask
{
  use ConfigTrait;

  protected $_instanceName = "";
  protected $_resetRanges = false;
  protected $_buildRanges = false;
  protected $_enableDebug = false;
  protected $_displayReport;

  public function __construct($loader = null, $args = null)
  {
    $this->_displayReport = true;
    foreach($args as $arg => $value)
    {
      switch($arg)
      {
        case 'instance':
          $this->_instanceName = $value;
          break;
        case 'resetRanges':
          $this->_resetRanges = true;
          break;
        case 'buildRanges':
          $this->_buildRanges = $value;
          break;
        case 'debugger':
          $this->_enableDebug = true;
          break;
        case 'displayReport':
          $this->_displayReport = $value != 0;
          break;
        default:
          die('Invalid argument: ' . $arg . "\n");
      }
    }
  }

  public function init()
  {
    TokenRange::setTableName($this->_getTokenRangesTableName());

    if($this->_enableDebug)
    {
      $debugger = new DebuggerBundle();
      $debugger->init();
    }

    $logger  = new CliLogger(
      $this->_getEchoLevel(), $this->_getLogLevel(), "", $this->_instanceName
    );
    $pidFile = new PidFile("", $this->_instanceName);

    $rangeManager = new RangeManager(
      $this->_getCassServiceName(), $this->_getColumnFamilyName(),
      $this->_getProcessor(), $this->_instanceName, $this->_displayReport
    );

    if($this->_buildRanges)
    {
      $rangeManager->buildRanges($this->_buildRanges);
    }
    else if($this->_resetRanges)
    {
      $rangeManager->resetRanges();
    }
    else
    {
      $rangeManager->processAll();
    }
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
