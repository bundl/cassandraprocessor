<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

use Cubex\Chronos\ProfilingStats;
use Cubex\Chronos\StopwatchCollection;
use Cubex\Events\EventManager;
use Cubex\Helpers\Strings;
use Cubex\Text\TextTable;

class Profiler
{
  protected $_stopwatchCollection;
  protected $_profilingStats;

  public function __construct()
  {
    $this->_stopwatchCollection = new StopwatchCollection();
    $this->_profilingStats = new ProfilingStats();

    $this->_stopwatchCollection->setDisplayAllInReport(false);

    $this->_addDefaultTimers();

    EventManager::listen(
      Events::DISPLAY_REPORT_END,
      [$this, 'displayReport']
    );
  }

  public function addAllEvents($className)
  {
    $class = new \ReflectionClass($className);
    $consts = $class->getConstants();

    // Find matching pairs of "start" and "stop" constants
    foreach($consts as $constName => $value)
    {
      if(ends_with($constName, '_START') && ends_with($value, '.start'))
      {
        $stopName = substr($constName, 0, strlen($constName) - 6) . '_END';
        $stopValue = substr($value, 0, strlen($value) - 6) . '.end';
        if(isset($consts[$stopName]) && ($consts[$stopName] == $stopValue))
        {
          $this->addStartStopEvent($value, $stopValue);
        }
      }
    }
  }

  public function addStartStopEvent($startEvent, $stopEvent)
  {
    $timerName = rtrim(Strings::commonPrefix($startEvent, $stopEvent), '.');

    $this->_stopwatchCollection->newEventStopwatch(
      $timerName,
      $startEvent,
      $stopEvent
    );
  }

  public function addMultipleEvents(array $events)
  {
    foreach($events as $eventPair)
    {
      $this->addStartStopEvent($eventPair[0], $eventPair[1]);
    }
  }

  public function displayReport()
  {
    echo "\n";
    $this->_stopwatchCollection->displayReport();

    $t = new TextTable();
    $t->appendRows($this->_profilingStats->getReportData());
    echo $t;
  }

  protected function _addDefaultTimers()
  {
    $p = $this->_stopwatchCollection;

    // ------------------------------------------------
    //  CassandraProcessor events
    // ------------------------------------------------

    $p->newEventStopwatch(
      'cassandraprocessor.processBatch',
      Events::PROCESS_BATCH_START,
      Events::PROCESS_BATCH_END
    );
    $p->newEventStopwatch(
      'cassandraprocessor.refreshKeys',
      Events::REFRESH_KEYS_START,
      Events::REFRESH_KEYS_END
    );
    $p->newEventStopwatch(
      'cassandraprocessor.cassConnect',
      Events::CASS_CONNECT_START,
      Events::CASS_CONNECT_END
    );
    $p->newEventStopwatch(
      'cassandraprocessor.getKeys',
      Events::GET_KEYS_START,
      Events::GET_KEYS_END
    );
    $p->newEventStopwatch(
      'cassandraprocessor.claimRange',
      Events::CLAIM_RANGE_START,
      Events::CLAIM_RANGE_END
    );
    $p->newEventStopwatch(
      'cassandraprocessor.requeueRange',
      Events::REQUEUE_RANGE_START,
      Events::REQUEUE_RANGE_END
    );
    $p->newEventStopwatch(
      'cassandraprocessor.rangeSaveChanges',
      Events::RANGE_SAVE_CHANGES_START,
      Events::RANGE_SAVE_CHANGES_END
    );
  }
}
