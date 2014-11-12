<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor\Tools\Lib;

use Cubex\Facade\DB;

class RangeStats
{
  const SQL_INTERVAL = 15;
  const REQUEUE_COUNT_INTERVAL = 300;

  private $_dbServiceName;
  private $_dbconn;
  private $_tableName;
  private $_tableList;

  private $_statsCache;
  private $_lastStatsTime;
  private $_rateCache;
  private $_lastRateTime;

  private $_cruncherToFlowMap;
  private $_hostsList;
  private $_lastHostsQuery;

  private $_lastRequeueCount;
  private $_lastRequeueCountTime;
  private $_requeueCountDelta;

  public function __construct($dbServiceName, $tableName)
  {
    $this->_dbServiceName = $dbServiceName;
    $this->_tableName = $tableName;
    $this->_dbconn = null;

    $this->_statsCache = null;
    $this->_rateCache = null;
    $this->_cruncherToFlowMap = array();
    $this->_hostsList = array();
    $this->_lastHostsQuery = 0;
    $this->_tableList = array();
    $this->_lastRequeueCount = -1;
    $this->_lastRequeueCountTime = 0;
    $this->_requeueCountDelta = 0;
  }

  private function _db()
  {
    return DB::getAccessor($this->_dbServiceName);
  }

  private function _getTableList()
  {
    if(!$this->_tableList)
    {
      if(substr($this->_tableName, -1) == '*')
      {
        $tables = array();
        $tableBase = substr($this->_tableName, 0, -1);
        $i = 1;
        while(true)
        {
          $table = $tableBase . $i;
          if($this->_tableExists($table))
          {
            $tables[] = $table;
          }
          else
          {
            break;
          }
          $i++;
        }
        $this->_tableList = $tables;
      }
      else
      {
        $this->_tableList = array($this->_tableName);
      }
    }
    return $this->_tableList;
  }

  private function _tableExists($tableName)
  {
    $res = $this->_db()->query("SHOW TABLES LIKE '" . $tableName . "'");
    return $res->num_rows > 0;
  }

  public function getStats()
  {
    if((! $this->_statsCache) ||
      ((time() - $this->_lastStatsTime) > self::SQL_INTERVAL))
    {
      $query = "
        SELECT
        CASE CONCAT(processed,failed,processing)
        WHEN '000' THEN 'unprocessed'
        WHEN '001' THEN 'processing'
        WHEN '110' THEN 'failed'
        WHEN '010' THEN 'failed'
        WHEN '100' THEN 'processed'
        ELSE 'unknown'
        END AS `Range Status`,
        COUNT(*) AS `Ranges`,
        SUM(totalItems) AS `Total Items`,
        SUM(processedItems) AS `Processed Items`,
        (SUM(totalItems)/SUM(processingTime))*COUNT(DISTINCT hostname) AS `Total Rate Overall`,
        MAX(processingTime) AS `Max Range Time`,
        AVG(processingTime) AS `Avg Range Time`,
        MAX(processedItems) AS `Max Proc Items`,
        AVG(processedItems) AS `Avg Proc Items`,
        MAX(totalItems) AS `Max Total Items`,
        AVG(totalItems) AS `Avg Total Items`
        FROM %T
        GROUP BY processed, failed, processing";

      $res = $this->_multiQuery($query);

      $sumFields = array(
        'Ranges',
        'Total Items',
        'Processed Items',
        'Total Rate Overall'
      );
      $maxFields = array(
        'Max Range Time',
        'Max Proc Items',
        'Max Total Items'
      );
      $avgFields = array(
        'Avg Range Time',
        'Avg Proc Items',
        'Avg Total Items'
      );

      $fullStats = array();
      foreach($res as $row)
      {
        $status = $row['Range Status'];
        if(! isset($fullStats[$status]))
        {
          $fullStats[$status] = array();
          foreach(array_merge($sumFields, $maxFields) as $field)
          {
            $fullStats[$status][$field] = 0;
          }
          foreach($avgFields as $field)
          {
            $fullStats[$status][$field] = array('total' => 0, 'count' => 0);
          }
        }

        foreach($sumFields as $field)
        {
          $fullStats[$status][$field] += $row[$field];
        }
        foreach($maxFields as $field)
        {
          $fullStats[$status][$field] = max(
            $fullStats[$status][$field],
            $row[$field]
          );
        }
        foreach($avgFields as $field)
        {
          $fullStats[$status][$field]['total'] += $row[$field];
          $fullStats[$status][$field]['count']++;
        }
      }

      foreach($avgFields as $field)
      {
        foreach($fullStats as $status => $data)
        {
          $total = $fullStats[$status][$field]['total'];
          $count = $fullStats[$status][$field]['count'];
          $fullStats[$status][$field] = $count > 0 ? ($total / $count) : 0;
        }
      }

      $this->_statsCache = $fullStats;
      $this->_lastStatsTime = time();
    }

    return $this->_statsCache;
  }

  /**
   * @param int $maxAge How old the oldest range to read should be in minutes
   * @return int
   */
  public function getAvgRangeTime($maxAge)
  {
    if((! $this->_rateCache) ||
      ((time() - $this->_lastRateTime) > self::SQL_INTERVAL))
    {
      $query = sprintf(
        "SELECT AVG(processingTime) AS procTime FROM %%T" .
        " WHERE processed=1 AND failed=0 AND updatedAt>'%s'",
        date('Y-m-d H:i:s', time() - ($maxAge * 60))
      );

      $res = $this->_multiQuery($query);
      $total = 0;
      $count = 0;
      foreach($res as $row)
      {
        if(isset($row['procTime']))
        {
          $total += $row['procTime'];
          $count++;
        }
      }

      if($count > 0)
      {
        $this->_rateCache = $total / $count;
      }
      $this->_lastRateTime = time();
    }
    return $this->_rateCache;
  }

  public function getProcessingHosts()
  {
    if((time() - $this->_lastHostsQuery) > self::SQL_INTERVAL)
    {
      $changed = false;

      $res = $this->_multiQuery(
        'SELECT DISTINCT hostname FROM %T WHERE processing=1'
      );

      $hosts = array();
      foreach($res as $row)
      {
        $hosts[$row['hostname']] = 1;
      }
      $hostnames = array_keys($hosts);

      foreach($hostnames as $hostname)
      {
        if(strpos($hostname, '|'))
        {
          list($hostname, $instance) = explode('|', $hostname, 2);
        }
        else
        {
          $instance = "";
        }

        $hostname = $this->_cruncherToFlow($hostname);

        // Add any new hosts
        $found = false;
        foreach($this->_hostsList as $hostInfo)
        {
          if(($hostInfo['hostname'] == $hostname) &&
            ($hostInfo['instance'] == $instance))
          {
            $found = true;
            break;
          }
        }

        if(!$found)
        {
          $changed = true;
          $this->_hostsList[] = array(
            'hostname' => $hostname,
            'instance' => $instance
          );
        }
      }

      if($changed)
      {
        usort(
          $this->_hostsList,
          function ($a, $b)
          {
            if($a['instance'] != $b['instance'])
            {
              return strnatcmp($a['instance'], $b['instance']);
            }
            else
            {
              return strnatcmp($a['hostname'], $b['hostname']);
            }
          }
        );
      }
      $this->_lastHostsQuery = time();
    }

    return $this->_hostsList;
  }

  /**
   * Count how many ranges have been requeued since the last check
   */
  public function getRequeuedRangesDelta()
  {
    if((time() - $this->_lastRequeueCountTime) > self::REQUEUE_COUNT_INTERVAL)
    {
      $res = $this->_multiQuery(
        'SELECT SUM(requeueCount) AS requeueCount FROM %T'
      );

      $count = 0;
      foreach($res as $row)
      {
        $count += intval($row['requeueCount']);
      }

      if($this->_lastRequeueCount == -1)
      {
        $this->_lastRequeueCount = $count;
        $this->_requeueCountDelta = 0;
      }
      else
      {
        $this->_requeueCountDelta = $count - $this->_lastRequeueCount;
        $this->_lastRequeueCount = $count;
      }
      $this->_lastRequeueCountTime = time();
    }

    return $this->_requeueCountDelta .
    " (" . date('H:i:s', $this->_lastRequeueCountTime) . ")";
  }

  private function _cruncherToFlow($cruncherName)
  {
    if(empty($this->_cruncherToFlowMap))
    {
      $this->_loadCruncherFlowMap();
    }

    if(isset($this->_cruncherToFlowMap[$cruncherName]) &&
      $this->_cruncherToFlowMap[$cruncherName])
    {
      return $this->_cruncherToFlowMap[$cruncherName];
    }
    else
    {
      list($hostname) = explode(".", $cruncherName);
      return $hostname;
    }
  }

  private function _loadCruncherFlowMap()
  {
    $lines = file("/etc/hosts");

    foreach($lines as $line)
    {
      $parts = preg_split("/\\s+/", $line);
      if(count($parts) >= 4)
      {
        $this->_cruncherToFlowMap[$parts[2]] = $parts[3];
      }
    }
  }

  private function _multiQuery($query, $indexByTable = false)
  {
    $tables = $this->_getTableList();

    $fullResult = array();

    foreach($tables as $table)
    {
      $thisQuery = str_replace('%T', '`' . $table . '`', $query);
      $res = $this->_db()->query($thisQuery);

      if($res && ($res->num_rows > 0))
      {
        while($row = $res->fetch_assoc())
        {
          if($indexByTable)
          {
            if(! isset($fullResult[$table]))
            {
              $fullResult[$table] = array();
            }
            $fullResult[$table][] = $row;
          }
          else
          {
            $fullResult[] = $row;
          }
        }
      }
    }
    return $fullResult;
  }
}
