<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor\Tools\Lib;

use Cubex\Foundation\Container;
use Cubex\Process\MultiExec;

class StatsCollector
{
  protected static function _buildReportSSHCommand(
    $hostname, $scriptDir, $className, $instance = ""
  )
  {
    $reportFile = $scriptDir . '/logs/' . $className;
    if($instance != "")
    {
      $reportFile .= '-' . $instance;
    }
    $reportFile .= '/stats.txt';

    $pidFile = '/var/run/cubex/' . $className;
    if($instance != "")
    {
      $pidFile .= '.' . $instance;
    }
    $pidFile .= '.pid';

    $command = "ps ax | grep -v grep | grep 'cubex " . $className . "' | " .
      'grep \`cat "' . $pidFile . '"\` >/dev/null && ' .
      "cat '" . $reportFile . "'";
    $command = 'ssh -oConnectTimeout=1 ' . $hostname .
      ' "' . $command . '" 2>/dev/null';

    return $command;
  }

  public static function getReportIfRunning(
    $hostname, $scriptDir, $className, $instance = ""
  )
  {
    $command = self::_buildReportSSHCommand(
      $hostname,
      $scriptDir,
      $className,
      $instance
    );

    $result = exec($command, $output, $retval);
    if($retval == 0)
    {
      return json_decode($result);
    }
    else
    {
      return false;
    }
  }

  protected static function _buildMultiInstanceSSHCommand($hostname, $instances)
  {
    $className = $_REQUEST['__path__'];
    $prjConf = Container::config()->get('project')->getData();
    $namespace = $prjConf['namespace'];
    $scriptDir = CUBEX_PROJECT_ROOT;

    $commands = array();
    foreach($instances as $instance)
    {
      $reportFile = $scriptDir . '/logs/' . $className;
      if($instance != "")
      {
        $reportFile .= '-' . $instance;
      }
      $reportFile .= '/stats.txt';

      $pidFile = '/var/run/cubex/' . $namespace . '/' . $className;
      if($instance != "")
      {
        $pidFile .= '.' . $instance;
      }
      $pidFile .= '.pid';

      $commands[] = "echo; echo -n '" . $instance . "=|=' ;" .
        "ps ax | grep -v grep | " .
        "grep -e 'cubex \\(--cubex-env=production \\)\\?" . $className . "' | ".
        'grep \`cat "' . $pidFile . '"\` >/dev/null && ' .
        "cat '" . $reportFile . "'";
    }

    $command = 'ssh -oConnectTimeout=1 ' . $hostname .
      ' "' . implode(";", $commands) . '" 2>/dev/null';
    return $command;
  }

  /**
   * @param $hosts
   *
   * @return stdClass[]
   */
  public static function getReports($hosts)
  {
    $mx     = new MultiExec();

    $hostInstances = array();
    foreach($hosts as $hostInfo)
    {
      $hostname = $hostInfo['hostname'];
      $instance = $hostInfo['instance'];

      if(! isset($hostInstances[$hostname]))
      {
        $hostInstances[$hostname] = array();
      }
      $hostInstances[$hostname][] = $instance;
    }

    foreach($hostInstances as $hostname => $instances)
    {
      $command = self::_buildMultiInstanceSSHCommand($hostname, $instances);
      $mx->addCommand($hostname, $command);
    }

    $mx->execute();

    $reports = array();
    foreach($hostInstances as $hostname => $instances)
    {
      $output = $mx->getOutput($hostname);
      $lines = explode("\n", $output);

      foreach($lines as $line)
      {
        if(strpos($line, '=|=') !== false)
        {
          list($instance, $stats) = explode('=|=', $line, 2);
          $reports[$hostname . '|' . $instance] = json_decode($stats);
        }
      }
    }
    return $reports;
  }
}
