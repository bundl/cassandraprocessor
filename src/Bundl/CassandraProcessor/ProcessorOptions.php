<?php
/**
 * @author  richard.gooding
 */

namespace Bundl\CassandraProcessor;

class ProcessorOptions
{
  // Set defaults here
  private static $_dryRun = false;

  public static function setDryRun($dryRun)
  {
    self::$_dryRun = $dryRun;
  }

  public static function dryRun()
  {
    return self::$_dryRun;
  }
}
