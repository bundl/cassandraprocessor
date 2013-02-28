<?php
/**
 * @author  Richard.Gooding
 */

namespace Bundl\CassandraProcessor;

class ScriptProgress
{
  private function _progressFileName()
  {
    return realpath(dirname(WEB_ROOT)) . DS . 'progress-' . $_REQUEST['__path__'];
  }

  public function load()
  {
    $progress = new \stdClass();
    $progress->rangeStartKey = "";
    $progress->lastKey = "";

    $progressFile = $this->_progressFileName();
    if(file_exists($progressFile))
    {
      $parts = explode(chr(3), file_get_contents($progressFile));
      if(count($parts) == 2)
      {
        $progress->rangeStartKey = $parts[0];
        $progress->lastKey = $parts[1];
      }
    }

    return $progress;
  }

  public function save($rangeStartKey, $key)
  {
    file_put_contents($this->_progressFileName(), $rangeStartKey . chr(3) . $key);
  }
}
