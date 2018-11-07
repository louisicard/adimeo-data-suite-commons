<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 28/10/2018
 * Time: 17:31
 */

namespace AdimeoDataSuite\Exception;


class DatasourceExecutionException extends \Exception
{

  protected $message;

  public function __construct($message)
  {
    $this->message = $message;
  }

}