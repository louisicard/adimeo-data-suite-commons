<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 28/10/2018
 * Time: 17:31
 */

namespace AdimeoDataSuite\Bundle\CommonsBundle\Exception;


class DatasourceExecutionException extends \Exception
{

  private $message;

  public function __construct($message)
  {
    $this->message = $message;
  }

}