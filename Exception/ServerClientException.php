<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 28/10/2018
 * Time: 17:31
 */

namespace AdimeoDataSuite\Exception;


class ServerClientException extends \Exception
{

  protected $message;

  private $statusCode;

  public function __construct($message, $code = null)
  {
    $this->message = $message;
    $this->statusCode = $code;
  }

  public function getStatusCode() {
    return $this->statusCode;
  }

}