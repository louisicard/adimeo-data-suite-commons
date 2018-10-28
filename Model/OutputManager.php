<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 28/10/2018
 * Time: 17:26
 */

namespace AdimeoDataSuite\Bundle\CommonsBundle\Model;


interface OutputManager
{

  /**
   * @param string $text
   */
  function writeLn($text);

}