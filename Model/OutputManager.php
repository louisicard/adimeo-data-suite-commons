<?php

namespace AdimeoDataSuite\Model;


interface OutputManager
{

  /**
   * @param string $text
   */
  function writeLn($text);

  /**
   * @param array $array
   */
  function dumpArray($array);

}