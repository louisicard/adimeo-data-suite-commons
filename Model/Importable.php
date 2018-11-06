<?php

namespace AdimeoDataSuite\Model;


use AdimeoDataSuite\Index\IndexManager;

interface Importable
{
  /**
   * @param string $data
   * @param IndexManager $indexManager
   * @param boolean $override
   */
  function import($data, IndexManager $indexManager, $override = false);

}