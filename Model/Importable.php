<?php

namespace AdimeoDataSuite\Model;


use AdimeoDataSuite\Index\IndexManager;

interface Importable
{
  /**
   * @param string $data
   * @param IndexManager $indexManager
   */
  function import($data, IndexManager $indexManager);

}