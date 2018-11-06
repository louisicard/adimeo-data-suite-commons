<?php

namespace AdimeoDataSuite\Model;


use AdimeoDataSuite\Index\IndexManager;

interface Exportable
{

  /**
   * @param IndexManager $indexManager
   * @return string
   */
  function export(IndexManager $indexManager);

}