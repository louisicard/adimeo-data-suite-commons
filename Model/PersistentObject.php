<?php

namespace AdimeoDataSuite\Model;

use AdimeoDataSuite\Index\IndexManager;

abstract class PersistentObject implements Importable, Exportable
{

  abstract function getId();
  abstract function setId($id);
  abstract function getName();
  abstract function getType();

  protected $createdBy = NULL;

  /**
   * @return mixed
   */
  public function getCreatedBy()
  {
    return $this->createdBy;
  }

  /**
   * @param mixed $createdBy
   */
  public function setCreatedBy($createdBy)
  {
    $this->createdBy = $createdBy;
  }

  public function serialize() {
    return serialize($this);
  }

  public static function unserialize($str) {
    return unserialize($str);
  }

  public function getTags() {
    return [];
  }

  function export(IndexManager $indexManager)
  {
    return serialize($this);
  }

  function import($data, IndexManager $indexManager, $override = false)
  {
    /** @var PersistentObject $obj */
    $obj = self::unserialize($data);
    if($override)
      $indexManager->deleteObject($obj->getId());
    $indexManager->persistObject($obj);
  }


}