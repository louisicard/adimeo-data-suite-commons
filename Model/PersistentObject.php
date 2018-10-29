<?php

namespace AdimeoDataSuite\Model;

abstract class PersistentObject
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

}