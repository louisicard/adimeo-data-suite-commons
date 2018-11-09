<?php

namespace AdimeoDataSuite\Model;

use AdimeoDataSuite\Index\IndexManager;

abstract class PersistentObject implements Importable, Exportable
{

  abstract function getId();
  abstract function setId($id);
  abstract function getName();
  abstract function getType();

  /**
   * @var \DateTime
   */
  protected $created;

  /**
   * @var \DateTime
   */
  protected $updated;

  protected $createdBy = NULL;

  /**
   * @return \DateTime
   */
  public function getCreated()
  {
    return $this->created;
  }

  /**
   * @param \DateTime $created
   */
  public function setCreated($created)
  {
    $this->created = $created;
  }

  /**
   * @return \DateTime
   */
  public function getUpdated()
  {
    return $this->updated;
  }

  /**
   * @param \DateTime $updated
   */
  public function setUpdated($updated)
  {
    $this->updated = $updated;
  }

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
    return json_encode(array('data' => $this->serialize()));
  }

  static function import($data, IndexManager $indexManager, $override = false)
  {
    $data = json_decode($data, true);
    /** @var PersistentObject $obj */
    $obj = self::unserialize($data['data']);
    if($override)
      $indexManager->deleteObject($obj->getId());
    $indexManager->persistObject($obj);

    return $obj;
  }


}