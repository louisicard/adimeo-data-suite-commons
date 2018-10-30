<?php

namespace AdimeoDataSuite\Model;


class SavedQuery extends PersistentObject
{

  private $id;
  private $target;
  private $definition;

  /**
   * @return mixed
   */
  public function getId()
  {
    return $this->id;
  }

  /**
   * @param mixed $id
   */
  public function setId($id)
  {
    $this->id = $id;
  }

  /**
   * @return mixed
   */
  public function getTarget()
  {
    return $this->target;
  }

  /**
   * @param mixed $target
   */
  public function setTarget($target)
  {
    $this->target = $target;
  }

  /**
   * @return mixed
   */
  public function getDefinition()
  {
    return $this->definition;
  }

  /**
   * @param mixed $definition
   */
  public function setDefinition($definition)
  {
    $this->definition = $definition;
  }

  function getName()
  {
    return 'Saved query for ' . $this->getTarget();
  }

  function getType()
  {
    return 'saved_query';
  }

  public function getTags()
  {
    if(strpos($this->getTarget(), '.') === 0) {
      $indexName = '.' . explode('.', $this->getTarget())[1];
    }
    else {
      $indexName = explode('.', $this->getTarget())[0];
    }
    return array(
      'index_name=' . $indexName
    );
  }

}