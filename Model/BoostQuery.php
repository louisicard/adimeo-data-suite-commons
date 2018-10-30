<?php

namespace AdimeoDataSuite\Model;


class BoostQuery extends PersistentObject
{
  /** @var  string */
  private $id;
  /** @var  string */
  private $target;
  /** @var  string */
  private $definition;

  /**
   * BoostQuery constructor.
   * @param string $id
   * @param string $target
   * @param string $definition
   */
  public function __construct($id, $target, $definition)
  {
    $this->id = $id;
    $this->target = $target;
    $this->definition = $definition;
  }

  /**
   * @return string
   */
  public function getId()
  {
    return $this->id;
  }

  /**
   * @param string $id
   */
  public function setId($id)
  {
    $this->id = $id;
  }

  /**
   * @return string
   */
  public function getTarget()
  {
    return $this->target;
  }

  /**
   * @param string $target
   */
  public function setTarget($target)
  {
    $this->target = $target;
  }

  /**
   * @return string
   */
  public function getDefinition()
  {
    return $this->definition;
  }

  /**
   * @param string $definition
   */
  public function setDefinition($definition)
  {
    $this->definition = $definition;
  }

  function getName()
  {
    return 'Boosting query for ' . $this->getTarget();
  }

  function getType()
  {
    return 'boost_query';
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