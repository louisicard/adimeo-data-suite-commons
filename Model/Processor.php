<?php

namespace AdimeoDataSuite\Model;


class Processor extends PersistentObject
{
  private $id;

  /**
   *
   * @var string
   */
  private $datasourceId;
  /**
   *
   * @var string
   */
  private $target;
  /**
   * @var array
   */
  private $targetSiblings;
  /**
   *
   * @var array
   */
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
   * @return string
   */
  public function getDatasourceId()
  {
    return $this->datasourceId;
  }

  /**
   * @param string $datasourceId
   */
  public function setDatasourceId($datasourceId)
  {
    $this->datasourceId = $datasourceId;
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
   * @return array
   */
  public function getTargetSiblings()
  {
    return $this->targetSiblings;
  }

  /**
   * @param array $targetSiblings
   */
  public function setTargetSiblings($targetSiblings)
  {
    $this->targetSiblings = $targetSiblings;
  }

  /**
   * @return array
   */
  public function getDefinition()
  {
    return $this->definition;
  }

  /**
   * @param array $definition
   */
  public function setDefinition($definition)
  {
    $this->definition = $definition;
  }


  function getName()
  {
    return $this->datasourceId . ' __TO__ ' . $this->target;
  }

  function getType()
  {
    return 'processor';
  }

  function __construct($id = null, $datasourceId = null, $target = '', $definition = array(), $targetSiblings = array())
  {
    $this->id = $id;
    $this->datasourceId = $datasourceId;
    $this->target = $target;
    $this->definition = $definition;
    $this->targetSiblings = $targetSiblings;
  }

}