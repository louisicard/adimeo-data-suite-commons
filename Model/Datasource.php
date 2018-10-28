<?php

namespace AdimeoDataSuite\Bundle\CommonsBundle\Model;


abstract class Datasource extends PersistentObject
{
  private $id;
  private $displayName;

  final function getId()
  {
    return $this->id;
  }

  final function setId($id)
  {
    $this->id = $id;
  }

  /**
   * @return mixed
   */
  public function getDisplayName()
  {
    return $this->displayName;
  }

  /**
   * @param mixed $displayName
   */
  public function setDisplayName($displayName)
  {
    $this->displayName = $displayName;
  }

  final function getType()
  {
    return 'datasource';
  }

  /**
   * @return array
   */
  abstract function getOutputFields();

  /**
   * @return array
   */
  abstract function getSettingFields();

  /**
   * @return array
   */
  abstract function getExecutionArguments();

  /**
   * @param array $settings
   */
  abstract function hydrate($settings);

  /**
   * @param array $args
   */
  abstract function execute($args, OutputManager $output);


  private $createdBy;

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

  final function index($data) {

  }


}