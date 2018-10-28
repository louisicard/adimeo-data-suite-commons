<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 28/10/2018
 * Time: 16:41
 */

namespace AdimeoDataSuite\Bundle\CommonsBundle\Model;


abstract class Datasource extends PersistentObject
{
  private $id;

  final function getId()
  {
    return $this->id;
  }

  final function setId($id)
  {
    $this->id = $id;
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
   * @return Datasource
   */
  abstract static function instantiate($settings);

  /**
   * @param array $args
   */
  abstract function execute($args);


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


}