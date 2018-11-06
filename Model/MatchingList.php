<?php

namespace AdimeoDataSuite\Model;


class MatchingList extends PersistentObject
{
  private $id;
  private $name;
  private $list;

  function __construct($name, $list = '{}', $id = null) {
    $this->name = $name;
    $this->list = $list;
    $this->id = $id;
  }

  function getId()
  {
    return $this->id;
  }

  function setId($id)
  {
    $this->id = $id;
  }

  function getName()
  {
    return $this->name;
  }

  function setName($name)
  {
    $this->name = $name;
  }

  function getType()
  {
    return 'matching_list';
  }

  /**
   * @return mixed
   */
  public function getList()
  {
    return $this->list;
  }

  /**
   * @param mixed $list
   */
  public function setList($list)
  {
    $this->list = $list;
  }


}