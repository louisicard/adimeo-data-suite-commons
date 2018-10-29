<?php

namespace AdimeoDataSuite\Model;

abstract class PersistentObject
{

  abstract function getId();
  abstract function setId($id);
  abstract function getName();
  abstract function getType();
  public function serialize() {
    return serialize($this);
  }

  public static function unserialize($str) {
    return unserialize($str);
  }

}