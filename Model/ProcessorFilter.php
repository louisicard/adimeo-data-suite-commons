<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 28/10/2018
 * Time: 19:50
 */

namespace AdimeoDataSuite\Bundle\CommonsBundle\Model;


abstract class ProcessorFilter
{

  private $settings;
  private $inStackName;
  private $autoImplode;
  private $autoImplodeSeparator;
  private $autoStriptags;
  private $isHTML;

  function __construct($data = array()) {
    $this->inStackName = '';
    $this->autoImplode = false;
    $this->autoImplodeSeparator = '';
    $this->autoStriptags = false;
    $this->isHTML = false;
    $this->setData($data);
  }

  /**
   * @return string
   */
  abstract function getDisplayName();

  public function getArgumentsAndSettings() {
    $r = array();
    foreach ($this->getSettings() as $k => $setting) {
      $r['setting_' . $k] = $setting;
    }
    foreach ($this->argumentsData as $arg) {
      $r['arg_' . $arg['key']] = $arg['value'];
    }
    $r['in_stack_name'] = $this->inStackName;
    $r['autoImplode'] = $this->autoImplode;
    $r['autoImplodeSeparator'] = $this->autoImplodeSeparator;
    $r['autoStriptags'] = $this->autoStriptags;
    $r['isHTML'] = $this->isHTML;
    return $r;
  }

  private $argumentsData;

  function getArgumentsData() {
    return $this->argumentsData;
  }

  abstract function getSettingsFields();

  /**
   * @return array
   */
  function getSettings() {
    return $this->settings;
  }

  /**
   * @param string[] $settings
   */
  function setData($data) {
    $settings = array();
    $arguments = array();
    foreach ($data as $k => $v) {
      if (strpos($k, 'setting_') === 0) {
        $settings[substr($k, strlen('setting_'))] = $v;
      }
      if (strpos($k, 'arg_') === 0) {
        $arguments[] = array('key' => substr($k, strlen('arg_')), 'value' => $v);
      }
      if ($k == 'in_stack_name')
        $this->inStackName = $v;
      if ($k == 'autoImplode')
        $this->autoImplode = $v;
      if ($k == 'autoImplodeSeparator')
        $this->autoImplodeSeparator = $v;
      if ($k == 'autoStriptags')
        $this->autoStriptags = $v;
      if ($k == 'isHTML')
        $this->isHTML = $v;
    }
    $this->argumentsData = $arguments;
    $this->settings = $settings;
  }

  /**
   * @return string[]
   */
  abstract function getFields();

  /**
   * Must return an associative array arg_key => arg_label
   */
  abstract function getArguments();

  abstract function execute(&$document);

  protected function getArgumentValue($argName, $document) {
    foreach ($this->getArgumentsData() as $arg) {
      if ($arg['key'] == $argName && isset($document[$arg['value']]))
        return $document[$arg['value']];
    }
    return '';
  }

  function getInStackName() {
    return $this->inStackName;
  }

  function setInStackName($inStackName) {
    $this->inStackName = $inStackName;
  }

  function getAutoImplode() {
    return $this->autoImplode;
  }

  function getAutoImplodeSeparator() {
    return $this->autoImplodeSeparator;
  }

  function getAutoStriptags() {
    return $this->autoStriptags;
  }

  function setAutoImplode($autoImplode) {
    $this->autoImplode = $autoImplode;
  }

  function setAutoImplodeSeparator($autoImplodeSeparator) {
    $this->autoImplodeSeparator = $autoImplodeSeparator;
  }

  function setAutoStriptags($autoStriptags) {
    $this->autoStriptags = $autoStriptags;
  }

  function getIsHTML() {
    return $this->isHTML;
  }

  function setIsHTML($isHTML) {
    $this->isHTML = $isHTML;
  }
}