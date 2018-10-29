<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 28/10/2018
 * Time: 11:31
 */

namespace AdimeoDataSuite\Index;


use AdimeoDataSuite\Exception\DictionariesPathNotDefinedException;
use AdimeoDataSuite\Model\SecurityContext;

class SynonymsDictionariesManager
{

  private $dictionariesPath;

  public function __construct($dictionariesPath = null) {
    $this->dictionariesPath = $dictionariesPath;
  }

  public function getDictionariesPath() {
    if($this->dictionariesPath == NULL
      || $this->dictionariesPath == ''
      || !file_exists($this->dictionariesPath)
      || !is_dir($this->dictionariesPath)
      || !is_writable($this->dictionariesPath))
      throw new DictionariesPathNotDefinedException();
    return $this->dictionariesPath;
  }

  public function getDictionaries(SecurityContext $context = NULL) {
    if($this->dictionariesPath == NULL
        || $this->dictionariesPath == ''
        || !file_exists($this->dictionariesPath)
        || !is_dir($this->dictionariesPath)
        || !is_writable($this->dictionariesPath))
      throw new DictionariesPathNotDefinedException();
    $files = scandir($this->dictionariesPath);
    $dictionaries = [];
    foreach($files as $file) {
      if(is_file($this->dictionariesPath . DIRECTORY_SEPARATOR . $file)) {
        if($context == NULL || $context->isAdmin() || in_array($file, $context->getRestrictions()['dictionaries'])) {
          $dictionaries[] = array(
            'path' => $this->dictionariesPath . DIRECTORY_SEPARATOR . $file,
            'name' => $file
          );
        }
      }
    }
    return $dictionaries;
  }

}