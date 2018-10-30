<?php

namespace AdimeoDataSuite\Model;


use AdimeoDataSuite\Index\IndexManager;

abstract class Datasource extends PersistentObject
{
  private $id;
  private $settings;
  private $hasBatchExecution;

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
  public function getSettings()
  {
    return $this->settings;
  }

  /**
   * @param mixed $settings
   */
  public function setSettings($settings)
  {
    $this->settings = $settings;
  }

  /**
   * @return boolean
   */
  public function hasBatchExecution()
  {
    return $this->hasBatchExecution;
  }

  /**
   * @param boolean $hasBatchExecution
   */
  public function setHasBatchExecution($hasBatchExecution)
  {
    $this->hasBatchExecution = $hasBatchExecution;
  }

  /**
   * @return mixed
   */
  public function getName()
  {
    return isset($this->settings['name']) ? $this->settings['name'] : '';
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
   * @return string
   */
  abstract function getDisplayName();

  /**
   * @param array $args
   */
  abstract function execute($args, OutputManager $output);

  final function index($data) {
    $startTime = round(microtime(true) * 1000);
    $debugTimeStat = [];
    try {
      $smartMappersToDump = [];
      foreach ($this->execProcessors as $proc) {
        $document = array();
        foreach ($data as $k => $v) {
          $document['datasource.' . $k] = $v;
        }
        $definition = json_decode($proc->getDefinition(), true);
        foreach ($definition['filters'] as $filter) {
          $filterStartTime = round(microtime(true) * 1000);
          $className = $filter['class'];
          /** @var ProcessorFilter $procFilter */
          $procFilter = new $className(array());
          //$procFilter->setOutput($this->getOutput());
          $filterData = array();
          foreach ($filter['settings'] as $k => $v) {
            $filterData['setting_' . $k] = Parameter::injectParameters($v);
          }
          foreach ($filter['arguments'] as $arg) {
            $filterData['arg_' . $arg['key']] = $arg['value'];
          }
          $procFilter->setData($filterData);
          $procFilter->setAutoImplode($filter['autoImplode']);
          $procFilter->setAutoImplodeSeparator($filter['autoImplodeSeparator']);
          $procFilter->setAutoStriptags($filter['autoStriptags']);
          $procFilter->setIsHTML($filter['isHTML']);
          $filterOutput = $procFilter->execute($data);
          //if($filter['id'] == 36840)
          //  $indexManager->log('debug', 'URL : ' . $data['datasource.url'], $filterOutput);
          if (empty($data)) {
            break;
          }
          if(get_class($procFilter) == SmartMapper::class){
            /** @var ProcessorFilter $procFilter */
            $smartSettings = $procFilter->getSettings();
            if(isset($smartSettings['force_index']) && $smartSettings['force_index']){
              if(isset($filterOutput['smart_array'])) {
                $smartMappersToDump[] = $filterOutput['smart_array'];
              }
            }
          }
          foreach ($filterOutput as $k => $v) {
            if ($procFilter->getAutoImplode()) {
              $v = $this->implode($procFilter->getAutoImplodeSeparator(), $v);
            }
            if ($procFilter->getAutoStriptags()) {
              if ($procFilter->getIsHTML()) {
                if(!is_array($v)){
                  $v = $this->cleanNonUtf8Chars($this->extractTextFromHTML($v));
                }
                else{
                  foreach($v as $v_k => $v_v){
                    $v[$v_k] = $this->cleanNonUtf8Chars($this->extractTextFromHTML($v_v));
                  }
                }
              } else {
                if(!is_array($v)){
                  $v = $this->cleanNonUtf8Chars($this->extractTextFromXML($v));
                }
                else{
                  foreach($v as $v_k => $v_v){
                    $v[$v_k] = $this->cleanNonUtf8Chars($this->extractTextFromXML($v_v));
                  }
                }
              }
            }
            if ($v != null) {
              $data['filter_' . $filter['id'] . '.' . $k] = $v;
            }
            unset($v);
          }
          if($debug){
            $debugTimeStat['filter_' . $filter['id']] = round(microtime(true) * 1000) - $filterStartTime;
          }
          unset($filter);
          unset($procFilter);
          unset($filterOutput);
          unset($filterData);
        }
        if (!empty($data)) {
          $to_index = array();
          foreach ($definition['mapping'] as $k => $input) {
            if(strpos($input, '.smart_array') === FALSE) {
              if (isset($data[$input])) {
                if (is_array($data[$input]) && count($data[$input]) == 1) {
                  $to_index[$k] = $data[$input][0];
                } else {
                  $to_index[$k] = $data[$input];
                }
              }
            }
            else{
              if (isset($data[$input][$k])) {
                if (is_array($data[$input][$k]) && count($data[$input][$k]) == 1) {
                  $to_index[$k] = $data[$input][$k][0];
                } else {
                  $to_index[$k] = $data[$input][$k];
                }
              }
            }
          }
          //taking care of smart mappers which force indexing all their fields
          foreach($smartMappersToDump as $smartMapper){
            foreach($smartMapper as $k => $v){
              if(!is_array($v)) {
                $to_index[$k] = trim($this->cleanNonUtf8Chars($v));
              }
              else{
                foreach($v as $vv){
                  if(is_array($vv)){
                    $to_index[$k][] = trim($this->cleanNonUtf8Chars($vv));
                  }
                }
              }
            }
          }
          $target_r = explode('.', $definition['target']);
          $indexName = $target_r[0];
          $mappingName = $target_r[1];
          $indexStartTime = round(microtime(true) * 1000);
          $this->indexDocument($indexName, $mappingName, $to_index);
          if ($debug && !$this->isHasBatchExecution()) {
            try {
              $debugTimeStat['indexing'] = round(microtime(true) * 1000) - $indexStartTime;
              $debugTimeStat['global'] = round(microtime(true) * 1000) - $startTime;
              IndexManager::getInstance()->log('debug', 'Timing info', $debugTimeStat, $this);
              IndexManager::getInstance()->log('debug', 'Indexing document from datasource "' . $this->getName() . '"', $to_index, $this);
            } catch (Exception $ex) {

            } catch (\Exception $ex2) {

            }
          }

        }
        unset($proc);
      }
      if(isset($definition))
        unset($definition);
      if(isset($processors))
        unset($processors);
      if(isset($data))
        unset($data);
      if(isset($to_index))
        unset($to_index);
    } catch (Exception $ex) {
      //var_dump($ex->getMessage());
      IndexManager::getInstance()->log('error', 'Exception occured while indexing document from datasource "' . $this->getName() . '"', array(
        'Exception type' => get_class($ex),
        'Message' => $ex->getMessage(),
        'File' => $ex->getFile(),
        'Line' => $ex->getLine(),
        'Data in process' => isset($data) ? $this->truncateArray($data) : array(),
      ), $this);
    } catch (\Exception $ex2) {
      //var_dump($ex2);
      IndexManager::getInstance()->log('error', 'Exception occured while indexing document from datasource "' . $this->getName() . '"', array(
        'Exception type' => get_class($ex2),
        'Message' => $ex2->getMessage(),
        'File' => $ex2->getFile(),
        'Line' => $ex2->getLine(),
        'Data in process' => isset($data) ? $this->truncateArray($data) : array(),
      ), $this);
    }

    gc_enable();
    gc_collect_cycles();
  }

  /** @var IndexManager */
  private $execIndexManager = null;

  /** @var Processor[] */
  private $execProcessors = [];

  final function initForExecution(IndexManager $indexManager) {
    $this->execIndexManager = $indexManager;
    $this->execProcessors = $this->execIndexManager->listObjects('processor', null, 0, 10000, 'asc', array(
      'tags' => 'datasource_id=' . $this->getId()
    ));
  }

  final function injectParameters($string) {
    preg_match_all('/(?<parameter>%[^%]*%)/i', $string, $matches);
    if(isset($matches['parameter'])) {
      foreach($matches['parameter'] as $param) {
        $name = trim($param, '%');
        $parameter = $this->execIndexManager->findObject($name);
        if($parameter != null) {
          $string = str_replace('%' . $name . '%', $parameter->getValue(), $string);
        }
      }
    }
    return $string;
  }

}