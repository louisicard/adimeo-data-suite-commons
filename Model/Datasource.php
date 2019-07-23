<?php

namespace AdimeoDataSuite\Model;


use AdimeoDataSuite\Index\IndexManager;
use AdimeoDataSuite\PDO\PDOPool;
use AdimeoDataSuite\ProcessorFilter\SmartMapper;

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
  abstract function getExecutionArgumentFields();

  /**
   * @return string
   */
  abstract function getDisplayName();

  /**
   * @param array $args
   */
  abstract function execute($args);

  final function startExecution($args) {

    $this->execute($args);

    //We empty the batch stack at the end of execution if there are documents left to index
    if($this->hasBatchExecution()) {
      $this->emptyBatchStack();
    }
  }

  final function index($data, $debug = false) {
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
            $filterData['setting_' . $k] = $this->injectParameters($v);
          }
          foreach ($filter['arguments'] as $arg) {
            $filterData['arg_' . $arg['key']] = $arg['value'];
          }
          $procFilter->setData($filterData);
          $procFilter->setAutoImplode($filter['autoImplode']);
          $procFilter->setAutoImplodeSeparator($filter['autoImplodeSeparator']);
          $procFilter->setAutoStriptags($filter['autoStriptags']);
          $procFilter->setIsHTML($filter['isHTML']);
          $filterOutput = $procFilter->execute($document, $this);
          if (empty($document)) {
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
              $document['filter_' . $filter['id'] . '.' . $k] = $v;
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
        if (!empty($document)) {
          $to_index = array();
          foreach ($definition['mapping'] as $k => $input) {
            if(strpos($input, '.smart_array') === FALSE) {
              if (isset($document[$input])) {
                if (is_array($document[$input]) && count($document[$input]) == 1) {
                  $to_index[$k] = $document[$input][0];
                } else {
                  $to_index[$k] = $document[$input];
                }
              }
            }
            else{
              if (isset($document[$input][$k])) {
                if (is_array($document[$input][$k]) && count($document[$input][$k]) == 1) {
                  $to_index[$k] = $document[$input][$k][0];
                } else {
                  $to_index[$k] = $document[$input][$k];
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
          //Let's clean data before indexing
          $this->cleanArray($to_index);

          $target_r = explode('.', $definition['target']);
          $indexName = $target_r[0];
          $mappingName = $target_r[1];
          $indexStartTime = round(microtime(true) * 1000);
          $this->indexDocument($indexName, $mappingName, $to_index);
          if ($debug && !$this->hasBatchExecution()) {
            try {
              $debugTimeStat['indexing'] = round(microtime(true) * 1000) - $indexStartTime;
              $debugTimeStat['global'] = round(microtime(true) * 1000) - $startTime;
              //IndexManager::getInstance()->log('debug', 'Timing info', $debugTimeStat, $this);
              //IndexManager::getInstance()->log('debug', 'Indexing document from datasource "' . $this->getName() . '"', $to_index, $this);

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
      if(isset($document))
        unset($document);
      if(isset($to_index))
        unset($to_index);
    } catch (\Exception $ex) {
      $this->getOutputManager()->writeLn('An exception has occured => ' . $ex->getMessage());
      $this->getOutputManager()->writeLn($ex->getTraceAsString());
//      IndexManager::getInstance()->log('error', 'Exception occured while indexing document from datasource "' . $this->getName() . '"', array(
//        'Exception type' => get_class($ex2),
//        'Message' => $ex2->getMessage(),
//        'File' => $ex2->getFile(),
//        'Line' => $ex2->getLine(),
//        'Data in process' => isset($document) ? $this->truncateArray($document) : array(),
//      ), $this);
    }

    gc_enable();
    gc_collect_cycles();
  }

  private function indexDocument($indexName, $mappingName, $to_index){
    if($this->hasBatchExecution()){
      $this->batchStack[] = array(
        'indexName' => $indexName,
        'mappingName' => $mappingName,
        'body' => $to_index,
      );
      if(count($this->batchStack) >= static::BATCH_STACK_SIZE){
        $this->emptyBatchStack();
      }
    }
    else{
      $this->execIndexManager->indexDocument($indexName, $mappingName, $to_index);
    }
  }

  private $batchStack = [];
  const BATCH_STACK_SIZE = 500;

  final function emptyBatchStack(){
    $this->execIndexManager->bulkIndex($this->batchStack);
    unset($this->batchStack);
    if ($this->getOutputManager() != null) {
      $this->getOutputManager()->writeln('Indexing documents in batch stack (stack size is ' . static::BATCH_STACK_SIZE . ')');
    }
    $this->batchStack = [];
  }

  /** @var IndexManager */
  private $execIndexManager = null;

  /** @var Processor[] */
  private $execProcessors = [];

  /** @var Parameter[]  */
  private $parameters = [];

  /**
   * @var OutputManager
   */
  private $outputManager;

  /**
   * @var PDOPool
   */
  private $pdoPool;

  final function initForExecution(IndexManager $indexManager, OutputManager $output, PDOPool $pdoPool) {
    $this->execIndexManager = $indexManager;

    $this->outputManager = $output;

    $this->pdoPool = $pdoPool;

    $this->execProcessors = $this->execIndexManager->listObjects('processor', null, 0, 10000, 'asc', array(
      'tags' => 'datasource_id=' . $this->getId()
    ));

    $this->parameters = $indexManager->listObjects('parameter');

    $settings = $this->getSettings();
    foreach($settings as $k => $v) {
      $settings[$k] = $this->injectParameters($v);
    }
    $this->setSettings($settings);
  }

  /**
   * @return OutputManager
   */
  public function getOutputManager()
  {
    return $this->outputManager;
  }

  /**
   * @param OutputManager $outputManager
   */
  public function setOutputManager($outputManager)
  {
    $this->outputManager = $outputManager;
  }

  /**
   * @return PDOPool
   */
  public function getPDOPool()
  {
    return $this->pdoPool;
  }

  /**
   * @return IndexManager
   */
  public function getExecIndexManager()
  {
    return $this->execIndexManager;
  }

  final function injectParameters($string) {
    preg_match_all('/(?<parameter>%[^%]*%)/i', $string, $matches);
    if(isset($matches['parameter'])) {
      foreach($matches['parameter'] as $param) {
        $name = trim($param, '%');
        $parameter = null;
        foreach($this->parameters as $p) {
          if($p->getName() == $name) {
            $parameter = $p;
          }
        }
        if($parameter != null) {
          $string = str_replace('%' . $name . '%', $parameter->getValue(), $string);
        }
      }
    }
    return $string;
  }

  protected function implode($separator, $input) {
    if(is_array($input))
      return implode($separator, $input);
    else
      return $input;
  }

  protected function extractTextFromHTML($html) {
    $html = str_replace('&nbsp;', ' ', $html);
    $html = str_replace('&rsquo;', ' ', $html);
    try {
      $tidy = tidy_parse_string($html, array(), 'utf8');
      $body = tidy_get_body($tidy);
      if($body != null)
        $html = $body->value;
    } catch (\Exception $ex) {

    }
    $html = html_entity_decode($html, ENT_COMPAT | ENT_HTML401, 'utf-8');
    $html = trim(preg_replace('#<[^>]+>#', ' ', $html));
    $html_no_multiple_spaces = trim(preg_replace('!\s+!', ' ', $html));
    if(preg_match('!\s+!', $html) && !empty($html_no_multiple_spaces)){
      $html = $html_no_multiple_spaces;
    }
    $clean_html = html_entity_decode(trim(htmlentities($html, null, 'utf-8')));
    $r = empty($clean_html) ? $html : $clean_html;

    return $r;
  }

  protected function extractTextFromXML($xml) {
    return strip_tags($xml);
  }

  private function cleanArray(&$array) {
    foreach($array as $k => $v) {
      if(is_array($v)) {
        $this->cleanArray($array[$k]);
      }
      elseif(is_numeric($v)) {
        $array[$k] = $v;
      }
      else {
        $array[$k] = $this->cleanNonUtf8Chars($v);
      }
    }
  }

  private function cleanNonUtf8Chars($text){
    if($text == null || empty($text)){
      return $text;
    }
    $regex = <<<'END'
/
  (
    (?: [\x00-\x7F]                 # single-byte sequences   0xxxxxxx
    |   [\xC0-\xDF][\x80-\xBF]      # double-byte sequences   110xxxxx 10xxxxxx
    |   [\xE0-\xEF][\x80-\xBF]{2}   # triple-byte sequences   1110xxxx 10xxxxxx * 2
    |   [\xF0-\xF7][\x80-\xBF]{3}   # quadruple-byte sequence 11110xxx 10xxxxxx * 3 
    ){1,100}                        # ...one or more times
  )
| .                                 # anything else
/x
END;
    return preg_replace($regex, '$1', $text);
  }

  /**
   * @var MatchingList[]
   */
  private $matchingLists = [];

  final function getMatchingList($id) {
    if(!isset($this->matchingLists[$id])) {
      $this->matchingLists[$id] = $this->execIndexManager->findObject('matching_list', $id);
    }
    return $this->matchingLists[$id];
  }


}