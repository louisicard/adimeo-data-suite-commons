<?php

namespace AdimeoDataSuite\Model;


use AdimeoDataSuite\Index\IndexManager;
use AdimeoDataSuite\ProcessorFilter\MatchingListFilter;

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

  public function getTags()
  {
    if(strpos($this->getTarget(), '.') === 0) {
      $indexName = '.' . explode('.', $this->getTarget())[1];
    }
    else {
      $indexName = explode('.', $this->getTarget())[0];
    }
    $tags = array(
      'datasource_id=' . $this->getDatasourceId(),
      'index_name=' . $indexName
    );

    foreach($this->getTargetSiblings() as $sibling) {
      $tags[] = 'datasource_id=' . $sibling;
    }

    return $tags;
  }

  function export(IndexManager $indexManager)
  {
    $data = [];
    if(strpos($this->getTarget(), '.') === 0) {
      $indexName = '.' . explode('.', $this->getTarget())[1];
      $mappingName = explode('.', $this->getTarget())[2];
    }
    else {
      $indexName = explode('.', $this->getTarget())[0];
      $mappingName = explode('.', $this->getTarget())[1];
    }
    $data['index'] = $indexManager->getIndex($indexName);
    $data['mapping'][$mappingName] = $indexManager->getMapping($indexName, $mappingName);
    $data['datasource'] = $indexManager->findObject('datasource', $this->getDatasourceId())->export($indexManager);
    if($this->targetSiblings != null && is_array($this->targetSiblings)) {
      foreach ($this->targetSiblings as $sibling) {
        $data['siblings'][] = $indexManager->findObject('datasource', $sibling)->export($indexManager);
      }
    }

    $definition = json_decode($this->getDefinition(), TRUE);
    if(isset($definition['filters'])) {
      foreach($definition['filters'] as $filter) {
        if($filter['class'] == MatchingListFilter::class) {
          if(isset($filter['settings']['matching_list_id'])) {
            $data['matching_lists'][] = $indexManager->findObject('matching_list', $filter['settings']['matching_list_id'])->export($indexManager);
          }
        }
      }
    }

    $data['processor'] = self::serialize();

    return json_encode($data);
  }

  static function import($data, IndexManager $indexManager, $override = false)
  {
    $data = json_decode($data, true);
    $indexName = array_keys($data['index'])[0];
    $mappingName = array_keys($data['mapping'])[0];

    //Index
    $indexExists = $indexManager->getIndex($indexName) != null;
    if($indexExists) {
      if ($override) {
        $indexManager->deleteIndex($indexName);
        $indexManager->createIndex($indexName, $data['index'][$indexName]['settings']['index']);
      } else {
        $indexManager->updateIndex($indexName, $data['index'][$indexName]['settings']['index']);
      }
    }
    else {
      $indexManager->createIndex($indexName, $data['index'][$indexName]['settings']['index']);
    }

    //Mapping
    $indexManager->putMapping($indexName, $mappingName, $data['mapping'][$mappingName]['properties'], isset($data['mapping'][$mappingName]['dynamic_templates']) ? $data['mapping'][$mappingName]['dynamic_templates'] : NULL);

    //Datasource
    PersistentObject::import($data['datasource'], $indexManager);

    //Sibligings
    if(isset($data['siblings'])) {
      foreach($data['siblings'] as $sibling) {
        PersistentObject::import($sibling, $indexManager);
      }
    }

    //Matching lists
    if(isset($data['matching_lists'])) {
      foreach($data['matching_lists'] as $matchingList) {
        PersistentObject::import($matchingList, $indexManager);
      }
    }

    //Processor
    /** @var Processor $processor */
    $processor = PersistentObject::unserialize($data['processor']);
    $indexManager->persistObject($processor);

    return $processor;
  }


}