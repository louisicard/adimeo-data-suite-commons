<?php

namespace AdimeoDataSuite\Index;

use AdimeoDataSuite\Exception\ServerClientException;
use AdimeoDataSuite\Model\Autopromote;
use AdimeoDataSuite\Model\PersistentObject;
use AdimeoDataSuite\Model\SecurityContext;

class IndexManager
{

  const APP_INDEX_NAME = '.adimeo_data_suite';
  const APP_RECO_INDEX_NAME = '.adimeo_data_suite_reco';

  /**
   * @var ServerClient
   */
  private $serverClient;

  /**
   * @var bool
   */
  private $isLegacy = false;

  public function __construct($elasticsearchServerUrl, $isLegacy = false) {

    $this->serverClient = new ServerClient($elasticsearchServerUrl);

    if($isLegacy === '1' or $isLegacy === 1) {
      $this->isLegacy = true;
    }
  }

  public function isLegacy() {
    return $this->isLegacy;
  }

  public function getServerClient() {
    return $this->serverClient;
  }

  public function getServerInfo() {
    return array(
      'server_info' => $this->getServerClient()->info(),
      'health' => $this->getServerClient()->clusterHealth(),
      'stats' => $this->getServerClient()->clusterStats(),
    );
  }

  public function getIndicesList(SecurityContext $context = NULL) {
    $mappings = $this->getServerClient()->indicesMappings();
    $settings = $this->getServerClient()->indicesSettings();
    $indices = $this->getServerClient()->clusterStats()['indices'];
    foreach($indices as $index => $stats) {
      if(isset($settings[$index])) {
        $indices[$index]['settings'] = $settings[$index]['settings'];
      }
      if(isset($mappings[$index])) {
        $indices[$index]['mappings'] = $mappings[$index]['mappings'];
      }
    }
    ksort($indices);
    if($context != NULL && !$context->isAdmin()) {
      foreach($indices as $k => $data) {
        if(!in_array($k, $context->getRestrictions()['indexes']))
          unset($indices[$k]);
      }
    }
    return $indices;
  }

  function getIndicesInfo(SecurityContext $context = NULL)
  {
    $info = array();
    $stats = $this->getServerClient()->clusterStats();
    foreach ($stats['indices'] as $index_name => $stat) {
      $info[$index_name] = array(
        'count' => $stat['total']['docs']['count'] - $stat['total']['docs']['deleted'],
        'size' => round($stat['total']['store']['size_in_bytes'] / 1024 / 1024, 2) . ' MB',
      );
      $mappings = $this->getServerClient()->mapping($index_name);
      if($this->isLegacy()) {
        foreach ($mappings[$index_name]['mappings'] as $mapping => $properties) {
          $info[$index_name]['mappings'][] = array(
            'name' => $mapping,
            'field_count' => count($properties['properties']),
          );
        }
      }
      else {
        if(isset($mappings[$index_name]['mappings']['properties'])) {
          $info[$index_name]['mappings'][] = array(
            'name' => '_doc',
            'field_count' => count($mappings[$index_name]['mappings']['properties']),
          );
        }
      }
    }
    ksort($info);
    if($context != NULL && !$context->isAdmin()) {
      foreach($info as $k => $data) {
        if(!in_array($k, $context->getRestrictions()['indexes']))
          unset($info[$k]);
      }
    }
    unset($stats);
    return $info;
  }

  public function getIndex($indexName) {
    try {
      return $this->getServerClient()->indexSettings($indexName);
    }
    catch(ServerClientException $ex) {
      return null;
    }
  }

  public function createIndex($indexName, $settings) {
    if (isset($settings['creation_date']))
      unset($settings['creation_date']);
    if (isset($settings['version']))
      unset($settings['version']);
    if (isset($settings['uuid']))
      unset($settings['uuid']);
    if (isset($settings['provided_name']))
      unset($settings['provided_name']);
    $settings['analysis']['analyzer']['transliterator'] = array(
      'filter' => array('asciifolding', 'lowercase'),
      'tokenizer' => 'keyword'
    );
    if(!$this->isLegacy()) {
      foreach($settings['analysis']['analyzer'] as $analyzer => $def) {
        if(isset($def['filter'])) {
          foreach($def['filter'] as $index => $filter) {
            if($filter == 'standard') {
              unset($settings['analysis']['analyzer'][$analyzer]['filter'][$index]);
            }
          }
        }
      }
    }
    return $this->getServerClient()->createIndex($indexName, $settings);
  }

  function updateIndex($indexName, $settings)
  {
    $this->getServerClient()->closeIndex($indexName);
    if (isset($settings['creation_date']))
      unset($settings['creation_date']);
    if (isset($settings['version']))
      unset($settings['version']);
    if (isset($settings['uuid']))
      unset($settings['uuid']);
    if (isset($settings['number_of_shards']))
      unset($settings['number_of_shards']);
    if (isset($settings['number_of_replicas']))
      unset($settings['number_of_replicas']);
    //if (isset($settings['analysis']))
    //  unset($settings['analysis']);
    if (isset($settings['provided_name']))
      unset($settings['provided_name']);
    if (count($settings) > 0) {
      try {
        $this->getServerClient()->updateIndex($indexName, $settings);
      }
      catch(\Exception $ex) {

      }
    }
    $this->getServerClient()->openIndex($indexName);
  }

  public function deleteIndex($indexName) {
    return $this->getServerClient()->deleteIndex($indexName);
  }

  /**
   *
   * @param string $indexName
   * @return string[]
   */
  function getAnalyzers($indexName)
  {
    $analyzers = array('standard', 'simple', 'whitespace', 'stop', 'keyword', 'pattern', 'language', 'snowball');
    $settings = $this->getServerClient()->indexSettings($indexName);
    if (isset($settings[$indexName]['settings']['index']['analysis']['analyzer'])) {
      foreach ($settings[$indexName]['settings']['index']['analysis']['analyzer'] as $analyzer => $definition) {
        $analyzers[] = $analyzer;
      }
    }
    unset($settings);
    return $analyzers;
  }

  /**
   *
   * @return string[]
   */
  function getFieldTypes()
  {
    $types = array('integer', 'long', 'float', 'double', 'boolean', 'date', 'ip', 'geo_point');
    if($this->getServerMajorVersionNumber() >= 5){
      $types = array_merge($types, array('text', 'keyword'));
    }
    else{
      $types = array_merge($types, array('string'));
    }
    asort($types);
    return $types;
  }

  /**
   *
   * @return string[]
   */
  function getDateFormats()
  {
    return array('basic_date', 'basic_date_time', 'basic_date_time_no_millis', 'basic_ordinal_date', 'basic_ordinal_date_time', 'basic_ordinal_date_time_no_millis', 'basic_time', 'basic_time_no_millis', 'basic_t_time', 'basic_t_time_no_millis', 'basic_week_date', 'basic_week_date_time', 'basic_week_date_time_no_millis', 'date', 'date_hour', 'date_hour_minute', 'date_hour_minute_second', 'date_hour_minute_second_fraction', 'date_hour_minute_second_millis', 'date_optional_time', 'date_time', 'date_time_no_millis', 'hour', 'hour_minute', 'hour_minute_second', 'hour_minute_second_fraction', 'hour_minute_second_millis', 'ordinal_date', 'ordinal_date_time', 'ordinal_date_time_no_millis', 'time', 'time_no_millis', 't_time', 't_time_no_millis', 'week_date', 'week_date_time', 'weekDateTimeNoMillis', 'week_year', 'weekyearWeek', 'weekyearWeekDay', 'year', 'year_month', 'year_month_day');
  }

  function getServerMajorVersionNumber(){
    $info = $this->getServerInfo()['server_info'];
    return (int)explode('.', $info['version']['number'])[0];
  }

  public function putMapping($indexName, $mappingName, $mapping, $dynamicTemplates = NULL, $wipeData = false) {
    if ($wipeData) {
      $this->deleteByQuery($indexName, array(
        'query' => array(
          'match_all' => array('boost' => 1)
        )
      ), $this->isLegacy() ? $mappingName : null);
    }

    $body = array(
      'properties' => $mapping
    );
    if($dynamicTemplates != NULL) {
      $body['dynamic_templates'] = $dynamicTemplates;
    }
    $this->getServerClient()->putMapping($indexName, $this->isLegacy ? $mappingName : null, $body);
  }

  function getMapping($indexName, $mappingName)
  {
    try {
      $mapping = $this->getServerClient()->getMapping($indexName,$this->isLegacy() ? $mappingName : null);
      if($this->isLegacy()) {
        if (isset($mapping[$indexName]['mappings'][$mappingName])) {
          return $mapping[$indexName]['mappings'][$mappingName];
        }
      }
      else {
        if (isset($mapping[$indexName]['mappings'])) {
          return $mapping[$indexName]['mappings'];
        }
      }
      return null;
    } catch (\Exception $ex) {
      return null;
    }
  }

  public function initStore($numberOfShards = 1, $numberOfReplicas = 1) {
    $indices = array_keys($this->getIndicesList());
    if(!in_array(static::APP_INDEX_NAME, $indices)) {
      $json = json_decode(file_get_contents(__DIR__ . '/../Resources/store_structure.json'), TRUE);
      $indexSettings = $json['index'];
      $indexSettings['number_of_shards'] = $numberOfShards;
      $indexSettings['number_of_replicas'] = $numberOfReplicas;
      $this->createIndex(static::APP_INDEX_NAME, $indexSettings);
    }
    $mapping = $this->getMapping(static::APP_INDEX_NAME, $this->isLegacy ? 'store_item' : null);
    if($mapping == null) {
      $json = json_decode(file_get_contents(__DIR__ . '/../Resources/store_structure.json'), TRUE);
      $this->putMapping(static::APP_INDEX_NAME, $this->isLegacy ? 'store_item' : null, $json['mapping']);
    }
  }

  public function search($indexName, $query, $from = 0, $size = 20, $type = null) {
    $this->sanitizeGlobalAgg($query);
    $query['from'] = $from;
    $query['size'] = $size;
    return $this->getServerClient()->search($indexName, $query, $type != null && $this->isLegacy() ? $type : null);
  }

  private function sanitizeGlobalAgg(&$array)
  { //Bug fix form empty queries in global aggregations
    if ($array != null) {
      foreach ($array as $k => $v) {
        if ($k == 'global' && empty($v) || $k == 'reverse_nested' && empty($v))
          $array[$k] = new \stdClass();
        elseif (is_array($v))
          $this->sanitizeGlobalAgg($array[$k]);
      }
    }
  }

  public function persistObject(PersistentObject $o) {
    $created = new \DateTime();
    $updated = new \DateTime();
    $o->setUpdated($updated);
    if($o->getId() == null) {
      $o->setCreated($created);
    }
    $params = array(
      'index' => static::APP_INDEX_NAME,
      'body' => array(
        'name' => $o->getName(),
        'type' => $o->getType(),
        'created_by' => $o->getCreatedBy(),
        'tags' => $o->getTags(),
        'data' => $o->serialize(),
        'updated' => $updated->format('Y-m-d\TH:i:s')
      )
    );
    if($this->isLegacy) {
      $params['type'] = 'store_item';
    }
    if($o->getId() != null) {
      $params['id'] = $o->getId();
      if($o->getCreated() == null) {
        $o->setCreated(new \DateTime());
      }
      $params['body']['created'] = $o->getCreated()->format('Y-m-d\TH:i:s');
    }
    else {
      $params['body']['created'] = $created->format('Y-m-d\TH:i:s');
    }
    $r = $this->getServerClient()->index($params['index'], $params['body'], $o->getId(), $this->isLegacy ? 'store_item' : null);
    if(isset($r['_id'])){
      $o->setId($r['_id']);
    }
    $this->getServerClient()->flush();
    $this->getServerClient()->refresh();
    return $o;
  }

  public function deleteObject($id) {
    $this->getServerClient()->delete(static::APP_INDEX_NAME, $id, $this->isLegacy() ? 'store_item' : null);
    $this->getServerClient()->flush();
    $this->getServerClient()->refresh();
  }

  /**
   * @param string $type
   * @param string $id
   * @return PersistentObject
   */
  public function findObject($type, $id) {
    $query = array(
      'query' => array(
        'bool' => array(
          'must' => array(
            array(
              'term' => array(
                'type' => $type
              )
            ),
            array(
              'ids' => array(
                'values' => array($id)
              )
            )
          )
        )
      )
    );
    $r = $this->search(static::APP_INDEX_NAME, $query);
    if(isset($r['hits']['hits'][0])) {
      $object = PersistentObject::unserialize($r['hits']['hits'][0]['_source']['data']);
      if($object instanceof PersistentObject) {
        $object->setId($r['hits']['hits'][0]['_id']);
        return $object;
      }
      else {
        return NULL;
      }
    }
    else {
      return null;
    }
  }

  /**
   * @param string $type
   * @param SecurityContext|NULL $context
   * @param int $from
   * @param int $size
   * @param string $order
   * @param array $criterias
   * @return PersistentObject[]
   */
  public function listObjects($type, SecurityContext $context = NULL, $from = 0, $size = 10000, $order = 'asc', $criterias = []) {
    $query = array(
      'query' => array(
        'bool' => array(
          'should' => array(
            array(
              'bool' => array(
                'must' => [
                  array(
                    'term' => array(
                      'type' => $type
                    )
                  )
                ]
              )
            )
          )
        )
      ),
      'size' => $size,
      'from' => $from,
      'sort' => array(
        'name.raw' => $order
      )
    );
    foreach($criterias as $criteriaName => $criteriaValue) {
      $query['query']['bool']['should'][0]['bool']['must'][] = array(
        'term' => array(
          $criteriaName => $criteriaValue
        )
      );
    }
    if($context != null && !$context->isAdmin()) {
      $restricted = array(
        'datasource' => 'datasources',
        'matching_list' => 'matchingLists'
      );
      foreach($restricted as $restrictionType => $restriction) {
        if ($type == $restrictionType) {
          $query['query']['bool']['should'][0]['bool']['must'][] = array(
            'ids' => array(
              'values' => $context->getRestrictions()[$restriction]
            )
          );
        }
      }
      if($type == 'processor') {
        $procQuery = array(
          'bool' => array(
            'must' => array(
              array(
                'bool' => array(
                  'should' => []
                )
              ),
              array(
                'bool' => array(
                  'should' => []
                )
              )
            )
          )
        );
        foreach($context->getRestrictions()['datasources'] as $procDs) {
          $procQuery['bool']['must'][0]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'datasource_id=' . $procDs
            )
          );
        }
        foreach($context->getRestrictions()['indexes'] as $procIndexes) {
          $procQuery['bool']['must'][1]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'index_name=' . $procIndexes
            )
          );
        }
        $query['query']['bool']['should'][0]['bool']['must'][] = $procQuery;
      }
      if($type == 'search_page') {
        $spQuery = array(
          'bool' => array(
            'must' => array(
              array(
                'bool' => array(
                  'should' => []
                )
              )
            )
          )
        );
        foreach($context->getRestrictions()['indexes'] as $spIndexes) {
          $spQuery['bool']['must'][0]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'index_name=' . $spIndexes
            )
          );
        }
        $query['query']['bool']['should'][0]['bool']['must'][] = $spQuery;
      }
      if($type == 'boost_query') {
        $bqQuery = array(
          'bool' => array(
            'must' => array(
              array(
                'bool' => array(
                  'should' => []
                )
              )
            )
          )
        );
        foreach($context->getRestrictions()['indexes'] as $bqIndexes) {
          $bqQuery['bool']['must'][0]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'index_name=' . $bqIndexes
            )
          );
        }
        $query['query']['bool']['should'][0]['bool']['must'][] = $bqQuery;
      }
      if($type == 'saved_query') {
        $sqQuery = array(
          'bool' => array(
            'must' => array(
              array(
                'bool' => array(
                  'should' => []
                )
              )
            )
          )
        );
        foreach($context->getRestrictions()['indexes'] as $sqIndexes) {
          $sqQuery['bool']['must'][0]['bool']['should'][] = array(
            'term' => array(
              'tags' => 'index_name=' . $sqIndexes
            )
          );
        }
        $query['query']['bool']['should'][0]['bool']['must'][] = $sqQuery;
      }
      $query['query']['bool']['should'][] = array(
        'bool' => array(
          'must' => array(
            array(
              'term' => array(
                'created_by' => $context->getUserUid()
              )
            ),
            array(
              'term' => array(
                'type' => $type
              )
            )
          )
        )
      );
    }
    $r = $this->search(static::APP_INDEX_NAME, $query, $from, $size);
    $objects = [];
    foreach($r['hits']['hits'] as $hit) {
      $object = PersistentObject::unserialize($hit['_source']['data']);
      if($object instanceof PersistentObject) {
        $object->setId($hit['_id']);
        $objects[] = $object;
      }
    }
    return $objects;
  }

  public function deleteByQuery($indexName, $query, $mappingName = null)
  {
    if($this->getServerMajorVersionNumber() >= 5) {
      $this->getServerClient()->deleteByQuery($indexName, $query, $mappingName);
    }
    else{
      //Delete by query is not available on ES 2.x clusters so let's do it on our own
      $this->scroll($query, $indexName, $this->isLegacy() ? $mappingName : null, function($items){
        $this->bulkDelete($items);
      },500);
    }
  }

  public function scroll($queryBody, $index, $mapping, $callback, $size = 10, $context = null)
  {
    $queryBody['size'] = $size;
    $r = $this->getServerClient()->search($index, $queryBody, $this->isLegacy() ? $mapping : null, '10ms');
    if (isset($r['_scroll_id'])) {
      while (count($r['hits']['hits']) > 0) {
        $callback($r['hits']['hits'], $context);
        $scrollId = $r['_scroll_id'];
        $r = $this->getServerClient()->scroll($scrollId, '1m');
      }
    }
  }

  public function bulkDelete($items)
  {
    $bulkString = '';
    foreach ($items as $item) {
      $def = ['_index' => $item['_index'], '_id' => $item['_id']];
      if($this->isLegacy()) {
        $def['_type'] = $item['_type'];
      }
      $data = array('delete' => $def);
      $bulkString .= json_encode($data) . "\n";
    }
    if (count($items) > 0) {
      $this->getServerClient()->bulk($bulkString);
    }
  }

  public function getAutopromoteIndexName($fromIndex) {
    return '.ads_autopromote_' . str_replace('.', '', $fromIndex);
  }

  public function getAutopromoteAnalyzer($fromIndex) {
    $mapping = $this->getMapping($this->getAutopromoteIndexName($fromIndex), 'autopromote');
    return $mapping['properties']['keywords']['analyzer'];
  }

  public function createAutopromoteIndex($fromIndex, $analyzer) {
    $index = $this->getIndex($fromIndex);
    $indexName = $this->getAutopromoteIndexName($fromIndex);
    $this->createIndex('.ads_autopromote_' . str_replace('.', '', $fromIndex), array(
      'analysis' => $index[$fromIndex]['settings']['index']['analysis']
    ));
    $json = json_decode(file_get_contents(__DIR__ . '/../Resources/autopromote_structure.json'), TRUE);
    $json['mapping']['keywords']['analyzer'] = $analyzer;
    $this->putMapping($indexName, 'autopromote', $json['mapping']);
  }

  public function saveAutopromote(Autopromote $autopromote) {
    $doc = array(
      'name' => $autopromote->getName(),
      'keywords' => $autopromote->getKeywords(),
      'data' => serialize($autopromote)
    );
    $r = $this->getServerClient()->index($this->getAutopromoteIndexName($autopromote->getIndex()), $doc, $autopromote->getId(), $this->isLegacy() ? 'autopromote' : null);
    if(isset($r['_id'])){
      $autopromote->setId($r['_id']);
    }
    $this->getServerClient()->flush();
    $this->getServerClient()->refresh();
  }

  public function listAutopromotes(SecurityContext $securityContext = null) {
    $query = [
      'query' => array(
        'match_all' => array(
          'boost' => 1
        )
      ),
      'from' => 0,
      'size' => 10000,
    ];
    if($securityContext == null || $securityContext->isAdmin()) {
      $indexName = '.ads_autopromote_*';
    }
    else {
      $indexRestrictions = $securityContext->getRestrictions()['indexes'];
      if(empty($indexRestrictions)) {
        return [];
      }
      else {
        $restrictedIndexes = [];
        foreach($indexRestrictions as $ir) {
          if($this->getIndex($this->getAutopromoteIndexName($ir)) != null)
            $restrictedIndexes[] = $this->getAutopromoteIndexName($ir);
        }
        if(empty($restrictedIndexes))
          return [];
        $indexName = implode(',', $restrictedIndexes);
      }
    }
    $r = $this->getServerClient()->search($indexName, $query, $this->isLegacy() ? 'autopromote' : null);
    $autopromotes = [];
    foreach($r['hits']['hits'] as $hit) {
      $autopromote = unserialize($hit['_source']['data']);
      $autopromote->setId($hit['_id']);
      $autopromotes[] = $autopromote;
    }
    return $autopromotes;
  }

  public function getAutopromote($id, $index) {
    $query = [
      'query' => array(
        'ids' => array(
          'values' => [$id]
        )
      ),
      'from' => 0,
      'size' => 1,
    ];
    $indexName = $this->getAutopromoteIndexName($index);
    $r = $this->getServerClient()->search($indexName, $query, $this->isLegacy() ? 'autopromote' : null);
    if(isset($r['hits']['hits'][0])) {
      $autopromote = unserialize($r['hits']['hits'][0]['_source']['data']);
      $autopromote->setId($r['hits']['hits'][0]['_id']);
      return $autopromote;
    }
    return NULL;
  }

  public function deleteAutopromote($id, $index) {
    $this->getServerClient()->delete($this->getAutopromoteIndexName($index), $id, $this->isLegacy() ? 'autopromote' : null);
    $this->getServerClient()->flush();
    $this->getServerClient()->refresh();
  }

  public function bulkIndex($items)
  {
    $bulkString = '';
    foreach ($items as $item) {
      $data = array('index' => array('_index' => $item['indexName'], '_type' => $item['mappingName']));
      if(!$this->isLegacy()) {
        unset($data['index']['_type']);
      }
      if (isset($item['body']['_id'])) {
        $data['index']['_id'] = $item['body']['_id'];
        unset($item['body']['_id']);
      }
      $bulkString .= json_encode($data) . "\n";
      $bulkString .= json_encode($item['body']) . "\n";
    }
    if (count($items) > 0) {

      $tries = 0;
      $retry = true;
      while ($tries == 0 || $retry) {
        try {
          $this->getServerClient()->bulk($bulkString);
          $retry = false;
        } catch (ServerClientException $ex) {
          print get_class($this) . ' >> NoNodesAvailableException has been caught (' . $ex->getMessage() . ')' . PHP_EOL;
          if ($tries > 20) {
            $retry = false;
            print get_class($this) . ' >> This is over, I choose to die.' . PHP_EOL;
            return; //Kill the datasource
          } else {
            print get_class($this) . ' >> Retrying in 1 second...' . PHP_EOL;
            sleep(1); //Sleep for 1 second
          }
        } finally {
          $tries++;
        }
      }
    }
    $this->getServerClient()->refresh();
  }

  public function indexDocument($indexName, $mappingName, $document, $flush = true)
  {
    $id = null;
    if (isset($document['_id'])) {
      $id = $document['_id'];
      unset($document['_id']);
    }
    if(empty($document)) {
      return null;
    }
    $tries = 0;
    $retry = true;
    while ($tries == 0 || $retry) {
      try {
        $r = $this->getServerClient()->index($indexName, $document, $id, $this->isLegacy() ? $mappingName : null);
        if ($flush) {
          $this->getServerClient()->flush();
        }
        $retry = false;
      } catch (ServerClientException $ex) {
        print get_class($this) . ' >> ServerClientException has been caught (' . $ex->getMessage() . ')' . PHP_EOL;
        if ($tries > 20) {
          $retry = false;
          print get_class($this) . ' >> This is over, I choose to die.' . PHP_EOL;
          return null; //Kill the datasource
        } else {
          print get_class($this) . ' >> Retrying in 1 second...' . PHP_EOL;
          sleep(1); //Sleep for 1 second
        }
      } finally {
        $tries++;
      }
    }
    return isset($r) ? $r : null;
  }

  public function flush() {
    $this->getServerClient()->flush();
  }

  public function analyze($index, $analyzer, $text) {
    return $this->getServerClient()->analyze($index, $analyzer, $text);
  }
}