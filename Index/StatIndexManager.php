<?php

namespace AdimeoDataSuite\Index;

use AdimeoDataSuite\Client\ElasticsearchClient;

class StatIndexManager
{

  const APP_INDEX_NAME = '.adimeo_data_suite_stat';

  /**
   * @var ElasticsearchClient
   */
  private $client;

  private $indexNumberOfShards;

  private $indexNumberOfReplicas;

  public function __construct($elasticsearchServerUrl, $numberOfShards = 1, $numberOfReplicas = 1) {
    $this->client = new ElasticsearchClient($elasticsearchServerUrl);

    $this->indexNumberOfShards = $numberOfShards;
    $this->indexNumberOfReplicas = $numberOfReplicas;
  }

  /**
   * @return ElasticsearchClient
   */
  public function getClient() {
    return $this->client;
  }

  public function saveStat($target, $facets = array(), $text, $keywords = [], $rawKeyWords = [], $apiUrl = '', $resultCount = 0, $responseTime = 0, $remoteAddress = '', $tag = '', $hits = [])
  {
    try {
      $this->getClient()->search(
        static::APP_INDEX_NAME,
        array(
          'query' => array(
            'match_all' => array(
              'boost' => 1
            )
          )
        ),
        'stat'
      );
    }
    catch(\Exception $ex) {
      //stat index does not exist
      $this->client->createIndex(static::APP_INDEX_NAME, [
        'number_of_shards' => $this->indexNumberOfShards,
        'number_of_replicas' => $this->indexNumberOfReplicas,
      ]);
      $json = json_decode(file_get_contents(__DIR__ . '/../Resources/stat_structure.json'), TRUE);
      $this->putMapping(static::APP_INDEX_NAME, 'stat', $json);
    }
    $indexName = strpos($target, '.') === 0 ? ('.' . explode('.', $target)[1]) : explode('.', $target)[0];
    $r = $this->getClient()->index(static::APP_INDEX_NAME, 'stat', array(
      'date' => date('Y-m-d\TH:i:s'),
      'index' => $indexName,
      'mapping' => $target,
      'remote_addr' => $remoteAddress,
      'log' => $tag,
      'facets' => $facets,
      'keywords' => $keywords,
      'keywords_raw' => $rawKeyWords,
      'api_url' => $apiUrl,
      'result_count' => $resultCount,
      'response_time' => $responseTime,
      'text' => $text,
      'hits' => $hits
    ));
    $this->getClient()->flush();
    unset($params);
    return $r;
  }

  private function putMapping($indexName, $mappingName, $mapping) {
    $this->client->putMapping($indexName, $mappingName, $mapping);
  }

  public function search($indexName, $query, $from = 0, $size = 20, $type = null) {
    $body = $query;
    $body['from'] = $from;
    $body['size'] = $size;
    return $this->client->search($indexName, $body);
  }

}