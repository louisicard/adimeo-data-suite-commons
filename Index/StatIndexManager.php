<?php

namespace AdimeoDataSuite\Index;

use AdimeoDataSuite\Exception\ServerClientException;

class StatIndexManager
{

  const APP_INDEX_NAME = '.adimeo_data_suite_stat';

  private $indexNumberOfShards;

  private $indexNumberOfReplicas;

  /**
   * @var ServerClient
   */
  private $serverClient;

  /**
   * @var bool
   */
  private $isLegacy;

  public function __construct($elasticsearchServerUrl, $numberOfShards = 1, $numberOfReplicas = 1, $isLegacy = false) {

    $this->indexNumberOfShards = $numberOfShards;
    $this->indexNumberOfReplicas = $numberOfReplicas;

    $this->serverClient = new ServerClient($elasticsearchServerUrl);
    $this->isLegacy = $isLegacy;
  }

  /**
   * @return ServerClient
   */
  public function getServerClient() {
    return $this->serverClient;
  }

  public function isLegacy() {
    return $this->isLegacy;
  }

  public function saveStat($target, $facets = array(), $text, $keywords = [], $rawKeyWords = [], $apiUrl = '', $resultCount = 0, $responseTime = 0, $remoteAddress = '', $tag = '', $hits = [])
  {
    try {
      $query = array(
        'query' => array(
          'match_all' => array(
            'boost' => 1
          )
        )
      );
      $this->getServerClient()->search(static::APP_INDEX_NAME, $query, $this->isLegacy() ? 'stat' : NULL);
    }
    catch(ServerClientException $ex) {
      if($ex->getStatusCode() == 404) {
        //stat index does not exist
        $settings = [
          'number_of_shards' => $this->indexNumberOfShards,
          'number_of_replicas' => $this->indexNumberOfReplicas,
        ];
        $this->getServerClient()->createIndex(static::APP_INDEX_NAME, $settings);
        $json = json_decode(file_get_contents(__DIR__ . '/../Resources/stat_structure.json'), TRUE);
        $this->getServerClient()->putMapping(static::APP_INDEX_NAME, $this->isLegacy() ? 'stat' : null, ['properties' => $json]);
      }
    }
    $indexName = strpos($target, '.') === 0 ? ('.' . explode('.', $target)[1]) : explode('.', $target)[0];
    $document = array(
      'date' => date('Y-m-d\TH:i:s'),
      'index' => $indexName,
      'mapping' => $this->isLegacy() ? $target : $indexName . '._doc',
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
    );
    $r = $this->getServerClient()->index(static::APP_INDEX_NAME, $document, null, $this->isLegacy() ? 'stat' : NULL);
    $this->getServerClient()->flush();
    unset($document);
    return $r;
  }

  public function search($indexName, $query, $from = 0, $size = 20, $type = null) {
    $query['from'] = $from;
    $query['size'] = $size;
    return $this->getServerClient()->search($indexName, $query, $this->isLegacy() ? $type : null);
  }

}