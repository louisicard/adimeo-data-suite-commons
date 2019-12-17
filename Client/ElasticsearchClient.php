<?php

namespace AdimeoDataSuite\Client;

use GuzzleHttp\Client;

class ElasticsearchClient
{

  private $elasticsearchServerUrl;

  /**
   * @var Client
   */
  private $client;

  private $stats = null;
  private $structure = null;

  public function __construct($elasticsearchServerUrl)
  {
    $this->elasticsearchServerUrl = $elasticsearchServerUrl;
    $this->client = new Client();
  }

  private function callAPI($uri, $options = []) {
    $r = $this->client->request('GET', $this->elasticsearchServerUrl . $uri, $options);
    $body = (string)$r->getBody();
    return json_decode($body, true);
  }

  public function getStats() {
    if($this->stats == null) {
      $this->stats = $this->callAPI('/_stats');
    }
    return $this->stats;
  }

  public function getIndices() {
    return $this->getStats()['indices'];
  }

  public function getStructure() {
    if($this->structure == null) {
      $this->structure = $this->callAPI('/_all');
    }
    return $this->structure;
  }

  public function getMapping($indexName, $mappingName) {
    $mappings = $this->callAPI('/' . $indexName)[$indexName]['mappings'];
    if(isset($mappings[$mappingName])) {
      return $mappings[$mappingName];
    }
    return null;
  }

  public function search($indexName, $body, $type = null) {
    $uri = '/' . $indexName . ($type != null ? '/' . $type : '') . '/_search';
    $options = [
      'body' => json_encode($body)
    ];
    return $this->callAPI($uri, $options);
  }

  public function getServerInfo() {
    return $this->callAPI('/');
  }

  public function getClusterHealth() {
    return $this->callAPI('/_cluster/health');
  }

}