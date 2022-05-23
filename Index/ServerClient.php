<?php

namespace AdimeoDataSuite\Index;


use AdimeoDataSuite\Exception\ServerClientException;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\ClientException;

class ServerClient
{

  /**
   * @var string
   */
  private $serverUrl;

  /**
   * @var Client
   */
  private $client;

  /**
   * @var bool
   */
  private $isLegacy;

  /**
   * @var bool
   */
  private $isSecurityEnabled;

  /**
   * @var string
   */
  private $caCertPath;

  /**
   * @var string
   */
  private $username;

  /**
   * @var string
   */
  private $password;

  public function __construct($serverUrl, $isLegacy, $isSecurityEnabled, $caCertPath, $username, $password)
  {

    $this->serverUrl = $serverUrl;
    $this->client = new Client();
    $this->isLegacy = $isLegacy;
    $this->isSecurityEnabled = $isSecurityEnabled;
    $this->caCertPath = $caCertPath;
    $this->username = $username;
    $this->password = $password;
  }

  public function isLegacy() {
    return $this->isLegacy;
  }

  private function request($method, $uri, $params = [], $body = null, $bodyAsString = false) {

    try {
      $reqParams = [];
      if($body != null) {
        $reqParams['body'] = $bodyAsString ? $body : json_encode($body);
        $reqParams['headers']['Content-type'] = 'application/json';
      }
      if(!empty($params)) {
        $qsParts = [];
        foreach($params as $k => $v) {
          $qsParts[]= $k . '=' . urlencode($v);
        }
        $querystring = '?' . implode('&', $qsParts);
      }
      else {
        $querystring = '';
      }
      if($this->isSecurityEnabled) {
        $reqParams['verify'] = $this->caCertPath;
        $reqParams['headers']['Authorization'] = 'Basic ' . base64_encode($this->username . ':' . $this->password);
      }
      $res = $this->client->request($method, $this->serverUrl . $uri . $querystring, $reqParams);
    }
    catch(\Exception $ex) {
      /** @var ClientException $ex */
      //var_dump((string)$ex->getResponse()->getBody());
      throw new ServerClientException($ex->getMessage(), $ex->getResponse()->getStatusCode());
    }
    if($res->getStatusCode() >= 200 && $res->getStatusCode() < 300) {
      $json = (string)$res->getBody();
      $data = json_decode($json, true);
      if($data !== null) {
        return $data;
      }
      else {
        throw new ServerClientException('No valid data returned from server', $res->getStatusCode());
      }
    }
    throw new ServerClientException('Server responded with status code ' . $res->getStatusCode(), $res->getStatusCode());
  }

  public function info() {
    return $this->request('GET', '/');
  }

  public function clusterHealth() {
    return $this->request('GET', '/_cluster/health');
  }

  public function clusterStats() {
    return $this->request('GET', '/_stats');
  }

  public function indicesMappings() {
    return $this->request('GET', '/_mapping');
  }

  public function indicesSettings() {
    return $this->request('GET', '/_settings');
  }

  public function mapping($indexName = '_all') {
    return $this->request('GET', '/' . $indexName . '/_mapping');
  }

  public function indexSettings($indexName = '_all') {
    return $this->request('GET', '/' . $indexName . '/_settings');
  }

  public function createIndex($indexName, $settings = [], $maxReplicas = 0) {
    if(!isset($settings['number_of_replicas']) || $settings['number_of_replicas'] > $maxReplicas) {
      $settings['number_of_replicas'] = $maxReplicas;
    }
    return $this->request('PUT', '/' . $indexName, [], ['settings' => $settings]);
  }

  public function updateIndex($indexName, $settings = []) {
    return $this->request('PUT', '/' . $indexName . '/_settings', [], ['index' => $settings]);
  }

  public function closeIndex($indexName) {
    return $this->request('POST', '/' . $indexName . '/_close');
  }

  public function deleteIndex($indexName) {
    return $this->request('DELETE', '/' . $indexName);
  }

  public function openIndex($indexName) {
    return $this->request('POST', '/' . $indexName . '/_open');
  }

  public function putMapping($indexName, $mappingName = null, $definition = []) {
    return $this->request('PUT', '/' . $indexName . '/_mapping' . ($mappingName != null ? '/' . $mappingName : ''), [], $definition);
  }

  public function getMapping($indexName, $mappingName = null) {
    return $this->request('GET', '/' . $indexName . '/_mapping' . ($mappingName != null ? '/' . $mappingName : ''));
  }

  public function index($indexName, $document, $id = null, $mappingname = null) {
    $uri = '/' . $indexName;
    if($mappingname != null) {
      $uri .= '/' . $mappingname;
    }
    else {
      $uri .= '/_doc';
    }
    if($id != null) {
      $method = 'PUT';
      $uri .= '/' . $id;
    }
    else {
      $method = 'POST';
    }
    return $this->request($method, $uri, [], $document);
  }

  public function deleteByQuery($indexName, $queryBody, $mappingName = null) {
    return $this->request('POST', '/' . $indexName . ($mappingName != null ? '/' . $mappingName : '') . '/_delete_by_query', [], $queryBody);
  }

  public function search($indexName, $queryBody, $mappingName = null, $scroll = null) {
    if($this->isLegacy()) {
      $params = [];
    }
    else {
      $params = ['rest_total_hits_as_int' => 'true'];
    }
    if($scroll != null) {
      $params['scroll'] = $scroll;
    }
    return $this->request('GET', '/' . $indexName . ($mappingName != null ? '/' . $mappingName : '') . '/_search', $params, $queryBody);
  }

  public function scroll($scrollId, $scroll) {
    return $this->request('GET', '/_search/scroll', [], [
      'scroll_id' => $scrollId,
      'scroll' => $scroll,
    ]);
  }

  public function flush($indexName = null) {
    return $this->request('POST', ($indexName != null ? '/' . $indexName : '') . '/_flush');
  }

  public function refresh($indexName = null) {
    return $this->request('POST', ($indexName != null ? '/' . $indexName : '') . '/_refresh');
  }

  public function delete($indexName, $docId, $mappingName = null) {
    return $this->request('DELETE', '/' . $indexName . '/' . ($mappingName == null ? '_doc' : $mappingName) . '/' . $docId);
  }

  public function bulk($bulkString) {
    return $this->request('POST', '/_bulk', [], $bulkString, true);
  }

  public function analyze($indexName, $analyzer, $text) {
    return $this->request('GET', '/' . $indexName . '/_analyze', [], [
      'analyzer' => $analyzer,
      'text' => $text
    ]);
  }

  public function getRepository($name = '_all') {
    return $this->request('GET', '/_snapshot/' . $name);
  }

  public function createRepository($name, $type, $location, $compressed) {
    $params = [
      'type' => $type,
      'settings' => [
        'location' => $location,
        'compress' => $compressed
      ]
    ];
    return $this->request('PUT', '/_snapshot/' . $name, [], $params);
  }

  public function deleteRepository($name) {
    return $this->request('DELETE', '/_snapshot/' . $name);
  }

  public function getSnapshots($repositoryName, $snapshot = '_all') {
    return $this->request('GET', '/_snapshot/' . $repositoryName . '/' . $snapshot);
  }

  public function createSnapshot($repositoryName, $name, $indices, $ignoreUnavailable, $includeGlobalState) {
    $params = [
      'indices' => $indices,
      'ignore_unavailable' => $ignoreUnavailable,
      'include_global_state' => $includeGlobalState
    ];
    return $this->request('PUT', '/_snapshot/' . $repositoryName . '/' . $name, [], $params);
  }

  public function deleteSnapshot($repositoryName, $name) {
    return $this->request('DELETE', '/_snapshot/' . $repositoryName . '/' . $name);
  }

  public function restoreSnapshot($repositoryName, $name, $settings) {
    return $this->request('POST', '/_snapshot/' . $repositoryName . '/' . $name . '/_restore', [], $settings);
  }

}