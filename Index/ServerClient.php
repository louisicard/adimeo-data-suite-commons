<?php

namespace AdimeoDataSuite\Index;


use AdimeoDataSuite\Exception\ServerClientException;
use GuzzleHttp\Client;

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

  public function __construct($serverUrl)
  {

    $this->serverUrl = $serverUrl;
    $this->client = new Client();

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
      $res = $this->client->request($method, $this->serverUrl . $uri . $querystring, $reqParams);
    }
    catch(\Exception $ex) {
      throw new ServerClientException($ex->getMessage());
    }
    if($res->getStatusCode() >= 200 && $res->getStatusCode() < 300) {
      $json = (string)$res->getBody();
      $data = json_decode($json, true);
      if($data !== null) {
        return $data;
      }
      else {
        throw new ServerClientException('No valid data returned from server');
      }
    }
    throw new ServerClientException('Server responded with status code ' . $res->getStatusCode());
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

  public function createIndex($indexName, $settings = []) {
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
    $params = ['rest_total_hits_as_int' => 'true'];
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

}