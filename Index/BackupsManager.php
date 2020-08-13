<?php

namespace AdimeoDataSuite\Index;


class BackupsManager
{

  /**
   * @var ServerClient
   */
  private $serverClient;

  /**
   * BackupManager constructor.
   *
   * @param $elasticsearchServerUrl
   */
  public function __construct($elasticsearchServerUrl)
  {
    $this->serverClient = new ServerClient($elasticsearchServerUrl);
  }

  /**
   * @return ServerClient
   */
  public function getServerClient() {
    return $this->serverClient;
  }

  /**
   * Get all repositories
   *
   * @return array
   */
  public function getBackupsRepositories()
  {
    return $this->getServerClient()->getRepository();
  }

  /**
   * Get a repository
   *
   * @param $repositoryName
   * @return array
   */
  public function getRepository($repositoryName)
  {
    return $this->getServerClient()->getRepository($repositoryName);
  }

  /**
   * Create a repository
   *
   * @param $data
   */
  public function createRepository($data)
  {
    $this->getServerClient()->createRepository(preg_replace("/[^A-Za-z0-9]/", '_', strtolower($data['name'])), $data['type'], $data['location'], $data['compress']);
  }

  /**
   * Delete a repository
   *
   * @param $name
   * @return array
   */
  public function deleteRepository($name)
  {
    return $this->getServerClient()->deleteRepository($name);
  }

  /**
   * Get all snapshots
   *
   * @param $repoName
   * @return array
   */
  public function getSnapshots($repoName)
  {
    return $this->getServerClient()->getSnapshots($repoName);
  }

  /**
   * Get a snapshot
   *
   * @param $repositoryName
   * @param $snapshotName
   * @return string|null
   */
  public function getSnapshot($repositoryName, $snapshotName)
  {
    $repository = $this->getServerClient()->getSnapshots($repositoryName, $snapshotName);

    return (isset($repository['snapshots'][0])) ? $repository['snapshots'][0] : null;
  }

  /**
   * Create a snapshot
   *
   * @param $repositoryName
   * @param $snapshotName
   * @param $indexes
   * @param bool $ignoreUnavailable
   * @param bool $includeGlobalState
   */
  public function createSnapshot($repositoryName, $snapshotName, $indexes, $ignoreUnavailable = true, $includeGlobalState = false)
  {
    $this->getServerClient()->createSnapshot($repositoryName, preg_replace("/[^A-Za-z0-9]/", '_', strtolower($snapshotName)), implode(',', $indexes), $ignoreUnavailable, $includeGlobalState);
  }

  /**
   * Delete a snapshot
   *
   * @param $repositoryName
   * @param $snapshotName
   * @return array
   */
  public function deleteSnapshot($repositoryName, $snapshotName)
  {
    return $this->getServerClient()->deleteSnapshot($repositoryName, $snapshotName);
  }

  /**
   * Restore a snapshot
   *
   * @param $repositoryName
   * @param $snapshotName
   * @param $params
   */
  public function restoreSnapshot($repositoryName, $snapshotName, $params)
  {
    $body = array();
    if (isset($params['indexes']) && !empty($params['indexes'])) {
      $body['indices'] = $params['indexes'];
    }
    if (isset($params['ignoreUnavailable'])) {
      $body['ignore_unavailable'] = $params['ignoreUnavailable'];
    }
    if (isset($params['includeGlobalState'])) {
      $body['include_global_state'] = $params['includeGlobalState'];
    }
    if (isset($params['renamePattern']) && !empty($params['renamePattern']) && $params['renamePattern'] != null) {
      $body['rename_pattern'] = $params['renamePattern'];
    }
    if (isset($params['renameReplacement']) && !empty($params['renameReplacement']) && $params['renameReplacement'] != null) {
      $body['rename_replacement'] = $params['renameReplacement'];
    }
    $this->getServerClient()->restoreSnapshot($repositoryName, $snapshotName, $body);
  }
}