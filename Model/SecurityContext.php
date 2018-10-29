<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 29/10/2018
 * Time: 11:35
 */

namespace AdimeoDataSuite\Model;


class SecurityContext
{

  private $userUid;
  private $restrictions;

  /**
   * @return mixed
   */
  public function getUserUid()
  {
    return $this->userUid;
  }

  /**
   * @param mixed $userUid
   */
  public function setUserUid($userUid)
  {
    $this->userUid = $userUid;
  }

  /**
   * @return mixed
   */
  public function getRestrictions()
  {
    return $this->restrictions;
  }

  /**
   * @param mixed $restrictions
   */
  public function setRestrictions($restrictions)
  {
    $this->restrictions = $restrictions;
  }


}