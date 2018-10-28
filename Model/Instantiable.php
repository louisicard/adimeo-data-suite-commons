<?php
/**
 * Created by PhpStorm.
 * User: louis
 * Date: 28/10/2018
 * Time: 17:14
 */

namespace AdimeoDataSuite\Bundle\CommonsBundle\Model;


interface Instantiable
{

  /**
   * @param array $settings
   * @return Instantiable
   */
  static function instantiate($settings);

}