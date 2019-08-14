<?php
class Onefeed_Catalogfeed_Adminhtml_CatalogfeedbackendController extends Mage_Adminhtml_Controller_Action
{
	public function indexAction()
    {
      $lockFile = Mage::getBaseDir('var') . DS . 'onefeed.lock';
      unlink($lockFile);
      $obj =  new Onefeed_Catalogfeed_Model_Cron();
      $obj->forceGenerateFeed();
      echo "start";die();
    }
    public function checkAction(){
    	$lockFile = Mage::getBaseDir('var') . DS . 'onefeed.lock';
    	$lockVar = unserialize(file_get_contents($lockFile));
    	if($lockVar['status']=='done' && $lockVar['date']== Mage::getModel('core/date')->date('Y-m-d'))
      	{
      		echo "1";
			Mage::getSingleton('adminhtml/session')->addSuccess('Feed generated successfully');
	  	} else {
	  		echo "0";
			Mage::getSingleton('adminhtml/session')->addError('An error occurred.');
	  	}
		die();
    }
}
