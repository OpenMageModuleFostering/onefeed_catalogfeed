<?php
class Onefeed_Catalogfeed_Helper_Data extends Mage_Core_Helper_Abstract
{
	public function isActive($store_id=null)
    {
        return Mage::getStoreConfig("onefeed_config/onefeed_general/enabled", $store_id);
    }
}
