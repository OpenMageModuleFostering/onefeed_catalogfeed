<?php
class Onefeed_Catalogfeed_Model_Cron{

	private $_tablePrefix;
	private $_storeId;
	private $_websiteId;
	private $_mediaBaseUrl;
	private $_webBaseUrl;
	private $_mediaBasePath;
	private $_dbi;
	private $IncludeDisabled;
	private $ExcludeOutOfStock;
	private $DownloadAsAttachment;
	private $_FtpHostName;
	private $_FtpUserName;
	private $_FtpPassword;
	private $_selectLimit;
	public  $_lockFile;
	public  $_delimiter;


	public function __construct(){
		// Get the table prefix
		$tableName 				= Mage::getSingleton('core/resource')->getTableName('core_website');
		$this->_tablePrefix 	= substr($tableName, 0, strpos($tableName, 'core_website'));
		$websites 				= Mage::app()->getWebsites();
		$this->_websiteId 		= $websites[1]->getId();
		$this->_storeId 		= $websites[1]->getDefaultStore()->getId();
		$this->_mediaBaseUrl 	= Mage::getBaseUrl(Mage_Core_Model_Store::URL_TYPE_MEDIA);
		$this->_webBaseUrl 		= Mage::getBaseUrl();
		$this->_dbi 			= Mage::getSingleton('core/resource') ->getConnection('core_read');
		$this->_mediaBasePath 	= Mage::getBaseDir('media');
		// check if onefeed dir exist
		if(!file_exists($this->_mediaBasePath . DS . 'onefeed')){
			mkdir($this->_mediaBasePath . DS .'onefeed' , 0777 , true);
			chmod($this->_mediaBasePath . DS .'onefeed' , 0777);
		}
		$this->ExcludeOutOfStock = (Mage::getStoreConfig('onefeed_config/onefeed_defaults/excludeoutofstock') == '1') ? true : false;
		$this->IncludeDisabled = (Mage::getStoreConfig('onefeed_config/onefeed_defaults/includedisabled') == '1') ? true : false;

		// SET FTP Credentials
		$this->_FtpHostName = 'ftp.onefeed.co.uk';
		$this->_FtpUserName = Mage::getStoreConfig('onefeed_config/onefeed_ftp/username');
		$this->_FtpPassword = Mage::getStoreConfig('onefeed_config/onefeed_ftp/password');

		// set default select limit to 500
		$this->_selectLimit = 5000;
		if($this->getFreeMemory() > 500)
		{
			$this->_selectLimit = 5000+(int)(($this->getFreeMemory()-500));
		}
		$this->_lockFile = Mage::getBaseDir('var') . DS . 'onefeed.lock';
		$this->_delimiter= Mage::getStoreConfig('onefeed_config/onefeed_defaults/delimeter');
	}

	public function generateFeed(){
		// Run extraction
		if(Mage::helper('catalogfeed')->isActive()){
			return $this->_extractFromMySQL();
		}else{
			return false;
		}
	}


	public function forceGenerateFeed(){
		// Run extraction
		return $this->_extractFromMySQL();
	}
	// Apply prefix to table names in the query
	private function _applyTablePrefix($query)
	{
		return str_replace('PFX_', $this->_tablePrefix, $query);
	}

	// Extract natively directly from the database
	private function _extractFromMySQL()
	{
		// set execution time to 3 mins
		ini_set('max_execution_time', 100);
		$exportContinue = false;
		// Set up a file name
		if(Mage::getStoreConfig('onefeed_config/onefeed_defaults/filename')!=''){
			$FileName = sprintf('%s.csv',Mage::getStoreConfig('onefeed_config/onefeed_defaults/filename'));
		} else {
			$FileName = sprintf('%s_%d_%d.csv', date('Y-m-d'),  $this->_websiteId, $this->_storeId);
		}
		//read lock file and manage accordingly
		$lockVar = unserialize(file_get_contents($this->_lockFile));
		// check if lock variable exist
		if($lockVar){
			// check if today export
			if($lockVar['date'] == Mage::getModel('core/date')->date('Y-m-d'))
			{
				// check if export for today is done
				if($lockVar['status'] == 'done'){
					Mage::log('Export for '.$lockVar['date'].' is done' , null , 'onefeed.log');
					return false;
				}
				// if last time is < 1 min than do not run
				if($lockVar['timestamp'] > strtotime("-1 min") && $lockVar['status'] == 'continue')
				{
					Mage::log('Export for '.$lockVar['date'].' is running' , null , 'onefeed.log');
					Mage::log('Current Datetime: '.date("Y-m-d H:i:s").' and Last run on :'.date("Y-m-d H:i:s",$lockVar['timestamp']) , null , 'onefeed.log');
					return false;
				}
				else{
					$exportContinue = true;
					// file handler start in append mode
					$fp = fopen($this->_mediaBasePath . DS .'onefeed'.DS.$FileName, 'a+');

				}
			}
			else
			{
				// start fresh export
				// file handler start in fresh write mode
				$fp = fopen($this->_mediaBasePath . DS .'onefeed'.DS.$FileName, 'w');
			}
		}
		else
		{
			$lockVar['date'] = Mage::getModel('core/date')->date('Y-m-d');
			$lockVar['timestamp'] = time();
			$lockVar['status'] = 'continue';
			$lockVar['exported_entity_id'] = 0;
			file_put_contents($this->_lockFile , serialize($lockVar));
			$fp = fopen($this->_mediaBasePath . DS .'onefeed'.DS.$FileName, 'w');

		}

		if(empty($fp))
		{
			$fp = fopen($this->_mediaBasePath . DS .'onefeed'.DS.$FileName, 'w');
		}
		// Start sending file
		// Check if Amasty Product Labels table exists
		$query = "SHOW TABLES LIKE 'PFX_am_label'";
		$query = $this->_applyTablePrefix($query);
		$AmastyProductLabelsTableExists = $this->_dbi->fetchOne($query);
		$AmastyProductLabelsTableExists = !empty($AmastyProductLabelsTableExists);

		// Create a lookup table for the SKU to label_id
		$AmastyProductLabelsLookupTable = array();
		if($AmastyProductLabelsTableExists == true)
		{
			// NOTE: Only fetch simple labels and ignore all matching rules.
			//   include_type=0 means "all matching SKUs and listed SKUs"
			//   include_type=1 means "all matching SKUs EXCEPT listed SKUs"
			//   include_type=2 means "listed SKUs only"
			$query = "SELECT label_id, name, include_sku
				FROM PFX_am_label
				WHERE include_type IN (0,2)
				ORDER BY pos DESC";
			$query = $this->_applyTablePrefix($query);
			$labelsTable = $this->_dbi->fetchAll($query);
			// Load each label into the lookup table
			foreach($labelsTable as $row)
			{
				// Get the comma-separated SKUs
				$skus = explode(",", $row[2]);
				// Add each SKU to the lookup table
				foreach($skus as $sku)
				{
					$AmastyProductLabelsLookupTable[$sku] = array($row[0], $row[1]);
				}
			}
		}

		// Increase maximium length for group_concat (for additional image URLs field)
		$query = "SET SESSION group_concat_max_len = 1000000;";
		$this->_dbi->query($query);

		// By default, set media gallery attribute id to 703
		//  Look it up later
		$MEDIA_GALLERY_ATTRIBUTE_ID = 703;


		// Get the entity type for products
		$query = "SELECT entity_type_id FROM PFX_eav_entity_type
			WHERE entity_type_code = 'catalog_product'";
		$query = $this->_applyTablePrefix($query);
		$PRODUCT_ENTITY_TYPE_ID = $this->_dbi->fetchOne($query);


		// Get attribute codes and types
		$query = "SELECT attribute_id, attribute_code, backend_type, frontend_input
			FROM PFX_eav_attribute
			WHERE entity_type_id = $PRODUCT_ENTITY_TYPE_ID
			";
		$query = $this->_applyTablePrefix($query);
		$attributes = $this->_dbi->fetchAssoc($query);
		$attributeCodes = array();
		$blankProduct = array();
		$blankProduct['sku'] = '';
		foreach($attributes as $row)
		{
			// Save attribute ID for media gallery
			if($row['attribute_code'] == 'media_gallery')
			{
				$MEDIA_GALLERY_ATTRIBUTE_ID = $row['attribute_id'];
			}

			switch($row['backend_type'])
			{
				case 'datetime':
				case 'decimal':
				case 'int':
				case 'text':
				case 'varchar':
					$attributeCodes[$row['attribute_id']] = $row['attribute_code'];
					$blankProduct[$row['attribute_code']] = '';
				break;
			case 'static':
				// ignore columns in entity table
				// print("Skipping static attribute: ".$row['attribute_code']."\n");
				break;
			default:
				// print("Unsupported backend_type: ".$row['backend_type']."\n");
				break;
			}

			// If the type is multiple choice, cache the option values
			//   in a lookup array for performance (avoids several joins/aggregations)
			if($row['frontend_input'] == 'select' || $row['frontend_input'] == 'multiselect')
			{
				// Get the option_id => value from the attribute options
				$query = "
					SELECT
						 CASE WHEN SUM(aov.store_id) = 0 THEN MAX(aov.option_id) ELSE
							MAX(CASE WHEN aov.store_id = ".$this->_storeId." THEN aov.option_id ELSE NULL END)
						 END AS 'option_id'
						,CASE WHEN SUM(aov.store_id) = 0 THEN MAX(aov.value) ELSE
							MAX(CASE WHEN aov.store_id = ".$this->_storeId." THEN aov.value ELSE NULL END)
						 END AS 'value'
					FROM PFX_eav_attribute_option AS ao
					INNER JOIN PFX_eav_attribute_option_value AS aov
						ON ao.option_id = aov.option_id
					WHERE aov.store_id IN (".$this->_storeId.", 0)
						AND ao.attribute_id = ".$row['attribute_id']."
					GROUP BY aov.option_id
				";
				$query = $this->_applyTablePrefix($query);
				$result = $this->_dbi->fetchPairs($query);

				// If found, then save the lookup table in the attributeOptions array
				if(is_array($result))
				{
					$attributeOptions[$row['attribute_id']] = $result;
				}
				else
				{
					// Otherwise, leave a blank array
					$attributeOptions[$row['attribute_id']] = array();
				}
				$result = null;
			}

		}
		$blankProduct['onefeed_product_url'] = '';
		$blankProduct['onefeed_image_url'] = '';
		$blankProduct['onefeed_additional_image_url'] = '';
		$blankProduct['onefeed_additional_image_value_id'] = '';
		$blankProduct['json_categories'] = '';
		$blankProduct['json_tier_pricing'] = '';
		$blankProduct['qty'] = 0;
		$blankProduct['stock_status'] = '';
		$blankProduct['onefeed_color_attribute_id'] = '';
		$blankProduct['onefeed_regular_price'] = '';
		$blankProduct['parent_id'] = '';
		$blankProduct['entity_id'] = '';
		$blankProduct['created_at'] = '';
		$blankProduct['updated_at'] = '';
		if($AmastyProductLabelsTableExists == true)
		{
			$blankProduct['amasty_label_id'] = '';
			$blankProduct['amasty_label_name'] = '';
		}

		// Build queries for each attribute type
		$backendTypes = array(
			'datetime',
			'decimal',
			'int',
			'text',
			'varchar',
		);
		$queries = array();
		foreach($backendTypes as $backendType)
		{
			// Get store value if there is one, otherwise, global value
			$queries[] = "
		SELECT CASE WHEN SUM(ev.store_id) = 0 THEN MAX(ev.value) ELSE
			MAX(CASE WHEN ev.store_id = ".$this->_storeId." THEN ev.value ELSE NULL END)
			END AS 'value', ev.attribute_id
		FROM PFX_catalog_product_entity
		INNER JOIN PFX_catalog_product_entity_$backendType AS ev
			ON PFX_catalog_product_entity.entity_id = ev.entity_id
		WHERE ev.store_id IN (".$this->_storeId.", 0)
		AND ev.entity_type_id = $PRODUCT_ENTITY_TYPE_ID
		AND ev.entity_id = @ENTITY_ID
		GROUP BY ev.attribute_id, ev.entity_id
		";
		}
		$query = implode(" UNION ALL ", $queries);
		$MasterProductQuery = $query;

		// Get all entity_ids for all products in the selected store
		//  into an array - require SKU to be defined
		// check if exportContinue is set
		if(!$exportContinue){
			$query = "
				SELECT cpe.entity_id
				FROM PFX_catalog_product_entity AS cpe
				INNER JOIN PFX_catalog_product_website as cpw
					ON cpw.product_id = cpe.entity_id
				WHERE cpw.website_id = ".$this->_websiteId."
					AND IFNULL(cpe.sku, '') != ''
				LIMIT 0, ".$this->_selectLimit."
			";
		}
		else{
			$query = "
				SELECT cpe.entity_id
				FROM PFX_catalog_product_entity AS cpe
				INNER JOIN PFX_catalog_product_website as cpw
					ON cpw.product_id = cpe.entity_id
				WHERE cpw.website_id = ".$this->_websiteId."
					AND IFNULL(cpe.sku, '') != '' AND cpe.entity_id > ".$lockVar['exported_entity_id']."
					LIMIT 0, ".$this->_selectLimit."
			";
		}
		$query = $this->_applyTablePrefix($query);
		// Just fetch the entity_id column to save memory
		$entity_ids = $this->_dbi->fetchCol($query);


		// check if $entity_ids count is zero than export is done
		if(count($entity_ids) == 0)
		{
			$lockVar['timestamp'] = time();
			$lockVar['status'] = 'done';
			file_put_contents($this->_lockFile , serialize($lockVar));
			Mage::log('Entity id count ==0 cron export finish' , null , 'onefeed.log');
			return true;
		}


		// Print header row
		$headerFields = array();
		$headerFields[] = 'sku';
		foreach($attributeCodes as $fieldName)
		{
			$headerFields[] = str_replace('"', '""', $fieldName);
		}
		$headerFields[] = 'onefeed_product_url';
		$headerFields[] = 'onefeed_image_url';
		$headerFields[] = 'onefeed_additional_image_url';
		$headerFields[] = 'onefeed_additional_image_value_id';
		$headerFields[] = 'json_categories';

		$headerFields[] = 'json_tier_pricing';
		$headerFields[] = 'qty';
		$headerFields[] = 'stock_status';
		$headerFields[] = 'onefeed_color_attribute_id';
		$headerFields[] = 'onefeed_regular_price';
		$headerFields[] = 'parent_id';
		$headerFields[] = 'entity_id';
		$headerFields[] = 'created_at';
		$headerFields[] = 'updated_at';
		if($AmastyProductLabelsTableExists == true)
		{
			$headerFields[] = 'amasty_label_id';
			$headerFields[] = 'amasty_label_name';
		}

		$headerFields[] = 'category';
		$headerFields[] = 'category2';
		$headerFields[] = 'category3';

		//print '"'.implode('","', $headerFields).'"'."\n";

		// check if exportContinue mode is on than do not print headers
		if(!$exportContinue)
		{
			fputcsv($fp ,$headerFields,$this->_delimiter);
		}

		// Loop through each product and output the data
		foreach($entity_ids as $entity_id)
		{
			// Check if the item is out of stock and skip if needed
			if($this->ExcludeOutOfStock == true)
			{
				$query = "
					SELECT stock_status
					FROM PFX_cataloginventory_stock_status AS ciss
					WHERE ciss.website_id = ".$this->_websiteId."
						AND ciss.product_id = ".$entity_id."
				";
				$query = $this->_applyTablePrefix($query);
				$stock_status = $this->_dbi->fetchOne($query);
				// If stock status not found or equal to zero, skip the item
				if(empty($stock_status))
				{
					continue;
				}
			}

			// Create a new product record
			$product = $blankProduct;
			$product['entity_id'] = $entity_id;

			// Get the basic product information
			$query = "
				SELECT cpe.sku, cpe.created_at, cpe.updated_at
				FROM PFX_catalog_product_entity AS cpe
				WHERE cpe.entity_id = ".$entity_id."
			";
			$query = $this->_applyTablePrefix($query);
			$entity = $this->_dbi->fetchRow($query);
			if(empty($entity) == true)
			{
				continue;
			}

			// Initialize basic product data
			$product['sku'] = $entity['sku'];
			$product['created_at'] = $entity['created_at'];
			$product['updated_at'] = $entity['updated_at'];

			// Set label information
			if($AmastyProductLabelsTableExists == true)
			{
				// Check if the SKU has a label
				if(array_key_exists($product['sku'], $AmastyProductLabelsLookupTable) == true)
				{
					// Set the label ID and name
					$product['amasty_label_id'] = $AmastyProductLabelsLookupTable[$product['sku']][0];
					$product['amasty_label_name'] = $AmastyProductLabelsLookupTable[$product['sku']][1];
				}
			}

			// Fill the master query with the entity ID
			$query = str_replace('@ENTITY_ID', $entity_id, $MasterProductQuery);
			$query = $this->_applyTablePrefix($query);
			$result = $this->_dbi->query($query);

			// Escape the SKU (it may contain double-quotes)
			$product['sku'] = str_replace('"', '""', $product['sku']);

			// Loop through each field in the row and get the value
			while(true)
			{
				// Get next column
				// $column[0] = value
				// $column[1] = attribute_id
				$column = $result->fetch(Zend_Db::FETCH_NUM);
				// Break if no more rows
				if(empty($column))
				{
					break;
				}
				// Skip attributes that don't exist in eav_attribute
				if(!isset($attributeCodes[$column[1]]))
				{
					continue;
				}

				// Save color attribute ID (for CJM automatic color swatches extension)
				//  NOTE: do this prior to translating option_id to option_value below
				if($attributeCodes[$column[1]] == 'color')
				{
					$product['onefeed_color_attribute_id'] = $column[0];
				}

				// Translate the option option_id to a value.
				if(isset($attributeOptions[$column[1]]) == true)
				{
					// Convert all option values
					$optionValues = explode(',', $column[0]);
					$convertedOptionValues = array();
					foreach($optionValues as $optionValue)
					{
						if(isset($attributeOptions[$column[1]][$optionValue]) == true)
						{
							// If a option_id is found, translate it
							$convertedOptionValues[] = $attributeOptions[$column[1]][$optionValue];
						}
					}
					// Erase values that are set to zero
					if($column[0] == '0')
					{
						$column[0] = '';
					}
					elseif(empty($convertedOptionValues) == false)
					{
						// Use convert values if any conversions exist
						$column[0] = implode(',', $convertedOptionValues);
					}
					// Otherwise, leave value as-is
				}

				// Escape double-quotes and add to product array

				if($attributeCodes[$column[1]]=='description' || $attributeCodes[$column[1]]=='short_description')
				{
					$product[$attributeCodes[$column[1]]] =  htmlspecialchars(strip_tags(str_replace(array("\n", "\t", "\r"), array(" "," "," "), $column[0])), ENT_QUOTES);
				} else {
					$product[$attributeCodes[$column[1]]] = str_replace('"', '""',str_replace(array("\n", "\t", "\r"), array(" "," "," "), $column[0]));
				}

			}

			$result = null;

			// Skip product that are disabled or have no status
			//  if the checkbox is not checked (this is the default setting)
			if($this->IncludeDisabled == false)
			{
				if(empty($product['status']) || $product['status'] == Mage_Catalog_Model_Product_Status::STATUS_DISABLED)
				{
					continue;
				}
			}

			// Get category information
			$query = "
				SELECT fs.entity_id, fs.path, fs.name
				FROM PFX_catalog_category_product_index AS pi
					INNER JOIN PFX_catalog_category_flat_store_".$this->_storeId." AS fs
						ON pi.category_id = fs.entity_id
				WHERE pi.product_id = ".$entity_id."
			";
			$query = $this->_applyTablePrefix($query);
			$categoriesTable = $this->_dbi->fetchAll($query);
			// Save entire table in JSON format
			$product['json_categories'] = json_encode($categoriesTable);
			// Escape double-quotes
			$product['json_categories'] = str_replace('"', '""', $product['json_categories']);

			//get category,sub category,sub-sub-category

			$query_cat1 = "
				SELECT fs.name
				FROM PFX_catalog_category_product_index AS pi
					INNER JOIN PFX_catalog_category_flat_store_".$this->_storeId." AS fs
						ON pi.category_id = fs.entity_id
				WHERE pi.product_id = ".$entity_id." AND level=1
			";


			$query_cat1 = $this->_applyTablePrefix($query_cat1);
			$categoriesTable1 = $this->_dbi->fetchOne($query_cat1);
			// Save entire table in JSON format
			$product['category'] = $categoriesTable1;


			$query_cat2 = "
				SELECT fs.name
				FROM PFX_catalog_category_product_index AS pi
					INNER JOIN PFX_catalog_category_flat_store_".$this->_storeId." AS fs
						ON pi.category_id = fs.entity_id
				WHERE pi.product_id = ".$entity_id." AND level=2
			";

			$query_cat2 = $this->_applyTablePrefix($query_cat2);
			$categoriesTable2 = $this->_dbi->fetchOne($query_cat2);
			// Save entire table in JSON format
			$product['category2'] = $categoriesTable2;

			$query_cat3 = "
				SELECT fs.name
				FROM PFX_catalog_category_product_index AS pi
					INNER JOIN PFX_catalog_category_flat_store_".$this->_storeId." AS fs
						ON pi.category_id = fs.entity_id
				WHERE pi.product_id = ".$entity_id." AND level=3
			";

			$query_cat3 = $this->_applyTablePrefix($query_cat3);

			$categoriesTable3 = $this->_dbi->fetchOne($query_cat3);
			// Save entire table in JSON format
			$product['category3'] = $categoriesTable3;


			// Get stock quantity
			// NOTE: stock_id = 1 is the 'Default' stock
			$query = "
				SELECT qty, stock_status
				FROM PFX_cataloginventory_stock_status
				WHERE product_id=".$entity_id."
					AND website_id=".$this->_websiteId."
					AND stock_id = 1";
			$query = $this->_applyTablePrefix($query);
			$stockInfoResult = $this->_dbi->query($query);
			$stockInfo = $stockInfoResult->fetch();
			if(empty($stockInfo) == true)
			{
				$product['qty'] = '0';
				$product['stock_status'] = '';
			}
			else
			{
				$product['qty'] = $stockInfo['qty'];
				$product['stock_status'] = $stockInfo['stock_status'];
			}
			$stockInfoResult = null;

			// Get additional image URLs
			$galleryImagePrefix = $this->_dbi->quote($this->_mediaBaseUrl.'catalog/product');
			$query = "
				SELECT
					 GROUP_CONCAT(gallery.value_id SEPARATOR ',') AS value_id
					,GROUP_CONCAT(CONCAT(".$galleryImagePrefix.", gallery.value) SEPARATOR ',') AS value
				FROM PFX_catalog_product_entity_media_gallery AS gallery
					INNER JOIN PFX_catalog_product_entity_media_gallery_value AS gallery_value
						ON gallery.value_id = gallery_value.value_id
				WHERE   gallery_value.store_id IN (".$this->_storeId.", 0)
					AND gallery_value.disabled = 0
					AND gallery.entity_id=".$entity_id."
					AND gallery.attribute_id = ".$MEDIA_GALLERY_ATTRIBUTE_ID."
				ORDER BY gallery_value.position ASC";
			$query = $this->_applyTablePrefix($query);
			$galleryValues = $this->_dbi->fetchAll($query);
			if(empty($galleryValues) != true)
			{
				// Save value IDs for CJM automatic color swatches extension support

				$product['onefeed_additional_image_value_id'] = $galleryValues[0][0];
				$product['onefeed_additional_image_url'] = $galleryValues[0][1];
			}

			// Get parent ID
			$query = "
				SELECT GROUP_CONCAT(parent_id SEPARATOR ',') AS parent_id
				FROM PFX_catalog_product_super_link AS super_link
				WHERE super_link.product_id=".$entity_id."";
			$query = $this->_applyTablePrefix($query);
			$parentId = $this->_dbi->fetchAll($query);
			if(empty($parentId) != true)
			{
				// Save value IDs for CJM automatic color swatches extension support
				$product['parent_id'] = $parentId[0]['parent_id'];
			}

			// Get the regular price (before any catalog price rule is applied)
			$product['onefeed_regular_price'] = $product['price'];

			// Override price with catalog price rule, if found
			$query = "
				SELECT crpp.rule_price
				FROM PFX_catalogrule_product_price AS crpp
				WHERE crpp.rule_date = CURDATE()
					AND crpp.product_id = ".$entity_id."
					AND crpp.customer_group_id = 1
					AND crpp.website_id = ".$this->_websiteId;
			$query = $this->_applyTablePrefix($query);
			$rule_price = $this->_dbi->fetchAll($query);
			if(empty($rule_price) != true)
			{
				// Override price with catalog rule price
				$product['price'] = $rule_price[0][0];
			}

			// Calculate image and product URLs
			if(empty($product['url_path']) == false)
			{
				$product['onefeed_product_url'] = $this->_urlPathJoin($this->_webBaseUrl, $product['url_path']);
			}
			if(empty($product['image']) == false)
			{
				$product['onefeed_image_url'] = $this->_urlPathJoin($this->_mediaBaseUrl, 'catalog/product');
				$product['onefeed_image_url'] = $this->_urlPathJoin($product['onefeed_image_url'], $product['image']);
			}

			// Get tier pricing information
			$query = "
				SELECT tp.qty, tp.value
				FROM PFX_catalog_product_entity_tier_price AS tp
				WHERE tp.entity_id = ".$entity_id."
					AND tp.website_id IN (0, ".$this->_websiteId.")
					AND tp.all_groups = 1
					AND tp.customer_group_id = 0
			";
			$query = $this->_applyTablePrefix($query);
			$tierPricingTable = $this->_dbi->fetchAll($query);
			// Save entire table in JSON format
			$product['json_tier_pricing'] = json_encode($tierPricingTable);
			// Escape double-quotes
			$product['json_tier_pricing'] = str_replace('"', '""', $product['json_tier_pricing']);

			// Print out the line in CSV format
			//print '"'.implode('","', $product).'"'."\n";
			fputcsv($fp ,$product,$this->_delimiter);

			// re-write lock variable
			$lockVar['date'] = Mage::getModel('core/date')->date('Y-m-d');
			$lockVar['timestamp'] = time();
			$lockVar['status'] = 'continue';
			$lockVar['exported_entity_id'] = $entity_id;
			file_put_contents($this->_lockFile , serialize($lockVar));
		}
		// now send file to FTP
		$lockVar = unserialize(file_get_contents($this->_lockFile));
		if($lockVar['status']=='done' && $lockVar['date']== Mage::getModel('core/date')->date('Y-m-d')){
			return true;
		}
		else
		{
			$source 	= $this->_mediaBasePath . DS .'onefeed'.DS.$FileName;
			$dest       = $FileName;
			$flocal = fopen($source, 'r');
			$ftp 		= new Varien_Io_Ftp();
			if(strlen($this->_FtpUserName)>0 && strlen($this->_FtpPassword)>0){
				$ftp->open(
				        array(
				                'host'      => $this->_FtpHostName,
				                'user'  	=> $this->_FtpUserName,
				                'password'  => $this->_FtpPassword,
				            )
				        );
				$ftp_w = $ftp->write($dest, $flocal);
				$ftp->close();
			}
			$lockVar['date'] 		= Mage::getModel('core/date')->date('Y-m-d');
			$lockVar['timestamp'] 	= time();
			$lockVar['status'] 		= 'done';
			file_put_contents($this->_lockFile , serialize($lockVar));
		}
	}

	// Join two URL paths and handle forward slashes
	private function _urlPathJoin($part1, $part2)
	{
		return rtrim($part1, '/').'/'.ltrim($part2, '/');
	}

	// Print the results of a select query to output for debugging purposes and exit
	private function _debugPrintQuery($query)
	{
		$query = "SELECT 1";
		print '<pre>';
		print_r($this->_dbi->fetchAll($query));
		print '</pre>';
		exit();
	}

	public function getFreeMemory()
	{
		$used = memory_get_usage(true)/1048576;
		$limitStr = (int)ini_get('memory_limit');
		$limitStr = trim($limitStr);
		$last = strtolower($limitStr[strlen($limitStr)-1]);
		switch($last) {
			// The 'G' modifier is available since PHP 5.1.0
			case 'g':
				$limit *= 1024;
			case 'm':
				$limit *= 1024;
			case 'k':
				$limit *= 1024;
		}
		$limit = $limit/1048576;
		return $limit - $used;
	}

}
