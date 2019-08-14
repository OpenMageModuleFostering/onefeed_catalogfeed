<?php
class Onefeed_Catalogfeed_Block_Adminhtml_Button extends Mage_Adminhtml_Block_System_Config_Form_Field
{
    /*
     * Set template
     */
    protected function _construct()
    {
        parent::_construct();
        $this->setTemplate('onefeed/button.phtml');
    }
 
    /**
     * Return element html
     *
     * @param  Varien_Data_Form_Element_Abstract $element
     * @return string
     */
    protected function _getElementHtml(Varien_Data_Form_Element_Abstract $element)
    {
        return $this->_toHtml();
    }
 
    /**
     * Return ajax url for button
     *
     * @return string
     */
    public function getAjaxIndexUrl()
    {
        return Mage::helper('adminhtml')->getUrl('admin_catalogfeed/adminhtml_catalogfeedbackend/index');
    }

     public function getAjaxCheckUrl()
    {
        return Mage::helper('adminhtml')->getUrl('admin_catalogfeed/adminhtml_catalogfeedbackend/check');
    }
 
    /**
     * Generate button html
     *
     * @return string
     */
    public function getButtonHtml()
    {
        $button = $this->getLayout()->createBlock('adminhtml/widget_button')
            ->setData(array(
            'id'        => 'check_button',
            'label'     => $this->helper('adminhtml')->__('Export Now'),
            'onclick'   => 'javascript:run(); return false;'
        ));
 
        return $button->toHtml();
    }
}