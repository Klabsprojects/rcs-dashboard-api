const express = require('express');
const router = express.Router();
const ct_apcms = require('../controllers/apcms');

// For APCMS Data entry & PowerBI
router.post('/add_item_master', ct_apcms.addItemMaster);
router.get('/get_item_master', ct_apcms.listAllItems);

router.post('/add_society_master', ct_apcms.addSocietyMaster);
router.get('/get_society_master', ct_apcms.listSocietyMaster);

router.post('/add_member_loan_deposit', ct_apcms.addMultipleMemberLoanDeposits);
router.get('/get_member_loan_deposit/:type', ct_apcms.getMemberLoanDeposit);

router.post('/add_sales_purchase', ct_apcms.addMultipleSalesPurchases);
router.get('/get_sales_purchase/:type', ct_apcms.getSalesPurchase);

router.post('/add_marketing', ct_apcms.addMultipleMarketingRecords);
router.get('/get_marketing', ct_apcms.getMarketing);

router.post('/add_godown_util', ct_apcms.addMultipleGodownUtilizations);
router.get('/get_godown_util', ct_apcms.getGodownUtilization);


//router.post('/add_ledger_master', ct_apcms.addLedgerMaster);
//router.get('/get_last_record/:type', ct_apcms.getLastRecordByType);
//router.post('/add_loan_data', ct_apcms.addMultipleLoanData);
//router.post('/add_sales_data', ct_apcms.addMultipleSalesData);
//router.get('/getLedgers', ct_apcms.listAllLedgers);
//router.get('/getLoanRecords/:ledgerNo/:itemNo', ct_apcms.getLoanRecordsByLedgerItemAndDate);
//router.get('/getLoan', ct_apcms.getLoan);
//router.get('/getSales', ct_apcms.getSales);


module.exports = router;