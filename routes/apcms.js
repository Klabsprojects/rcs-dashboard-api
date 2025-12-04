const express = require('express');
const router = express.Router();
const ct_apcms = require('../controllers/apcms');

// For APCMS Data entry
router.post('/add_item_master', ct_apcms.addItemMaster);
router.post('/add_ledger_master', ct_apcms.addLedgerMaster);

router.get('/get_last_record/:type', ct_apcms.getLastRecordByType);

router.post('/add_loan_data', ct_apcms.addMultipleLoanData);
router.post('/add_sales_data', ct_apcms.addMultipleSalesData);

// For PowerBI
router.get('/getLoanRecords/:ledgerNo/:itemNo', ct_apcms.getLoanRecordsByLedgerItemAndDate);

router.get('/getLoan', ct_apcms.getLoan);
router.get('/getSales', ct_apcms.getSales);


module.exports = router;