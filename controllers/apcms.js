const express = require('express');
const pool = require('../db');
const AppError = require('../utils/AppError');
const router = express.Router();

// simple required-field check
const requireFields = (body, fields) => {
  const missing = fields.filter(f => body[f] === undefined || body[f] === null);
  return missing;
};

exports.addItemMaster = async (req, res, next) => {
  try {
    const body = req.body;

    const requiredFields = [
      'row_id',
      'item_id',
      'item_code',
      'category',
      'item_name'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(
        `Missing required fields: ${missing.join(', ')}`,
        400,
        { isOperational: true } // show this message even in production
      );
    }


    const { ritem_id, item_code, category, item_name } = body;

    // Check if row_id exists
    const [rows] = await pool.execute(
      'SELECT row_id FROM apcms_item_master WHERE row_id = ?',
      [row_id]
    );

    let result;
    let action;

    if (rows.length === 0) {
      // INSERT
      const insertSql = `
        INSERT INTO apcms_item_master (
          row_id,
          item_id,
          item_code,
          category,
          item_name
        ) VALUES (?, ?, ?, ?, ?)
      `;

      const insertValues = [
        row_id,
        item_id,
        item_code,
        category,
        item_name
      ];

      const [insertResult] = await pool.execute(insertSql, insertValues);
      result = insertResult;
      action = 'insert';
    } else {
      // UPDATE
      const updateSql = `
        UPDATE apcms_item_master
        SET
          item_id = ?,
          item_code = ?,
          category = ?,
          item_name = ?
        WHERE row_id = ?
      `;

      const updateValues = [
        item_id,
        item_code,
        category,
        item_name,
        row_id
      ];

      const [updateResult] = await pool.execute(updateSql, updateValues);
      result = updateResult;
      action = 'update';
    }

    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      id: result.insertId || row_id,
      data: body,
      message:
        action === 'insert'
          ? 'Item master record created successfully'
          : 'Item master record updated successfully'
    });
  } catch (error) {
    next(error);
  }
};


exports.addLedgerMaster = async (req, res, next) => {
  try {
    const body = req.body;

    const requiredFields = [
      'row_id',
      'ledger_id',
      'ledger_code',
      'ledger_name',
      'created_date',
      'status',
      'ledger_type'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(
        `Missing required fields: ${missing.join(', ')}`,
        400,
        { isOperational: true } // show this message even in production
      );
    }


    const {
      row_id,
      ledger_id,
      ledger_code,
      ledger_name,
      created_date,
      status,
      ledger_type
    } = body;

    // Check if row_id exists
    const [rows] = await pool.execute(
      'SELECT row_id FROM apcms_ledger_master WHERE row_id = ?',
      [row_id]
    );

    let result;
    let action;

    if (rows.length === 0) {
      // INSERT
      const insertSql = `
        INSERT INTO apcms_ledger_master (
          row_id,
          ledger_id,
          ledger_code,
          ledger_name,
          created_date,
          status,
          ledger_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `;

      const insertValues = [
        row_id,
        ledger_id,
        ledger_code,
        ledger_name,
        created_date,
        status,
        ledger_type
      ];

      const [insertResult] = await pool.execute(insertSql, insertValues);
      result = insertResult;
      action = 'insert';
    } else {
      // UPDATE
      const updateSql = `
        UPDATE apcms_ledger_master
        SET
          ledger_id = ?,
          ledger_code = ?,
          ledger_name = ?,
          created_date = ?,
          status = ?,
          ledger_type = ?
        WHERE row_id = ?
      `;

      const updateValues = [
        ledger_id,
        ledger_code,
        ledger_name,
        created_date,
        status,
        ledger_type,
        row_id
      ];

      const [updateResult] = await pool.execute(updateSql, updateValues);
      result = updateResult;
      action = 'update';
    }

    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      id: result.insertId || row_id,
      data: body,
      message:
        action === 'insert'
          ? 'Ledger master record created successfully'
          : 'Ledger master record updated successfully'
    });
  } catch (error) {
    next(error);
  }
};


exports.addLoanData = async (req, res, next) => {
  try {
    const body = req.body;

    // 1) Basic validation
    const requiredFields = [
      'row_id',
      'ledger_id',
      'item_id',
      'item_name',
      'entry_date',
      'pkt_opening',
      'pkt_issued',
      'pkt_collection',
      'pkt_balance',
      'pkt_opening_value',
      'pkt_issued_value',
      'pkt_collection_value',
      'pkt_balance_value',
      'created_date',
      'system_name'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(
        `Missing required fields: ${missing.join(', ')}`,
        400,
        { isOperational: true } // show this message even in production
      );
    }

    const {
      row_id,
      ledger_id,
      item_id,
      item_name,
      entry_date,
      pkt_opening,
      pkt_issued,
      pkt_collection,
      pkt_balance,
      pkt_opening_value,
      pkt_issued_value,
      pkt_collection_value,
      pkt_balance_value,
      created_date,
      system_name
    } = body;

    // 2) Check if row_id exists
    const [rows] = await pool.execute(
      'SELECT row_id FROM apcms_loan WHERE row_id = ?',
      [row_id]
    );

    // --- If Record exist -->  ---
    if (rows.length > 0) {
      // 3A) Record exists → DO NOTHING (exit and return success status)
      return res.status(200).json({
        success: true,
        action: 'no action', // No operation performed
        id: row_id,
        message: `Loan record with row_id ${row_id} already exists. No action taken.`
      });
    }

    // 3B) Not exists → INSERT
    const insertSql = `
        INSERT INTO apcms_loan (
          row_id,
          ledger_id,
          item_id,
          item_name,
          entry_date,
          pkt_opening,
          pkt_issued,
          pkt_collection,
          pkt_balance,
          pkt_opening_value,
          pkt_issued_value,
          pkt_collection_value,
          pkt_balance_value,
          created_date,
          system_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;

    const insertValues = [
      row_id,
      ledger_id,
      item_id,
      item_name,
      entry_date,
      pkt_opening,
      pkt_issued,
      pkt_collection,
      pkt_balance,
      pkt_opening_value,
      pkt_issued_value,
      pkt_collection_value,
      pkt_balance_value,
      created_date,
      system_name
    ];

    const [result] = await pool.execute(insertSql, insertValues);

    // 4) Response for successful INSERT
    res.status(201).json({
      success: true,
      action: 'insert',
      id: result.insertId || row_id,
      data: body,
      message: 'Loan record created successfully.'
    });
  } catch (error) {
    next(error); // centralized error handler + logger
  }
};

exports.addMultipleLoanData = async (req, res, next) => {
  try {
    const records = req.body;

    // 1. Initial validation: Check if records is an array and not empty
    if (!Array.isArray(records) || records.length === 0) {
      throw new AppError('Request body must be a non-empty array of loan records.', 400, { isOperational: true });
    }

    const requiredFields = [
      'row_id', 'ledger_id', 'item_id', 'item_name', 'entry_date',
      'pkt_opening', 'pkt_issued', 'pkt_collection', 'pkt_balance',
      'pkt_opening_value', 'pkt_issued_value', 'pkt_collection_value',
      'pkt_balance_value', 'created_date', 'system_name'
    ];

    const results = []; // Array to store the outcome of each record operation

    // 2. ITERATE OVER ALL RECORDS
    for (const body of records) {
      const result = { row_id: body.row_id, action: 'error', message: 'Unknown error occurred.' };

      // --- A) PER-RECORD VALIDATION ---
      const missing = requireFields(body, requiredFields);
      if (missing.length) {
        result.message = `Validation failed: Missing fields: ${missing.join(', ')}`;
        results.push(result);
        continue; // Skip to the next record
      }

      // Destructure for easy access
      const {
        row_id, ledger_id, item_id, item_name, entry_date,
        pkt_opening, pkt_issued, pkt_collection, pkt_balance,
        pkt_opening_value, pkt_issued_value, pkt_collection_value,
        pkt_balance_value, created_date, system_name
      } = body;

      try {
        // 3. CHECK IF RECORD EXISTS
        const [rows] = await pool.execute(
          'SELECT row_id FROM apcms_loan WHERE row_id = ?',
          [row_id]
        );

        if (rows.length > 0) {
          // Record exists -> NO ACTION
          result.action = 'noop';
          result.message = `Record already exists. No action taken.`;
        } else {
          // Record NOT exists -> INSERT
          const insertSql = `
                        INSERT INTO apcms_loan (
                            row_id, ledger_id, item_id, item_name, entry_date, 
                            pkt_opening, pkt_issued, pkt_collection, pkt_balance, 
                            pkt_opening_value, pkt_issued_value, pkt_collection_value, 
                            pkt_balance_value, created_date, system_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `;

          const insertValues = [
            row_id, ledger_id, item_id, item_name, entry_date,
            pkt_opening, pkt_issued, pkt_collection, pkt_balance,
            pkt_opening_value, pkt_issued_value, pkt_collection_value,
            pkt_balance_value, created_date, system_name
          ];

          const [insertResult] = await pool.execute(insertSql, insertValues);

          result.action = 'insert';
          result.id = insertResult.insertId || row_id;
          result.message = 'Record created successfully.';
        }
      } catch (dbError) {
        // Catch database errors specific to this record
        result.message = `Database error: ${dbError.message}`;
        // Log the full error for debugging (optional)
        console.error(`Error processing row_id ${row_id}:`, dbError);
      }

      // Add the outcome to the final results array
      results.push(result);
    }

    // 4. SEND AGGREGATE RESPONSE
    const insertedCount = results.filter(r => r.action === 'insert').length;
    const noopCount = results.filter(r => r.action === 'noop').length;
    const errorCount = results.filter(r => r.action === 'error').length;

    res.status(200).json({
      success: errorCount === 0, // Overall success only if no records failed
      summary: {
        totalRecords: records.length,
        inserted: insertedCount,
        skipped: noopCount,
        failed: errorCount
      },
      results: results,
      message: `Batch process complete. Inserted ${insertedCount} records, skipped ${noopCount}, failed ${errorCount}.`
    });

  } catch (error) {
    next(error);
  }
};

exports.addSalesData = async (req, res, next) => {
  try {
    const body = req.body;

    const requiredFields = [
      'row_id',
      'ledger_id',
      'item_id',
      'item_name',
      'entry_date',
      'total_qty',
      'total_amount',
      'created_date',
      'system_name'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(
        `Missing required fields: ${missing.join(', ')}`,
        400,
        { isOperational: true } // show this message even in production
      );
    }

    const {
      row_id,
      ledger_id,
      item_id,
      item_name,
      entry_date,
      total_qty,
      total_amount,
      created_date,
      system_name
    } = body;

    // Check if row_id exists
    const [rows] = await pool.execute(
      'SELECT row_id FROM apcms_sales WHERE row_id = ?',
      [row_id]
    );

    let result;
    let action;

    if (rows.length === 0) {
      // INSERT
      const insertSql = `
        INSERT INTO apcms_sales (
          row_id,
          ledger_id,
          item_id,
          item_name,
          entry_date,
          total_qty,
          total_amount,
          created_date,
          system_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;

      const insertValues = [
        row_id,
        ledger_id,
        item_id,
        item_name,
        entry_date,
        total_qty,
        total_amount,
        created_date,
        system_name
      ];

      const [insertResult] = await pool.execute(insertSql, insertValues);
      result = insertResult;
      action = 'insert';
    } else {
      // UPDATE
      const updateSql = `
        UPDATE apcms_sales
        SET
          ledger_id = ?,
          item_id = ?,
          item_name = ?,
          entry_date = ?,
          total_qty = ?,
          total_amount = ?,
          created_date = ?,
          system_name = ?
        WHERE row_id = ?
      `;

      const updateValues = [
        ledger_id,
        item_id,
        item_name,
        entry_date,
        total_qty,
        total_amount,
        created_date,
        system_name,
        row_id
      ];

      const [updateResult] = await pool.execute(updateSql, updateValues);
      result = updateResult;
      action = 'update';
    }

    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      id: result.insertId || row_id,
      data: body,
      message:
        action === 'insert'
          ? 'Sales record created successfully'
          : 'Sales record updated successfully'
    });
  } catch (error) {
    next(error);
  }
};


exports.addMultipleSalesData = async (req, res, next) => {
  try {
    // 1. EXPECT req.body TO BE AN ARRAY
    const records = req.body;

    // Initial validation: Check if records is an array and not empty
    if (!Array.isArray(records) || records.length === 0) {
      throw new AppError('Request body must be a non-empty array of sales records.', 400, { isOperational: true });
    }

    const requiredFields = [
      'row_id', 'ledger_id', 'item_id', 'item_name', 'entry_date',
      'total_qty', 'total_amount', 'created_date', 'system_name'
    ];

    const results = []; // Array to store the outcome of each record operation

    // 2. ITERATE OVER ALL RECORDS
    for (const body of records) {
      // Initialize result object for current record
      const result = { row_id: body.row_id, action: 'error', message: 'Unknown error occurred.' };

      // --- A) PER-RECORD VALIDATION ---
      const missing = requireFields(body, requiredFields);
      if (missing.length) {
        result.message = `Validation failed: Missing fields: ${missing.join(', ')}`;
        results.push(result);
        continue; // Skip to the next record in the batch
      }

      // Destructure for easy access
      const {
        row_id, ledger_id, item_id, item_name, entry_date,
        total_qty, total_amount, created_date, system_name
      } = body;

      try {
        // 3. CHECK IF RECORD EXISTS
        const [rows] = await pool.execute(
          'SELECT row_id FROM apcms_sales WHERE row_id = ?',
          [row_id]
        );

        if (rows.length > 0) {
          // Record exists -> NO ACTION (Skip the update logic)
          result.action = 'noop';
          result.message = `Sales record with row_id ${row_id} already exists. No action taken.`;
        } else {
          // Record NOT exists -> INSERT
          const insertSql = `
                        INSERT INTO apcms_sales (
                            row_id, ledger_id, item_id, item_name, entry_date, 
                            total_qty, total_amount, created_date, system_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `;

          const insertValues = [
            row_id, ledger_id, item_id, item_name, entry_date,
            total_qty, total_amount, created_date, system_name
          ];

          const [insertResult] = await pool.execute(insertSql, insertValues);

          result.action = 'insert';
          result.id = insertResult.insertId || row_id;
          result.message = 'Sales record created successfully.';
        }
      } catch (dbError) {
        // Catch database errors specific to this record
        result.message = `Database error: ${dbError.message}`;
      }

      // Add the outcome to the final results array
      results.push(result);
    }

    // 4. SEND AGGREGATE RESPONSE
    const insertedCount = results.filter(r => r.action === 'insert').length;
    const skippedCount = results.filter(r => r.action === 'noop').length;
    const failedCount = results.filter(r => r.action === 'error').length;

    res.status(200).json({
      success: failedCount === 0, // Overall success only if no records failed
      summary: {
        totalRecords: records.length,
        inserted: insertedCount,
        skipped: skippedCount,
        failed: failedCount
      },
      results: results,
      message: `Batch process complete. Inserted ${insertedCount} records, skipped ${skippedCount}, failed ${failedCount}.`
    });

  } catch (error) {
    // Catch global errors (e.g., body not an array, initial setup failure)
    next(error);
  }
};

exports.getLastRecordByType = async (req, res, next) => {
  try {
    // 1. Get and validate the table type from ROUTE PARAMETERS (req.params)
    const { type } = req.params;

    if (!type) {
      // check if the route is defined correctly, but kept for safety
      throw new AppError('Missing required route parameter for table type (must be "sales" or "loan").', 400, { isOperational: true });
    }

    // Define a lookup map to safely get the database table name
    const tableMap = {
      'sales': 'apcms_sales',
      'loan': 'apcms_loan',
    };

    const recordType = type.toLowerCase();
    const tableName = tableMap[recordType];

    // Validate if the requested type exists in the map
    if (!tableName) {
      const validTypes = Object.keys(tableMap).join(', ');
      throw new AppError(`Invalid type parameter in route. Must be one of: ${validTypes}.`, 400, { isOperational: true });
    }

    // 2. Define the SQL query using the determined table name.
    const sql = `
            SELECT * FROM ${tableName} 
            ORDER BY row_id DESC 
            LIMIT 1
        `;

    // 3. Execute the query
    const [rows] = await pool.execute(sql);

    // 4. Check if a record was found
    if (rows.length === 0) {
      // No records found in the table
      return res.status(404).json({
        success: true,
        message: `No ${recordType} records found in the database table (${tableName}).`,
        data: null
      });
    }

    // 5. Send the successful response
    const lastRecord = rows[0];

    res.status(200).json({
      success: true,
      message: `Successfully retrieved the last ${recordType} record from ${tableName}.`,
      data: lastRecord
    });

  } catch (error) {
    // Pass any database or system errors to the centralized error handler
    next(error);
  }
};

exports.getLoanRecordsByLedgerItemAndDate = async (req, res, next) => {
  try {
    // 1. Get parameters from the route (mandatory)
    const { ledgerNo, itemNo } = req.params;
    console.log(req.params);

    // 2. Get optional query parameters (date range)
    const { fromPeriod, toPeriod } = req.query; // Expects format YYYY-MM-DD
    console.log(req.query);

    // Determine which parameters should be used for filtering
    // If the parameter is 'all' (case-insensitive), we skip filtering on that column.
    const filterLedger = ledgerNo.toLowerCase() !== 'all';
    const filterItem = itemNo.toLowerCase() !== 'all';

    // 3. Construct the base SQL query and parameters dynamically
    let sql = `
            SELECT * FROM apcms_loan 
            WHERE 1=1 
        `;
    let params = [];

    if (filterLedger) {
      sql += ' AND ledger_id = ?';
      params.push(ledgerNo);
    }

    if (filterItem) {
      sql += ' AND item_id = ?';
      params.push(itemNo);
    }

    // 4. Conditionally add date range filtering
    const dateFormatRegex = /^\d{4}-\d{2}-\d{2}$/;
    let dateRangeMsg = '';

    if (fromPeriod) {
      if (!dateFormatRegex.test(fromPeriod)) {
        throw new AppError('Invalid fromPeriod format. Use YYYY-MM-DD.', 400, { isOperational: true });
      }
      sql += ' AND entry_date >= ?'; // Filter records starting from this date
      params.push(fromPeriod);
      dateRangeMsg += ` from ${fromPeriod}`;
    }

    if (toPeriod) {
      if (!dateFormatRegex.test(toPeriod)) {
        throw new AppError('Invalid toPeriod format. Use YYYY-MM-DD.', 400, { isOperational: true });
      }
      sql += ' AND entry_date <= ?'; // Filter records up to this date
      params.push(toPeriod);
      dateRangeMsg += ` to ${toPeriod}`;
    }

    // 5. Add final sorting for ledger reporting (usually by date ascending)
    sql += ' ORDER BY entry_date ASC';

    // 6. Execute the query
    const [rows] = await pool.execute(sql, params);

    // 7. Send the successful response
    if (rows.length === 0) {
      return res.status(404).json({
        success: true,
        message: 'No loan records found matching the criteria.',
        data: []
      });
    }

    // Determine the message based on filters applied
    const ledgerMsg = filterLedger ? `Ledger ${ledgerNo}` : 'All Ledgers';
    const itemMsg = filterItem ? `Item ${itemNo}` : 'All Items';
    // Use the built dateRangeMsg

    res.status(200).json({
      success: true,
      message: `Found ${rows.length} loan records for ${ledgerMsg} and ${itemMsg}${dateRangeMsg}.`,
      data: rows
    });

  } catch (error) {
    // Pass any errors to the centralized error handler
    next(error);
  }
};




exports.getLoan = async (req, res, next) => {
  try {
    const conditions = [];
    const params = [];

    const addCondition = (sql, value, wildcard = false) => {
      conditions.push(sql);
      params.push(wildcard ? `%${value}%` : value);
    };

    const { ledger_id, item_id, from_entryDate, to_entryDate } = req.query;

    if (ledger_id) addCondition('apcms_loan.ledger_id = ?', ledger_id);
    if (item_id) addCondition('apcms_loan.item_id = ?', item_id);

    if (from_entryDate) {
      addCondition('apcms_loan.entry_date >= ?', from_entryDate + ' 00:00:00');
    }

    if (to_entryDate) {
      addCondition('apcms_loan.entry_date <= ?', to_entryDate + ' 23:59:59');
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const countQuery = `SELECT COUNT(*) AS total FROM apcms_loan ${whereClause}`;
    const [countResults] = await pool.execute(countQuery, params);
    const total = countResults[0].total;

    if (total === 0) {
      return res.status(200).json({ data: [], total: 0 });
    }

    const query = `
      SELECT *
      FROM apcms_loan
      ${whereClause}
    `;

    const [results] = await pool.execute(query, [...params]);
    res.status(200).json({
      data: results,
      total,
    });
  } catch (error) {
    next(error);
  }
};




exports.getSales = async (req, res, next) => {
  try {
    const conditions = [];
    const params = [];

    const addCondition = (sql, value, wildcard = false) => {
      conditions.push(sql);
      params.push(wildcard ? `%${value}%` : value);
    };

    const { ledger_id, item_id, from_entryDate, to_entryDate } = req.query;
    
    if (ledger_id) addCondition('apcms_sales.ledger_id = ?', ledger_id);
    if (item_id) addCondition('apcms_sales.item_id = ?', item_id);

    if (from_entryDate) {
      addCondition('apcms_sales.entry_date >= ?', from_entryDate + ' 00:00:00');
    }

    if (to_entryDate) {
      addCondition('apcms_sales.entry_date <= ?', to_entryDate + ' 23:59:59');
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const countQuery = `SELECT COUNT(*) AS total FROM apcms_sales ${whereClause}`;
    const [countResults] = await pool.execute(countQuery, params);
    const total = countResults[0].total;

    if (total === 0) {
      return res.status(200).json({ data: [], total: 0 });
    }

    const query = `
      SELECT *
      FROM apcms_sales
      ${whereClause}
    `;

    const [results] = await pool.execute(query, [...params]);
    res.status(200).json({
      data: results,
      total,
    });
  } catch (error) {
    next(error);
  }
};
