const express = require('express');
const pool = require('../db');
const AppError = require('../utils/AppError');
const router = express.Router();

////////////////////////////////////////////////////
////////////////// NEW APIS ////////////////////////

// simple required-field check
const requireFields = (body, fields) => {
  const missing = fields.filter(f => body[f] === undefined || body[f] === null);
  return missing;
};

exports.addItemMaster = async (req, res, next) => {
  try {
    const body = req.body;

    // 1. Define required fields (removed item_id)
    const requiredFields = [
      'item_code',
      'category',
      'item_name'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(
        `Missing required fields: ${missing.join(', ')}`,
        400,
        { isOperational: true }
      );
    }

    const { item_code, category, item_name } = body;

    // 2. Check if the item_code exists (This is now our unique key)
    const [rows] = await pool.execute(
      'SELECT id FROM apcms_item_master WHERE item_code = ?',
      [item_code]
    );

    let action;

    if (rows.length === 0) {
      // 3. INSERT - New Item
      action = 'insert';
      const insertSql = `
        INSERT INTO apcms_item_master (
          item_code,
          category,
          item_name
        ) VALUES (?, ?, ?)
      `;
      await pool.execute(insertSql, [item_code, category, item_name]);
    } else {
      // 4. UPDATE - Existing Item
      action = 'update';
      const updateSql = `
        UPDATE apcms_item_master
        SET
          category = ?,
          item_name = ?
        WHERE item_code = ?
      `;
      await pool.execute(updateSql, [category, item_name, item_code]);
    }

    // 5. FETCH the fresh record safely
    const [freshRecord] = await pool.execute(
      'SELECT item_code, item_name, category FROM apcms_item_master WHERE item_code = ?',
      [item_code]
    );

    // Guard Clause to prevent crash if record is missing
    if (!freshRecord || freshRecord.length === 0) {
      throw new AppError('Item processed but could not be retrieved from database.', 500);
    }

    const record = freshRecord[0];

    // 6. Send Clean Response
    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      message: `Item master record ${action === 'insert' ? 'created' : 'updated'} successfully`,
      data: {
        item_code: record.item_code,
        item_name: record.item_name,
        category: record.category
      }
    });
  } catch (error) {
    next(error);
  }
};

exports.listAllItems = async (req, res, next) => {
  try {
    // SQL query to select all columns and all rows from the item master table.
    const sql = 'SELECT * FROM apcms_item_master';

    // Execute the query.
    // The result is an array containing the row data and metadata, we only need the rows.
    const [rows] = await pool.execute(sql);

    // Check if any records were found
    if (rows.length === 0) {
      // If no items are found, return a 200 OK status with an empty data array and a specific message.
      return res.status(200).json({
        success: true,
        count: 0,
        data: [],
        message: 'No item master records found.'
      });
    }

    // If records are found, send a 200 OK status with the data array.
    res.status(200).json({
      success: true,
      count: rows.length,
      data: rows,
      message: 'Successfully retrieved all item master records.'
    });
  } catch (error) {
    // Catch any database or internal errors and pass them to the Express error handler.
    next(error);
  }
};

exports.addSocietyMaster = async (req, res, next) => {
  try {
    const body = req.body;

    // 1. Define only the fields actually expected from the frontend/body
    const requiredFields = [
      'society_name',
      'society_code'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(
        `Missing required fields: ${missing.join(', ')}`,
        400,
        { isOperational: true }
      );
    }

    const { society_name, society_code } = body;

    // 2. Check if the society_code already exists
    const [rows] = await pool.execute(
      'SELECT id FROM apcms_society_master WHERE society_code = ?',
      [society_code]
    );

    let action;

    if (rows.length === 0) {
      // 3. INSERT - New Society (Status defaults to 'active')
      action = 'insert';
      const insertSql = `
        INSERT INTO apcms_society_master (
          society_name,
          society_code,
          status
        ) VALUES (?, ?, 'active')
      `;

      const insertValues = [society_name, society_code];
      await pool.execute(insertSql, insertValues);
      
    } else {
      // 4. UPDATE - Existing Society 
      action = 'update';
      const updateSql = `
        UPDATE apcms_society_master
        SET
          society_name = ?
        WHERE society_code = ?
      `;

      const updateValues = [society_name, society_code];
      await pool.execute(updateSql, updateValues);
    }

    // 2. FETCH the fresh record from the database
    // This ensures we return the actual DB state (ID, Timestamps, Status)
    const [freshRecord] = await pool.execute(
      'SELECT * FROM apcms_society_master WHERE society_code = ?',
      [society_code]
    );

    // 4. Extract data safely
    const record = freshRecord[0];

    if (!record) {
      throw new AppError('Data consistency error: Record not found after save.', 500);
    }
    
    // 5. Send Response
    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      message: `Society record ${action === 'insert' ? 'created' : 'updated'} successfully`,
      data: {
        society_code: record.society_code,
        society_name: record.society_name,
        status: record.status,
      }
    });
  } catch (error) {
    next(error);
  }
};

exports.listSocietyMaster = async (req, res, next) => {
  try {
    // 1. SQL query to select societies. 
    // We add 'ORDER BY' to ensure the dropdowns in the portal look organized.
    const sql = 'SELECT id, society_name, society_code, status FROM apcms_society_master ORDER BY society_name ASC';

    // 2. Execute the query
    const [rows] = await pool.execute(sql);

    // 3. Check if any records were found
    if (rows.length === 0) {
      return res.status(200).json({
        success: true,
        count: 0,
        data: [],
        message: 'No society master records found.'
      });
    }

    // 4. Send successful response
    res.status(200).json({
      success: true,
      count: rows.length,
      data: rows,
      message: 'Successfully retrieved all society master records.'
    });
  } catch (error) {
    // Pass any errors (like DB connection issues) to the global error handler
    next(error);
  }
};

exports.addMemberLoanDeposit = async (req, res, next) => {
  try {
    const body = req.body;

    // 1. Validate Mandatory Fields
    const requiredFields = [
      'society_id', 
      'entry_date', 
      'acc_type', 
      'acc_sub_type', 
      'item_id'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(`Missing required fields: ${missing.join(', ')}`, 400);
    }

    const {
      society_id, entry_date, acc_type, acc_sub_type, item_id,
      opening_amount = 0, payment_amount = 0, receipt_amount = 0, balance_amount = 0,
      p_opening_qty = 0, p_issued_qty = 0, p_received_qty = 0, p_balance_qty = 0
    } = body;

    // 2. CHECK for existing record based on the 5-column unique combination
    const checkSql = `
      SELECT id FROM apcms_member_loan_deposit 
      WHERE entry_date = ? AND society_id = ? AND acc_type = ? AND acc_sub_type = ? AND item_id = ?
    `;
    const [existing] = await pool.execute(checkSql, [entry_date, society_id, acc_type, acc_sub_type, item_id]);

    let action;
    let finalId;

    if (existing.length === 0) {
      // 3. INSERT Logic
      action = 'insert';
      const insertSql = `
        INSERT INTO apcms_member_loan_deposit (
          acc_type, acc_sub_type, item_id, society_id, entry_date,
          opening_amount, payment_amount, receipt_amount, balance_amount,
          p_opening_qty, p_issued_qty, p_received_qty, p_balance_qty
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;
      const [insertResult] = await pool.execute(insertSql, [
        acc_type, acc_sub_type, item_id, society_id, entry_date,
        opening_amount, payment_amount, receipt_amount, balance_amount,
        p_opening_qty, p_issued_qty, p_received_qty, p_balance_qty
      ]);
      finalId = insertResult.insertId;
    } else {
      // 4. UPDATE Logic
      action = 'update';
      finalId = existing[0].id;
      const updateSql = `
        UPDATE apcms_member_loan_deposit SET
          opening_amount = ?, payment_amount = ?, receipt_amount = ?, balance_amount = ?,
          p_opening_qty = ?, p_issued_qty = ?, p_received_qty = ?, p_balance_qty = ?
        WHERE id = ?
      `;
      await pool.execute(updateSql, [
        opening_amount, payment_amount, receipt_amount, balance_amount,
        p_opening_qty, p_issued_qty, p_received_qty, p_balance_qty,
        finalId
      ]);
    }

    // 5. FETCH the fresh state to return to frontend
    const [freshRecord] = await pool.execute(
      'SELECT * FROM apcms_member_loan_deposit WHERE id = ?',
      [finalId]
    );

    if (!freshRecord.length) {
      throw new AppError('Error retrieving record after save.', 500);
    }

    // 6. Response
    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      message: `${acc_type} entry ${action}ed successfully`,
      data: freshRecord[0]
    });

  } catch (error) {
    next(error);
  }
};

exports.addMultipleMemberLoanDeposits = async (req, res, next) => {
    try {
        const records = req.body;

        // 1. Validate that the input is an array
        if (!Array.isArray(records)) {
            throw new AppError('Payload must be an array of records.', 400);
        }

        const requiredFields = [
            'society_id', 
            'entry_date', 
            'acc_type', 
            'acc_sub_type', 
            'item_id'
        ];

        const results = [];

        // 2. Process each record in the array
        for (const body of records) {
            const rowResult = { 
                identifier: body.entry_date,
                action: 'error', 
                message: 'Pending processing' 
            };

            try {
                // A) Validate Mandatory Fields for this specific row
                const missing = requireFields(body, requiredFields);
                if (missing.length) {
                    rowResult.message = `Missing fields: ${missing.join(', ')}`;
                    results.push(rowResult);
                    continue;
                }

                const {
                    society_id, entry_date, acc_type, acc_sub_type, item_id,
                    opening_amount = 0, payment_amount = 0, receipt_amount = 0, balance_amount = 0,
                    p_opening_qty = 0, p_issued_qty = 0, p_received_qty = 0, p_balance_qty = 0
                } = body;

                // B) CHECK for existing record based on the 5-column unique combination
                const checkSql = `
                    SELECT id FROM apcms_member_loan_deposit 
                    WHERE entry_date = ? AND society_id = ? AND acc_type = ? AND acc_sub_type = ? AND item_id = ?
                `;
                const [existing] = await pool.execute(checkSql, [entry_date, society_id, acc_type, acc_sub_type, item_id]);

                let finalId;
                let currentAction;

                if (existing.length === 0) {
                    // C) INSERT Logic
                    currentAction = 'insert';
                    const insertSql = `
                        INSERT INTO apcms_member_loan_deposit (
                            acc_type, acc_sub_type, item_id, society_id, entry_date,
                            opening_amount, payment_amount, receipt_amount, balance_amount,
                            p_opening_qty, p_issued_qty, p_received_qty, p_balance_qty
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `;
                    const [insertResult] = await pool.execute(insertSql, [
                        acc_type, acc_sub_type, item_id, society_id, entry_date,
                        opening_amount, payment_amount, receipt_amount, balance_amount,
                        p_opening_qty, p_issued_qty, p_received_qty, p_balance_qty
                    ]);
                    finalId = insertResult.insertId;
                } else {
                    // D) UPDATE Logic
                    currentAction = 'update';
                    finalId = existing[0].id;
                    const updateSql = `
                        UPDATE apcms_member_loan_deposit SET
                            opening_amount = ?, payment_amount = ?, receipt_amount = ?, balance_amount = ?,
                            p_opening_qty = ?, p_issued_qty = ?, p_received_qty = ?, p_balance_qty = ?
                        WHERE id = ?
                    `;
                    await pool.execute(updateSql, [
                        opening_amount, payment_amount, receipt_amount, balance_amount,
                        p_opening_qty, p_issued_qty, p_received_qty, p_balance_qty,
                        finalId
                    ]);
                }

                rowResult.action = currentAction;
                rowResult.id = finalId;
                rowResult.message = `Successfully ${currentAction}ed.`;

            } catch (rowError) {
                rowResult.action = 'error';
                rowResult.message = rowError.message;
            }

            results.push(rowResult);
        }

        // 3. Summarize outcomes
        const summary = {
            total: records.length,
            inserted: results.filter(r => r.action === 'insert').length,
            updated: results.filter(r => r.action === 'update').length,
            failed: results.filter(r => r.action === 'error').length
        };

        // 4. Final Response
        res.status(200).json({
            success: summary.failed === 0,
            summary,
            results
        });

    } catch (error) {
        next(error);
    }
};

exports.oldgetMemberLoanDeposit = async (req, res, next) => {
  try {
    // 1. Get the type from the URL parameter (e.g., /api/ledger/LOAN)
    const { type } = req.params;

    // 2. Validate that the type matches your database ENUM
    const allowedTypes = ['MEMBER', 'LOAN', 'DEPOSIT'];
    if (!allowedTypes.includes(type.toUpperCase())) {
      throw new AppError(
        `Invalid type. Please use one of: ${allowedTypes.join(', ')}`,
        400
      );
    }

    // 3. SQL Query with JOINs to get Society and Item names instead of just IDs
    // This makes the data much more useful for your Kooturavu Santhai frontend.
    const sql = `
  SELECT 
    l.id,
    l.society_id,
    s.society_name,
    l.entry_date,
    l.acc_type,
    l.acc_sub_type,
    l.item_id,
    i.item_name,
    l.opening_amount,
    l.payment_amount,
    l.receipt_amount,
    l.balance_amount,
    l.p_opening_qty,
    l.p_issued_qty,
    l.p_received_qty,
    l.p_balance_qty,
    l.created_at
  FROM apcms_member_loan_deposit l
  LEFT JOIN apcms_society_master s ON l.society_id = s.id
  LEFT JOIN apcms_item_master i ON l.item_id = i.id
  WHERE l.acc_type = ?
  ORDER BY l.entry_date DESC
    `;

    const [rows] = await pool.execute(sql, [type.toUpperCase()]);

    // 4. Handle empty results
    if (rows.length === 0) {
      return res.status(200).json({
        success: true,
        count: 0,
        data: [],
        message: `No records found for account type: ${type}`
      });
    }

    // 5. Send Response
    res.status(200).json({
      success: true,
      count: rows.length,
      data: rows
    });
  } catch (error) {
    next(error);
  }
};

exports.getMemberLoanDeposit = async (req, res, next) => {
  try {
    // 1. Get mandatory type from URL parameter
    const { type } = req.params;
    
    // 2. Get optional date filters from query params
    const { startdate, enddate } = req.query;

    // 3. Validate type matches database ENUM
    const allowedTypes = ['MEMBER', 'LOAN', 'DEPOSIT'];
    const upperType = type.toUpperCase();
    if (!allowedTypes.includes(upperType)) {
      throw new AppError(
        `Invalid type. Please use one of: ${allowedTypes.join(', ')}`,
        400
      );
    }

    // 4. Build dynamic WHERE clause and parameters
    let whereConditions = ['l.acc_type = ?'];
    let params = [upperType];

    // 5. Add date filters if provided
    if (startdate) {
      whereConditions.push('l.entry_date >= ?');
      params.push(startdate);
    }
    
    if (enddate) {
      whereConditions.push('l.entry_date <= ?');
      params.push(enddate);
    }

    // 6. SQL Query with dynamic WHERE clause
    const sql = `
      SELECT
        l.id,
        l.society_id,
        s.society_name,
        l.entry_date,
        l.acc_type,
        l.acc_sub_type,
        l.item_id,
        i.item_name,
        l.opening_amount,
        l.payment_amount,
        l.receipt_amount,
        l.balance_amount,
        l.p_opening_qty,
        l.p_issued_qty,
        l.p_received_qty,
        l.p_balance_qty,
        l.created_at
      FROM apcms_member_loan_deposit l
      LEFT JOIN apcms_society_master s ON l.society_id = s.id
      LEFT JOIN apcms_item_master i ON l.item_id = i.id
      WHERE ${whereConditions.join(' AND ')}
      ORDER BY l.entry_date DESC
    `;

    const [rows] = await pool.execute(sql, params);

    // 7. Handle empty results
    if (rows.length === 0) {
      return res.status(200).json({
        success: true,
        count: 0,
        data: [],
        filters: {
          type: upperType,
          startdate: startdate || null,
          enddate: enddate || null
        },
        message: `No records found for account type: ${upperType}${startdate || enddate ? ' with specified date range' : ''}`
      });
    }

    // 8. Send Response
    res.status(200).json({
      success: true,
      count: rows.length,
      filters: {
        type: upperType,
        startdate: startdate || null,
        enddate: enddate || null
      },
      data: rows
    });

  } catch (error) {
    next(error);
  }
};



exports.addSalesPurchase = async (req, res, next) => {
  try {
    const body = req.body;

    // 1. Validate Mandatory Fields
    const requiredFields = [
      'acc_type',
      'acc_sub_type',
      'item_id',
      'society_id',
      'entry_date'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(`Missing required fields: ${missing.join(', ')}`, 400);
    }

    const {
      acc_type,
      acc_sub_type,
      item_id,
      society_id,
      entry_date,
      total_qty = 0,
      total_amount = 0
    } = body;

    // 2. CHECK for existing record based on the 5-column unique combination
    const checkSql = `
      SELECT id FROM apcms_sales_purchase 
      WHERE entry_date = ? AND society_id = ? AND acc_type = ? AND acc_sub_type = ? AND item_id = ?
    `;
    const [existing] = await pool.execute(checkSql, [
      entry_date, 
      society_id, 
      acc_type, 
      acc_sub_type, 
      item_id
    ]);

    let action;
    let finalId;

    if (existing.length === 0) {
      // 3. INSERT Logic
      action = 'insert';
      const insertSql = `
        INSERT INTO apcms_sales_purchase (
          acc_type, 
          acc_sub_type, 
          item_id, 
          society_id, 
          entry_date, 
          total_qty, 
          total_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `;
      const [insertResult] = await pool.execute(insertSql, [
        acc_type,
        acc_sub_type,
        item_id,
        society_id,
        entry_date,
        total_qty,
        total_amount
      ]);
      finalId = insertResult.insertId;
    } else {
      // 4. UPDATE Logic
      action = 'update';
      finalId = existing[0].id;
      const updateSql = `
        UPDATE apcms_sales_purchase SET
          total_qty = ?, 
          total_amount = ?
        WHERE id = ?
      `;
      await pool.execute(updateSql, [total_qty, total_amount, finalId]);
    }

    // 5. FETCH the fresh state with Joins (to return names to frontend)
    const fetchSql = `
      SELECT 
        t.id,
        t.society_id,
        s.society_name,
        t.entry_date,
        t.acc_type,
        t.acc_sub_type,
        t.item_id,
        i.item_name,
        t.total_qty,
        t.total_amount,
        t.created_at
      FROM apcms_sales_purchase t
      LEFT JOIN apcms_society_master s ON t.society_id = s.id
      LEFT JOIN apcms_item_master i ON t.item_id = i.id
      WHERE t.id = ?
    `;
    const [freshRecord] = await pool.execute(fetchSql, [finalId]);

    if (!freshRecord.length) {
      throw new AppError('Error retrieving record after save.', 500);
    }

    // 6. Response
    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      message: `${acc_type} record ${action}ed successfully`,
      data: freshRecord[0]
    });

  } catch (error) {
    next(error);
  }
};

exports.addMultipleSalesPurchases = async (req, res, next) => {
    try {
        const records = req.body;

        // 1. Validate that the input is an array
        if (!Array.isArray(records)) {
            throw new AppError('Payload must be an array of records.', 400);
        }

        const requiredFields = [
            'acc_type',
            'acc_sub_type',
            'item_id',
            'society_id',
            'entry_date'
        ];

        const results = [];

        // 2. Process each record in the array
        for (const body of records) {
            const rowResult = { 
                entry_date: body.entry_date,
                item_id: body.item_id,
                action: 'error', 
                message: 'Pending processing' 
            };

            try {
                // A) Per-Record Validation
                const missing = requireFields(body, requiredFields);
                if (missing.length) {
                    rowResult.message = `Validation failed: Missing fields: ${missing.join(', ')}`;
                    results.push(rowResult);
                    continue; 
                }

                const {
                    acc_type,
                    acc_sub_type,
                    item_id,
                    society_id,
                    entry_date,
                    total_qty = 0,
                    total_amount = 0
                } = body;

                // B) CHECK for existing record based on the 5-column unique combination
                const checkSql = `
                    SELECT id FROM apcms_sales_purchase 
                    WHERE entry_date = ? AND society_id = ? AND acc_type = ? AND acc_sub_type = ? AND item_id = ?
                `;
                const [existing] = await pool.execute(checkSql, [
                    entry_date, 
                    society_id, 
                    acc_type, 
                    acc_sub_type, 
                    item_id
                ]);

                let finalId;
                let currentAction;

                if (existing.length === 0) {
                    // C) INSERT Logic
                    currentAction = 'insert';
                    const insertSql = `
                        INSERT INTO apcms_sales_purchase (
                            acc_type, acc_sub_type, item_id, society_id, entry_date, 
                            total_qty, total_amount
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    `;
                    const [insertResult] = await pool.execute(insertSql, [
                        acc_type, acc_sub_type, item_id, society_id, entry_date, 
                        total_qty, total_amount
                    ]);
                    finalId = insertResult.insertId;
                } else {
                    // D) UPDATE Logic
                    currentAction = 'update';
                    finalId = existing[0].id;
                    const updateSql = `
                        UPDATE apcms_sales_purchase SET
                            total_qty = ?, 
                            total_amount = ?
                        WHERE id = ?
                    `;
                    await pool.execute(updateSql, [total_qty, total_amount, finalId]);
                }

                rowResult.action = currentAction;
                rowResult.id = finalId;
                rowResult.message = `Successfully ${currentAction}ed.`;

            } catch (rowError) {
                rowResult.action = 'error';
                rowResult.message = `Database Error: ${rowError.message}`;
            }

            results.push(rowResult);
        }

        // 3. Summarize outcomes
        const summary = {
            total: records.length,
            inserted: results.filter(r => r.action === 'insert').length,
            updated: results.filter(r => r.action === 'update').length,
            failed: results.filter(r => r.action === 'error').length
        };

        // 4. Final Response
        res.status(200).json({
            success: summary.failed === 0,
            summary,
            message: `Processed ${summary.total} records: ${summary.inserted} inserted, ${summary.updated} updated, ${summary.failed} failed.`,
            results
        });

    } catch (error) {
        next(error);
    }
};

exports.getSalesPurchase = async (req, res, next) => {
  try {
    // 1. Get the type from the URL parameter (e.g., /api/sales-purchase/SALES)
    const { type } = req.params;

    // 2. Validate against the SALES/PURCHASE ENUM
    const allowedTypes = ['SALES', 'PURCHASE'];
    if (!type || !allowedTypes.includes(type.toUpperCase())) {
      throw new AppError(
        `Invalid type. Please use one of: ${allowedTypes.join(', ')}`,
        400
      );
    }

    // 3. SQL Query with JOINs
    // Columns reordered: Name fields follow their respective ID fields
    const sql = `
      SELECT 
        t.id,
        t.society_id,
        s.society_name,
        t.entry_date,
        t.acc_type,
        t.acc_sub_type,
        t.item_id,
        i.item_name,
        t.total_qty,
        t.total_amount,
        t.created_at
      FROM apcms_sales_purchase t
      LEFT JOIN apcms_society_master s ON t.society_id = s.id
      LEFT JOIN apcms_item_master i ON t.item_id = i.id
      WHERE t.acc_type = ?
      ORDER BY t.entry_date DESC
    `;

    const [rows] = await pool.execute(sql, [type.toUpperCase()]);

    // 4. Handle empty results
    if (rows.length === 0) {
      return res.status(200).json({
        success: true,
        count: 0,
        data: [],
        message: `No records found for account type: ${type}`
      });
    }

    // 5. Send Response
    res.status(200).json({
      success: true,
      count: rows.length,
      data: rows
    });
  } catch (error) {
    next(error);
  }
};

exports.addMarketing = async (req, res, next) => {
  try {
    const body = req.body;

    // 1. Validate Mandatory Fields
    const requiredFields = [
      'acc_type',
      'acc_sub_type',
      'item_id',
      'society_id',
      'entry_date'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(`Missing required fields: ${missing.join(', ')}`, 400);
    }

    const {
      acc_type,
      acc_sub_type,
      item_id,
      society_id,
      entry_date,
      no_of_lots = 0,
      total_qty = 0,
      total_amount = 0,
      service_amount = 0
    } = body;

    // 2. CHECK for existing record based on the 5-column unique combination
    const checkSql = `
      SELECT id FROM apcms_marketing 
      WHERE entry_date = ? AND society_id = ? AND acc_type = ? AND acc_sub_type = ? AND item_id = ?
    `;
    const [existing] = await pool.execute(checkSql, [
      entry_date, 
      society_id, 
      acc_type, 
      acc_sub_type, 
      item_id
    ]);

    let action;
    let finalId;

    if (existing.length === 0) {
      // 3. INSERT Logic
      action = 'insert';
      const insertSql = `
        INSERT INTO apcms_marketing (
          acc_type, 
          acc_sub_type, 
          item_id, 
          society_id, 
          entry_date, 
          no_of_lots,
          total_qty, 
          total_amount,
          service_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;
      const [insertResult] = await pool.execute(insertSql, [
        acc_type,
        acc_sub_type,
        item_id,
        society_id,
        entry_date,
        no_of_lots,
        total_qty,
        total_amount,
        service_amount
      ]);
      finalId = insertResult.insertId;
    } else {
      // 4. UPDATE Logic
      action = 'update';
      finalId = existing[0].id;
      const updateSql = `
        UPDATE apcms_marketing SET
          no_of_lots = ?,
          total_qty = ?, 
          total_amount = ?,
          service_amount = ?
        WHERE id = ?
      `;
      await pool.execute(updateSql, [
        no_of_lots, 
        total_qty, 
        total_amount, 
        service_amount, 
        finalId
      ]);
    }

    // 5. FETCH the fresh state with Joins (to return names to frontend)
    const fetchSql = `
      SELECT 
        m.id,
        m.society_id,
        s.society_name,
        m.entry_date,
        m.acc_type,
        m.acc_sub_type,
        m.item_id,
        i.item_name,
        m.no_of_lots,
        m.total_qty,
        m.total_amount,
        m.service_amount,
        m.created_at
      FROM apcms_marketing m
      LEFT JOIN apcms_society_master s ON m.society_id = s.id
      LEFT JOIN apcms_item_master i ON m.item_id = i.id
      WHERE m.id = ?
    `;
    const [freshRecord] = await pool.execute(fetchSql, [finalId]);

    if (!freshRecord.length) {
      throw new AppError('Error retrieving marketing record after save.', 500);
    }

    // 6. Response
    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      message: `Marketing record ${action}ed successfully`,
      data: freshRecord[0]
    });

  } catch (error) {
    next(error);
  }
};

exports.addMultipleMarketingRecords = async (req, res, next) => {
    try {
        const records = req.body;

        // 1. Validate that the input is an array
        if (!Array.isArray(records)) {
            throw new AppError('Payload must be an array of records.', 400);
        }

        const requiredFields = [
            'acc_type',
            'acc_sub_type',
            'item_id',
            'society_id',
            'entry_date'
        ];

        const results = [];

        // 2. Process each record in the array
        for (const body of records) {
            const rowResult = { 
                entry_date: body.entry_date,
                item_id: body.item_id,
                action: 'error', 
                message: 'Pending processing' 
            };

            try {
                // A) Validation for current record
                const missing = requireFields(body, requiredFields);
                if (missing.length) {
                    rowResult.message = `Missing fields: ${missing.join(', ')}`;
                    results.push(rowResult);
                    continue; 
                }

                const {
                    acc_type,
                    acc_sub_type,
                    item_id,
                    society_id,
                    entry_date,
                    no_of_lots = 0,
                    total_qty = 0,
                    total_amount = 0,
                    service_amount = 0
                } = body;

                // B) CHECK for existing record based on the 5-column unique combination
                const checkSql = `
                    SELECT id FROM apcms_marketing 
                    WHERE entry_date = ? AND society_id = ? AND acc_type = ? AND acc_sub_type = ? AND item_id = ?
                `;
                const [existing] = await pool.execute(checkSql, [
                    entry_date, 
                    society_id, 
                    acc_type, 
                    acc_sub_type, 
                    item_id
                ]);

                let finalId;
                let currentAction;

                if (existing.length === 0) {
                    // C) INSERT Logic
                    currentAction = 'insert';
                    const insertSql = `
                        INSERT INTO apcms_marketing (
                            acc_type, 
                            acc_sub_type, 
                            item_id, 
                            society_id, 
                            entry_date, 
                            no_of_lots,
                            total_qty, 
                            total_amount,
                            service_amount
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `;
                    const [insertResult] = await pool.execute(insertSql, [
                        acc_type,
                        acc_sub_type,
                        item_id,
                        society_id,
                        entry_date,
                        no_of_lots,
                        total_qty,
                        total_amount,
                        service_amount
                    ]);
                    finalId = insertResult.insertId;
                } else {
                    // D) UPDATE Logic
                    currentAction = 'update';
                    finalId = existing[0].id;
                    const updateSql = `
                        UPDATE apcms_marketing SET
                            no_of_lots = ?,
                            total_qty = ?, 
                            total_amount = ?,
                            service_amount = ?
                        WHERE id = ?
                    `;
                    await pool.execute(updateSql, [
                        no_of_lots, 
                        total_qty, 
                        total_amount, 
                        service_amount, 
                        finalId
                    ]);
                }

                rowResult.action = currentAction;
                rowResult.id = finalId;
                rowResult.message = `Successfully ${currentAction}ed.`;

            } catch (rowError) {
                rowResult.action = 'error';
                rowResult.message = `Database Error: ${rowError.message}`;
            }

            results.push(rowResult);
        }

        // 3. Summarize results for the response
        const summary = {
            total: records.length,
            inserted: results.filter(r => r.action === 'insert').length,
            updated: results.filter(r => r.action === 'update').length,
            failed: results.filter(r => r.action === 'error').length
        };

        // 4. Final Response
        res.status(200).json({
            success: summary.failed === 0,
            summary,
            message: `Batch complete: ${summary.inserted} inserted, ${summary.updated} updated, ${summary.failed} failed.`,
            results
        });

    } catch (error) {
        // Pass any system errors to the error handler
        next(error);
    }
};

exports.getMarketing = async (req, res, next) => {
  try {
    // 1. SQL Query with JOINs
    // Fetches all marketing records, joining with master tables for names
    const sql = `
      SELECT 
        m.id,
        m.society_id,
        s.society_name,
        m.entry_date,
        m.acc_type,
        m.acc_sub_type,
        m.item_id,
        i.item_name,
        m.no_of_lots,
        m.total_qty,
        m.total_amount,
        m.service_amount,
        m.created_at
      FROM apcms_marketing m
      LEFT JOIN apcms_society_master s ON m.society_id = s.id
      LEFT JOIN apcms_item_master i ON m.item_id = i.id
      ORDER BY m.entry_date DESC
    `;

    const [rows] = await pool.execute(sql);

    // 2. Handle empty results
    if (rows.length === 0) {
      return res.status(200).json({
        success: true,
        count: 0,
        data: [],
        message: "No marketing records found."
      });
    }

    // 3. Send Response
    res.status(200).json({
      success: true,
      count: rows.length,
      data: rows
    });
  } catch (error) {
    next(error);
  }
};

exports.addGodownUtilization = async (req, res, next) => {
  try {
    const body = req.body;

    // 1. Validate Mandatory Fields
    const requiredFields = [
      'acc_type',
      'acc_sub_type',
      'item_id',
      'society_id',
      'entry_date'
    ];

    const missing = requireFields(body, requiredFields);
    if (missing.length) {
      throw new AppError(`Missing required fields: ${missing.join(', ')}`, 400);
    }

    const {
      acc_type,
      acc_sub_type,
      item_id,
      society_id,
      entry_date,
      no_of_lots = 0,
      total_bag = 0,
      total_kgs = 0
    } = body;

    // 2. CHECK for existing record based on the 5-column unique combination
    const checkSql = `
      SELECT id FROM apcms_godown_utilization 
      WHERE entry_date = ? AND society_id = ? AND acc_type = ? AND acc_sub_type = ? AND item_id = ?
    `;
    const [existing] = await pool.execute(checkSql, [
      entry_date, 
      society_id, 
      acc_type, 
      acc_sub_type, 
      item_id
    ]);

    let action;
    let finalId;

    if (existing.length === 0) {
      // 3. INSERT Logic
      action = 'insert';
      const insertSql = `
        INSERT INTO apcms_godown_utilization (
          acc_type, 
          acc_sub_type, 
          item_id, 
          society_id, 
          entry_date, 
          no_of_lots,
          total_bag, 
          total_kgs
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `;
      const [insertResult] = await pool.execute(insertSql, [
        acc_type,
        acc_sub_type,
        item_id,
        society_id,
        entry_date,
        no_of_lots,
        total_bag,
        total_kgs
      ]);
      finalId = insertResult.insertId;
    } else {
      // 4. UPDATE Logic
      action = 'update';
      finalId = existing[0].id;
      const updateSql = `
        UPDATE apcms_godown_utilization SET
          no_of_lots = ?,
          total_bag = ?, 
          total_kgs = ?
        WHERE id = ?
      `;
      await pool.execute(updateSql, [
        no_of_lots, 
        total_bag, 
        total_kgs, 
        finalId
      ]);
    }

    // 5. FETCH the fresh state with Joins
    const fetchSql = `
      SELECT 
        g.id,
        g.society_id,
        s.society_name,
        g.entry_date,
        g.acc_type,
        g.acc_sub_type,
        g.item_id,
        i.item_name,
        g.no_of_lots,
        g.total_bag,
        g.total_kgs,
        g.created_at
      FROM apcms_godown_utilization g
      LEFT JOIN apcms_society_master s ON g.society_id = s.id
      LEFT JOIN apcms_item_master i ON g.item_id = i.id
      WHERE g.id = ?
    `;
    const [freshRecord] = await pool.execute(fetchSql, [finalId]);

    if (!freshRecord.length) {
      throw new AppError('Error retrieving godown record after save.', 500);
    }

    // 6. Response
    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      message: `Godown utilization record ${action}ed successfully`,
      data: freshRecord[0]
    });

  } catch (error) {
    next(error);
  }
};

exports.addMultipleGodownUtilizations = async (req, res, next) => {
    try {
        const records = req.body;

        // 1. Validate that the input is an array
        if (!Array.isArray(records)) {
            throw new AppError('Payload must be an array of records.', 400);
        }

        const requiredFields = [
            'acc_type',
            'acc_sub_type',
            'item_id',
            'society_id',
            'entry_date'
        ];

        const results = [];

        // 2. Process each record in the array
        for (const body of records) {
            const rowResult = { 
                entry_date: body.entry_date,
                item_id: body.item_id,
                action: 'error', 
                message: 'Pending processing' 
            };

            try {
                // A) Validation for individual record
                const missing = requireFields(body, requiredFields);
                if (missing.length) {
                    rowResult.message = `Missing fields: ${missing.join(', ')}`;
                    results.push(rowResult);
                    continue; 
                }

                const {
                    acc_type,
                    acc_sub_type,
                    item_id,
                    society_id,
                    entry_date,
                    no_of_lots = 0,
                    total_bag = 0,
                    total_kgs = 0
                } = body;

                // B) CHECK for existing record based on the 5-column unique combination
                const checkSql = `
                    SELECT id FROM apcms_godown_utilization 
                    WHERE entry_date = ? AND society_id = ? AND acc_type = ? AND acc_sub_type = ? AND item_id = ?
                `;
                const [existing] = await pool.execute(checkSql, [
                    entry_date, 
                    society_id, 
                    acc_type, 
                    acc_sub_type, 
                    item_id
                ]);

                let finalId;
                let currentAction;

                if (existing.length === 0) {
                    // C) INSERT Logic
                    currentAction = 'insert';
                    const insertSql = `
                        INSERT INTO apcms_godown_utilization (
                            acc_type, 
                            acc_sub_type, 
                            item_id, 
                            society_id, 
                            entry_date, 
                            no_of_lots,
                            total_bag, 
                            total_kgs
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    `;
                    const [insertResult] = await pool.execute(insertSql, [
                        acc_type,
                        acc_sub_type,
                        item_id,
                        society_id,
                        entry_date,
                        no_of_lots,
                        total_bag,
                        total_kgs
                    ]);
                    finalId = insertResult.insertId;
                } else {
                    // D) UPDATE Logic
                    currentAction = 'update';
                    finalId = existing[0].id;
                    const updateSql = `
                        UPDATE apcms_godown_utilization SET
                            no_of_lots = ?,
                            total_bag = ?, 
                            total_kgs = ?
                        WHERE id = ?
                    `;
                    await pool.execute(updateSql, [
                        no_of_lots, 
                        total_bag, 
                        total_kgs, 
                        finalId
                    ]);
                }

                rowResult.action = currentAction;
                rowResult.id = finalId;
                rowResult.message = `Successfully ${currentAction}ed.`;

            } catch (rowError) {
                rowResult.action = 'error';
                rowResult.message = `Database Error: ${rowError.message}`;
            }

            results.push(rowResult);
        }

        // 3. Summarize results
        const summary = {
            total: records.length,
            inserted: results.filter(r => r.action === 'insert').length,
            updated: results.filter(r => r.action === 'update').length,
            failed: results.filter(r => r.action === 'error').length
        };

        // 4. Final Response
        res.status(200).json({
            success: summary.failed === 0,
            summary,
            message: `Processed ${summary.total} records: ${summary.inserted} inserted, ${summary.updated} updated, ${summary.failed} failed.`,
            results
        });

    } catch (error) {
        next(error);
    }
};

exports.getGodownUtilization = async (req, res, next) => {
  try {
    // 1. SQL Query with JOINs
    // Fetches all godown records without filtering by type
    const sql = `
      SELECT 
        g.id,
        g.society_id,
        s.society_name,
        g.entry_date,
        g.acc_type,
        g.acc_sub_type,
        g.item_id,
        i.item_name,
        g.no_of_lots,
        g.total_bag,
        g.total_kgs,
        g.created_at
      FROM apcms_godown_utilization g
      LEFT JOIN apcms_society_master s ON g.society_id = s.id
      LEFT JOIN apcms_item_master i ON g.item_id = i.id
      ORDER BY g.entry_date DESC
    `;

    const [rows] = await pool.execute(sql);

    // 2. Handle empty results
    if (rows.length === 0) {
      return res.status(200).json({
        success: true,
        count: 0,
        data: [],
        message: "No godown utilization records found."
      });
    }

    // 3. Send Response
    res.status(200).json({
      success: true,
      count: rows.length,
      data: rows
    });
  } catch (error) {
    next(error);
  }
};



/////////////////// OLD APIS ////////////////////////

exports.addItemMaster_old_05_12_25 = async (req, res, next) => {
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

exports.addItemMasterold = async (req, res, next) => {
  try {
    const body = req.body;

    const requiredFields = [
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
        { isOperational: true }
      );
    }

    const { item_id, item_code, category, item_name } = body;

    // 1. Check if a record with this unique item_id already exists
    const [rows] = await pool.execute(
      'SELECT item_id FROM apcms_item_master WHERE item_id = ? AND item_code = ?',
      [item_id, item_code]
    );

    let result;
    let action;
    let final_row_id;

    if (rows.length === 0) {
      // 2. INSERT - No existing item_id found
      action = 'insert';
      const insertSql = `
        INSERT INTO apcms_item_master (
          item_id,
          item_code,
          category,
          item_name
        ) VALUES (?, ?, ?, ?)
      `;

      const insertValues = [
        item_id, // Unique key used for the new record
        item_code,
        category,
        item_name
      ];

      const [insertResult] = await pool.execute(insertSql, insertValues);
      result = insertResult;
      final_row_id = insertResult.insertId; // Get the auto-generated row_id
    } else {
      // 3. UPDATE - Existing item_id found
      action = 'update';
      final_row_id = rows[0].item_id; // Get the existing row_id for the response

      const updateSql = `
        UPDATE apcms_item_master
        SET
          item_code = ?,
          category = ?,
          item_name = ?
        WHERE item_id = ?
      `;

      // Note: item_id is NOT updated, but used in the WHERE clause
      const updateValues = [
        item_code,
        category,
        item_name,
        item_id // Used in the WHERE clause
      ];

      const [updateResult] = await pool.execute(updateSql, updateValues);
      result = updateResult;
    }

    // 4. Send Response
    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      // Return the item_id (the key) and the internal row_id
      item_id: item_id,
      //row_id: final_row_id,
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

exports.addLedgerMaster_old_05_12_25 = async (req, res, next) => {
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

exports.addLedgerMaster = async (req, res, next) => {
  try {
    const body = req.body;

    // 1. ledger_code is now the unique key used for upserting.
    const requiredFields = [
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
        { isOperational: true }
      );
    }

    const {
      ledger_id,
      ledger_code,
      ledger_name,
      created_date,
      status,
      ledger_type
    } = body;

    // 2. Check if a record with this unique ledger_code already exists
    const [rows] = await pool.execute(
      'SELECT ledger_id, ledger_code FROM apcms_ledger_master WHERE ledger_id = ?',
      [ledger_id]
    );

    let result;
    let action;
    let final_row_id; // To store the row_id for the response

    if (rows.length === 0) {
      // 3. INSERT - No existing ledger_code found
      action = 'insert';
      const insertSql = `
        INSERT INTO apcms_ledger_master (
          ledger_id,
          ledger_code,
          ledger_name,
          created_date,
          status,
          ledger_type
        ) VALUES (?, ?, ?, ?, ?, ?)
      `;

      const insertValues = [
        ledger_id,
        ledger_code,
        ledger_name,
        created_date,
        status,
        ledger_type
      ];

      const [insertResult] = await pool.execute(insertSql, insertValues);
      result = insertResult;
      final_row_id = insertResult.insertId; // Get the auto-generated row_id
    } else {
      // 4. UPDATE - Existing ledger_code found
      action = 'update';
      final_row_id = rows[0].ledger_id; // Get the existing row_id for the response

      const updateSql = `
        UPDATE apcms_ledger_master
        SET
          ledger_code = ?,
          ledger_name = ?,
          created_date = ?,
          status = ?,
          ledger_type = ?
        WHERE ledger_id = ?
      `;

      const updateValues = [
        ledger_code,
        ledger_name,
        created_date,
        status,
        ledger_type,
        ledger_id
      ];

      const [updateResult] = await pool.execute(updateSql, updateValues);
      result = updateResult;
    }

    // 5. Send Response
    res.status(action === 'insert' ? 201 : 200).json({
      success: true,
      action,
      // Return the ledger_code and the internal row_id
      ledger_id: ledger_id,
      //row_id: final_row_id,
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

exports.addLoanData_old_05_12_25 = async (req, res, next) => {
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
      'ledger_id', 'item_id', 'item_name', 'entry_date',
      'pkt_opening', 'pkt_issued', 'pkt_collection', 'pkt_balance',
      'pkt_opening_value', 'pkt_issued_value', 'pkt_collection_value',
      'pkt_balance_value', 'created_date', 'system_name'
    ];

    const results = []; // Array to store the outcome of each record operation

    // 2. ITERATE OVER ALL RECORDS
    for (const body of records) {
      const result = { action: 'error', message: 'Unknown error occurred.' };

      // --- A) PER-RECORD VALIDATION ---
      const missing = requireFields(body, requiredFields);
      if (missing.length) {
        result.message = `Validation failed: Missing fields: ${missing.join(', ')}`;
        results.push(result);
        continue; // Skip to the next record
      }

      
      // Destructure for easy access
      const {
        ledger_id, item_id, item_name, entry_date,
        pkt_opening, pkt_issued, pkt_collection, pkt_balance,
        pkt_opening_value, pkt_issued_value, pkt_collection_value,
        pkt_balance_value, created_date, system_name
      } = body;
      
      result.unique_key = `${ledger_id}/${item_id}/${entry_date}`;

      

      try {

        // 3. CHECK IF RECORD EXISTS
        const [rows] = await pool.execute(
          'SELECT id FROM apcms_loan WHERE ledger_id = ? AND item_id = ? AND entry_date = ?',
          [ledger_id, item_id, entry_date]
        );

        if (rows.length > 0) {
          // Record exists -> NO ACTION
          result.action = 'noop';
          result.id = rows[0].id;
          result.message = `Record already exists. No action taken.`;
        } else {
          // Record NOT exists -> INSERT
          const insertSql = `
                        INSERT INTO apcms_loan (
                            ledger_id, item_id, item_name, entry_date, 
                            pkt_opening, pkt_issued, pkt_collection, pkt_balance, 
                            pkt_opening_value, pkt_issued_value, pkt_collection_value, 
                            pkt_balance_value, created_date, system_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    `;

          const insertValues = [
            ledger_id, item_id, item_name, entry_date,
            pkt_opening, pkt_issued, pkt_collection, pkt_balance,
            pkt_opening_value, pkt_issued_value, pkt_collection_value,
            pkt_balance_value, created_date, system_name
          ];

          const [insertResult] = await pool.execute(insertSql, insertValues);

          result.action = 'insert';
          result.id = insertResult.insertId;
          result.message = 'Record created successfully.';
        }
      } catch (dbError) {
        // Catch database errors specific to this record
        result.message = `Database error: ${dbError.message}`;
        // Log the full error for debugging (optional)
        console.error(`Error processing id ${result.unique_key}:`, dbError);
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

exports.addSalesData_old_05_12_25 = async (req, res, next) => {
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
      'ledger_id', 'item_id', 'item_name', 'entry_date',
      'total_qty', 'total_amount', 'created_date', 'system_name'
    ];

    const results = []; // Array to store the outcome of each record operation

    // 2. ITERATE OVER ALL RECORDS
    for (const body of records) {
      // Initialize result object for current record
      const result = { action: 'error', message: 'Unknown error occurred.' };

      // --- A) PER-RECORD VALIDATION ---
      const missing = requireFields(body, requiredFields);
      if (missing.length) {
        result.message = `Validation failed: Missing fields: ${missing.join(', ')}`;
        results.push(result);
        continue; // Skip to the next record in the batch
      }

      // Destructure for easy access
      const {
        ledger_id, item_id, item_name, entry_date,
        total_qty, total_amount, created_date, system_name
      } = body;

      result.unique_key = `${ledger_id}/${item_id}/${entry_date}`;

      try {
        // 3. CHECK IF RECORD EXISTS
        const [rows] = await pool.execute(
          'SELECT id FROM apcms_sales WHERE ledger_id = ? AND item_id = ? AND entry_date = ?',
          [ledger_id, item_id, entry_date]
        );

        if (rows.length > 0) {
          // Record exists -> NO ACTION (Skip the update logic)
          result.action = 'noop';
          result.id = rows[0].id;
          result.message = `Sales record with row_id (ID: ${result.id}) already exists. No action taken.`;
        } else {
          // Record NOT exists -> INSERT
          const insertSql = `
                        INSERT INTO apcms_sales (
                            ledger_id, item_id, item_name, entry_date, 
                            total_qty, total_amount, created_date, system_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    `;

          const insertValues = [
            ledger_id, item_id, item_name, entry_date,
            total_qty, total_amount, created_date, system_name
          ];

          const [insertResult] = await pool.execute(insertSql, insertValues);

          result.action = 'insert';
          result.id = insertResult.insertId;
          result.message = 'Sales record created successfully.';
        }
      } catch (dbError) {
        // Catch database errors specific to this record
        result.message = `Database error: ${dbError.message}`;
        console.error(`Error processing sales record: ${result.unique_key}`, dbError);
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

    // Map the results to format the entry_date 
    const formattedResults = results.map(row => {
      // Create a copy of the row object
      const newRow = { ...row };
      
      // Check if entry_date exists and is a valid date object/string
      if (newRow.entry_date) {
        // Convert the date object/string to an ISO string and take the first 10 characters (YYYY-MM-DD)
        // Ensure it's treated as an ISO string first for reliable substring indexing.
        // If it's a Date object from the pool, use .toISOString()
        const dateString = newRow.entry_date instanceof Date 
                           ? newRow.entry_date.toISOString() 
                           : String(newRow.entry_date);
                           
        newRow.entry_date = dateString.substring(0, 10);
      }
      
      return newRow;
    });

    res.status(200).json({
      data: formattedResults,
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

    // Map the results to format the entry_date 
    const formattedResults = results.map(row => {
      // Create a copy of the row object
      const newRow = { ...row };
      
      // Check if entry_date exists and is a valid date object/string
      if (newRow.entry_date) {
        // Convert the date object/string to an ISO string and take the first 10 characters (YYYY-MM-DD)
        // Ensure it's treated as an ISO string first for reliable substring indexing.
        // If it's a Date object from the pool, use .toISOString()
        const dateString = newRow.entry_date instanceof Date 
                           ? newRow.entry_date.toISOString() 
                           : String(newRow.entry_date);
                           
        newRow.entry_date = dateString.substring(0, 10);
      }
      
      return newRow;
    });

    res.status(200).json({
      data: formattedResults,
      total,
    });
  } catch (error) {
    next(error);
  }
};

exports.listAllLedgers = async (req, res, next) => {
  try {
    // SQL query to select all columns and all rows from the item master table.
    const sql = 'SELECT * FROM apcms_ledger_master';

    // Execute the query.
    // The result is an array containing the row data and metadata, we only need the rows.
    const [rows] = await pool.execute(sql);

    // Check if any records were found
    if (rows.length === 0) {
      // If no items are found, return a 200 OK status with an empty data array and a specific message.
      return res.status(200).json({
        success: true,
        count: 0,
        data: [],
        message: 'No item master records found.'
      });
    }

    // If records are found, send a 200 OK status with the data array.
    res.status(200).json({
      success: true,
      count: rows.length,
      data: rows,
      message: 'Successfully retrieved all item ledger records.'
    });
  } catch (error) {
    // Catch any database or internal errors and pass them to the Express error handler.
    next(error);
  }
};