const express = require('express');
const router = express.Router();
const { getLatestAPYData, getAllAPYData } = require('../services/apyService');
const { modifyAPYDataWithTimeframes } = require('../transformers');

/**
 * @swagger
 * tags:
 *   - name: APY
 *     description: Endpoints related to core APY
 */

/**
 * @swagger
 * /apy/latest/{chain}:
 *   get:
 *     summary: Get the latest APY data
 *     tags: 
 *       - APY
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the APY data for
 *     responses:
 *       200:
 *         description: The latest APY data
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: integer
 *                     example: 1
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2023-06-24T12:34:56Z'
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2023-06-24T12:34:56Z'
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2023-06-24T12:34:56Z'
 *                   chain:
 *                     type: string
 *                     example: 'mainnet'
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: '0x1234567890abcdef'
 *                   collateral_value:
 *                     type: number
 *                     format: double
 *                     example: 1000.0
 *                   apy_24h:
 *                     type: number
 *                     format: double
 *                     example: 0.05
 *                   apy_7d:
 *                     type: number
 *                     format: double
 *                     example: 0.35
 *                   apy_28d:
 *                     type: number
 *                     format: double
 *                     example: 1.5
 *       404:
 *         description: APY data not found
 *       500:
 *         description: Server error
 */
router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestAPYData(chain);

    if (!result.length) {
      return res.status(404).send('APY data not found');
    }

    const transformedData = modifyAPYDataWithTimeframes(result);

    return res.json(transformedData); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

/**
 * @swagger
 * /apy/all/{chain}:
 *   get:
 *     summary: Get all APY data
 *     tags: 
 *       - APY
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the APY data for
 *     responses:
 *       200:
 *         description: All APY data
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: integer
 *                     example: 1
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2023-06-24T12:34:56Z'
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2023-06-24T12:34:56Z'
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2023-06-24T12:34:56Z'
 *                   chain:
 *                     type: string
 *                     example: 'mainnet'
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: '0x1234567890abcdef'
 *                   collateral_value:
 *                     type: number
 *                     format: double
 *                     example: 1000.0
 *                   apy_24h:
 *                     type: number
 *                     format: double
 *                     example: 0.05
 *                   apy_7d:
 *                     type: number
 *                     format: double
 *                     example: 0.35
 *                   apy_28d:
 *                     type: number
 *                     format: double
 *                     example: 1.5
 *       404:
 *         description: APY data not found
 *       500:
 *         description: Server error
 */
router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllAPYData(chain);

    if (!result.length) {
      return res.status(404).send('APY data not found');
    }

    const transformedData = modifyAPYDataWithTimeframes(result);

    return res.json(transformedData);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = router;
