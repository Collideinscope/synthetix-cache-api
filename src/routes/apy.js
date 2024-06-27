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
 *                   apys:
 *                     type: object
 *                     properties:
 *                       apy_24h:
 *                         type: object
 *                         properties:
 *                           minute:
 *                             type: number
 *                             format: double
 *                             example: 0.00000011
 *                           hour:
 *                             type: number
 *                             format: double
 *                             example: 0.00000640
 *                           week:
 *                             type: number
 *                             format: double
 *                             example: 0.00107918
 *                           month:
 *                             type: number
 *                             format: double
 *                             example: 0.00468485
 *                           year:
 *                             type: number
 *                             format: double
 *                             example: 0.05768958
 *                       apy_7d:
 *                         type: object
 *                         properties:
 *                           minute:
 *                             type: number
 *                             format: double
 *                             example: 0.00000026
 *                           hour:
 *                             type: number
 *                             format: double
 *                             example: 0.00001536
 *                           week:
 *                             type: number
 *                             format: double
 *                             example: 0.00259109
 *                           month:
 *                             type: number
 *                             format: double
 *                             example: 0.01127665
 *                           year:
 *                             type: number
 *                             format: double
 *                             example: 0.14403616
 *                       apy_28d:
 *                         type: object
 *                         properties:
 *                           minute:
 *                             type: number
 *                             format: double
 *                             example: 0.00000043
 *                           hour:
 *                             type: number
 *                             format: double
 *                             example: 0.00002573
 *                           week:
 *                             type: number
 *                             format: double
 *                             example: 0.00434311
 *                           month:
 *                             type: number
 *                             format: double
 *                             example: 0.01895681
 *                           year:
 *                             type: number
 *                             format: double
 *                             example: 0.25276421
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
 *                   apys:
 *                     type: object
 *                     properties:
 *                       apy_24h:
 *                         type: object
 *                         properties:
 *                           minute:
 *                             type: number
 *                             format: double
 *                             example: 0.00000011
 *                           hour:
 *                             type: number
 *                             format: double
 *                             example: 0.00000640
 *                           week:
 *                             type: number
 *                             format: double
 *                             example: 0.00107918
 *                           month:
 *                             type: number
 *                             format: double
 *                             example: 0.00468485
 *                           year:
 *                             type: number
 *                             format: double
 *                             example: 0.05768958
 *                       apy_7d:
 *                         type: object
 *                         properties:
 *                           minute:
 *                             type: number
 *                             format: double
 *                             example: 0.00000026
 *                           hour:
 *                             type: number
 *                             format: double
 *                             example: 0.00001536
 *                           week:
 *                             type: number
 *                             format: double
 *                             example: 0.00259109
 *                           month:
 *                             type: number
 *                             format: double
 *                             example: 0.01127665
 *                           year:
 *                             type: number
 *                             format: double
 *                             example: 0.14403616
 *                       apy_28d:
 *                         type: object
 *                         properties:
 *                           minute:
 *                             type: number
 *                             format: double
 *                             example: 0.00000043
 *                           hour:
 *                             type: number
 *                             format: double
 *                             example: 0.00002573
 *                           week:
 *                             type: number
 *                             format: double
 *                             example: 0.00434311
 *                           month:
 *                             type: number
 *                             format: double
 *                             example: 0.01895681
 *                           year:
 *                             type: number
 *                             format: double
 *                             example: 0.25276421
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
