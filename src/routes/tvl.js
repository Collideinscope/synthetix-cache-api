const express = require('express');
const router = express.Router();
const { getLatestTVLData, getAllTVLData } = require('../services/tvlService');

/**
 * @swagger
 * tags:
 *   - name: TVL
 *     description: Endpoints related to Total Value Locked (TVL)
 */

/**
 * @swagger
 * /tvl/latest/{chain}:
 *   get:
 *     summary: Get the latest TVL data
 *     tags: 
 *       - TVL
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the TVL data for
 *     responses:
 *       200:
 *         description: The latest TVL data
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                   block_number:
 *                     type: integer
 *                   pool_id:
 *                     type: integer
 *                   collateral_type:
 *                     type: string
 *                   contract_address:
 *                     type: string
 *                   amount:
 *                     type: number
 *                     format: double
 *                   collateral_value:
 *                     type: number
 *                     format: double
 *                   chain:
 *                     type: string
 *       404:
 *         description: TVL data not found
 *       500:
 *         description: Server error
 */
router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestTVLData(chain);

    if (!result.length) {
      return res.status(404).send('TVL data not found');
    }

    return res.json(result); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

/**
 * @swagger
 * /tvl/all/{chain}:
 *   get:
 *     summary: Get all TVL data
 *     tags: 
 *       - TVL
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the TVL data for
 *     responses:
 *       200:
 *         description: All TVL data
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                   block_number:
 *                     type: integer
 *                   pool_id:
 *                     type: integer
 *                   collateral_type:
 *                     type: string
 *                   contract_address:
 *                     type: string
 *                   amount:
 *                     type: number
 *                     format: double
 *                   collateral_value:
 *                     type: number
 *                     format: double
 *                   chain:
 *                     type: string
 *       404:
 *         description: TVL data not found
 *       500:
 *         description: Server error
 */
router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllTVLData(chain);

    if (!result.length) {
      return res.status(404).send('TVL data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = router;
