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
 *                   id:
 *                     type: integer
 *                     example: 1
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T15:44:39.678Z'
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T15:44:39.678Z'
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T13:00:00.000Z'
 *                   chain:
 *                     type: string
 *                     example: 'base'
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: '0xc74ea762cf06c9151ce074e6a569a5945b6302e7'
 *                   amount:
 *                     type: number
 *                     format: double
 *                     example: 25611607.3726851228
 *                   collateral_value:
 *                     type: number
 *                     format: double
 *                     example: 25611607.3726851228
 *                   block_ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T13:15:47.000Z'
 *                   block_number:
 *                     type: integer
 *                     example: 16266000
 *                   contract_address:
 *                     type: string
 *                     example: '0x32c222a9a159782afd7529c87fa34b96ca72c696'
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
 *                   id:
 *                     type: integer
 *                     example: 2531
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T15:44:41.072Z'
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T15:44:41.072Z'
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T14:00:00.000Z'
 *                   chain:
 *                     type: string
 *                     example: 'arbitrum'
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
 *                   amount:
 *                     type: number
 *                     format: double
 *                     example: 495.3753814020
 *                   collateral_value:
 *                     type: number
 *                     format: double
 *                     example: 1684819.8145111112
 *                   block_ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T14:52:28.000Z'
 *                   block_number:
 *                     type: integer
 *                     example: 225600000
 *                   contract_address:
 *                     type: string
 *                     example: '0xffffffaeff0b96ea8e4f94b2253f31abdd875847'
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
