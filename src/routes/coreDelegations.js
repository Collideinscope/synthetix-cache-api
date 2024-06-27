const express = require('express');
const router = express.Router();
const {
  getLatestCoreDelegationsData,
  getAllCoreDelegationsData
} = require('../services/coreDelegationsService');

/**
 * @swagger
 * tags:
 *   - name: Core Delegations
 *     description: Endpoints related to core delegations
 */

/**
 * @swagger
 * /core-delegations/latest/{chain}:
 *   get:
 *     summary: Get the latest core delegations data
 *     tags: 
 *       - Core Delegations
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the core delegations data for
 *     responses:
 *       200:
 *         description: The latest core delegations data
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: integer
 *                     example: 1910
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T16:00:47.837Z"
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T16:00:47.837Z"
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T15:00:00.000Z"
 *                   chain:
 *                     type: string
 *                     example: "base"
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: "0xC74eA762cF06c9151cE074E6a569a5945b6302E7"
 *                   amount_delegated:
 *                     type: number
 *                     format: double
 *                     example: 26161135.0136355676860495
 *                   block_ts:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T15:37:37.000Z"
 *       404:
 *         description: Core delegations data not found
 *       500:
 *         description: Server error
 */
router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestCoreDelegationsData(chain);

    if (!result.length) {
      return res.status(404).send('Core delegations data not found');
    }

    return res.json(result); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

/**
 * @swagger
 * /core-delegations/all/{chain}:
 *   get:
 *     summary: Get all core delegations data
 *     tags: 
 *       - Core Delegations
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the core delegations data for
 *     responses:
 *       200:
 *         description: All core delegations data
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: integer
 *                     example: 1912
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T16:00:47.950Z"
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T16:00:47.950Z"
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T15:00:00.000Z"
 *                   chain:
 *                     type: string
 *                     example: "arbitrum"
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: "0x912CE59144191C1204E64559FE8253a0e49E6548"
 *                   amount_delegated:
 *                     type: number
 *                     format: double
 *                     example: 3485854.8680902597222917
 *                   block_ts:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T15:11:44.000Z"
 *       404:
 *         description: Core delegations data not found
 *       500:
 *         description: Server error
 */
router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllCoreDelegationsData(chain);

    if (!result.length) {
      return res.status(404).send('Core delegations data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = router;
