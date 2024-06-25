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
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                   pool_id:
 *                     type: integer
 *                   collateral_type:
 *                     type: string
 *                   amount_delegated:
 *                     type: number
 *                     format: double
 *                   chain:
 *                     type: string
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
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                   pool_id:
 *                     type: integer
 *                   collateral_type:
 *                     type: string
 *                   amount_delegated:
 *                     type: number
 *                     format: double
 *                   chain:
 *                     type: string
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
