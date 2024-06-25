const express = require('express');
const router = express.Router();
const { getLatestPoolRewardsData, getAllPoolRewardsData } = require('../services/poolRewardsService');

/**
 * @swagger
 * tags:
 *   - name: Pool Rewards
 *     description: Endpoints related to pool rewards
 */

/**
 * @swagger
 * /pool-rewards/latest/{chain}:
 *   get:
 *     summary: Get the latest pool rewards data
 *     tags: 
 *       - Pool Rewards
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the pool rewards data for
 *     responses:
 *       200:
 *         description: The latest pool rewards data
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
 *                   rewards_usd:
 *                     type: number
 *                     format: double
 *                   chain:
 *                     type: string
 *       404:
 *         description: Pool rewards data not found
 *       500:
 *         description: Server error
 */
router.get('/latest/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestPoolRewardsData(chain);

    if (!result.length) {
      return res.status(404).send('Pool rewards data not found');
    }

    return res.json(result); 
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

/**
 * @swagger
 * /pool-rewards/all/{chain}:
 *   get:
 *     summary: Get all pool rewards data
 *     tags: 
 *       - Pool Rewards
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the pool rewards data for
 *     responses:
 *       200:
 *         description: All pool rewards data
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
 *                   rewards_usd:
 *                     type: number
 *                     format: double
 *                   chain:
 *                     type: string
 *       404:
 *         description: Pool rewards data not found
 *       500:
 *         description: Server error
 */
router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllPoolRewardsData(chain);

    if (!result.length) {
      return res.status(404).send('Pool rewards data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = router;
