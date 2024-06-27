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
 *                   id:
 *                     type: integer
 *                     example: 2780
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T14:01:04.001Z"
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T14:01:04.001Z"
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T13:00:00.000Z"
 *                   chain:
 *                     type: string
 *                     example: "base"
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: "0xc74ea762cf06c9151ce074e6a569a5945b6302e7"
 *                   rewards_usd:
 *                     type: number
 *                     format: double
 *                     example: 139.0817897508311102761881268232624900000000000000000000
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
 *                   id:
 *                     type: integer
 *                     example: 2780
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T14:01:04.001Z"
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T14:01:04.001Z"
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: "2024-06-27T13:00:00.000Z"
 *                   chain:
 *                     type: string
 *                     example: "base"
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: "0xc74ea762cf06c9151ce074e6a569a5945b6302e7"
 *                   rewards_usd:
 *                     type: number
 *                     format: double
 *                     example: 139.0817897508311102761881268232624900000000000000000000
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
