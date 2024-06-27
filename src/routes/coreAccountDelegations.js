const express = require('express');
const router = express.Router();
const {
  getStakerCount,
  getLatestCoreAccountDelegationsDataOrderedByAccount,
  getCoreAccountDelegationsDataByAccount,
  getAllCoreAccountDelegationsData
} = require('../services/coreAccountDelegationsService');

/**
 * @swagger
 * tags:
 *   - name: Core Account Delegations
 *     description: Endpoints related to core account delegations
 */

/**
 * @swagger
 * /core-account-delegations/staker-count/{chain}:
 *   get:
 *     summary: Get the unique staker count
 *     tags: 
 *       - Core Account Delegations
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the staker count for
 *     responses:
 *       200:
 *         description: The unique staker count
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               additionalProperties:
 *                 type: string
 *                 example: "305"
 *       500:
 *         description: Server error
 */
router.get('/staker-count/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const stakerCount = await getStakerCount(chain);

    return res.json(stakerCount);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

/**
 * @swagger
 * /core-account-delegations/all/{chain}:
 *   get:
 *     summary: Get all core account delegations data
 *     tags: 
 *       - Core Account Delegations
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the data for
 *     responses:
 *       200:
 *         description: All core account delegations data
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: integer
 *                     example: 1450
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-27T15:00:50.345Z'
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-27T15:00:50.345Z'
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-27T14:58:45.000Z'
 *                   chain:
 *                     type: string
 *                     example: 'arbitrum'
 *                   account_id:
 *                     type: string
 *                     example: '170141183460469231731687303715884105785'
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: '0x912CE59144191C1204E64559FE8253a0e49E6548'
 *                   amount_delegated:
 *                     type: number
 *                     format: double
 *                     example: 19128.7385237578
 *       404:
 *         description: Core account delegations data not found
 *       500:
 *         description: Server error
 */
router.get('/all/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getAllCoreAccountDelegationsData(chain);

    if (!result.length) {
      return res.status(404).send('Core account delegations data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

/**
 * @swagger
 * /core-account-delegations/account/{accountId}:
 *   get:
 *     summary: Get core account delegations data by account ID
 *     tags: 
 *       - Core Account Delegations
 *     parameters:
 *       - in: path
 *         name: accountId
 *         schema:
 *           type: string
 *         required: true
 *         description: The account ID to get the data for
 *     responses:
 *       200:
 *         description: Core account delegations data for the given account ID
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: integer
 *                     example: 879
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T15:45:11.350Z'
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T15:45:11.350Z'
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2023-12-15T08:04:45.000Z'
 *                   chain:
 *                     type: string
 *                     example: 'base'
 *                   account_id:
 *                     type: string
 *                     example: '1'
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: '0xC74eA762cF06c9151cE074E6a569a5945b6302E7'
 *                   amount_delegated:
 *                     type: number
 *                     format: double
 *                     example: 200.0000000000
 *       404:
 *         description: Core account delegations data not found for this account ID
 *       500:
 *         description: Server error
 */
router.get('/account/:accountId', async (req, res) => {
  try {
    const { accountId } = req.params;
    const result = await getCoreAccountDelegationsDataByAccount(accountId);

    if (!result.length) {
      return res.status(404).send('Core account delegations data not found for this account ID');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

/**
 * @swagger
 * /core-account-delegations/ordered-by-account/{chain}:
 *   get:
 *     summary: Get core account delegations data ordered by account
 *     tags: 
 *       - Core Account Delegations
 *     parameters:
 *       - in: path
 *         name: chain
 *         schema:
 *           type: string
 *         required: false
 *         description: The chain to get the data for
 *     responses:
 *       200:
 *         description: Core account delegations data ordered by account
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: integer
 *                     example: 879
 *                   created_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T15:45:11.350Z'
 *                   updated_at:
 *                     type: string
 *                     format: date-time
 *                     example: '2024-06-25T15:45:11.350Z'
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                     example: '2023-12-15T08:04:45.000Z'
 *                   chain:
 *                     type: string
 *                     example: 'base'
 *                   account_id:
 *                     type: string
 *                     example: '1'
 *                   pool_id:
 *                     type: integer
 *                     example: 1
 *                   collateral_type:
 *                     type: string
 *                     example: '0xC74eA762cF06c9151cE074E6a569a5945b6302E7'
 *                   amount_delegated:
 *                     type: number
 *                     format: double
 *                     example: 200.0000000000
 *       404:
 *         description: Core account delegations data not found
 *       500:
 *         description: Server error
 */
router.get('/ordered-by-account/:chain?', async (req, res) => {
  try {
    const { chain } = req.params;
    const result = await getLatestCoreAccountDelegationsDataOrderedByAccount(chain);

    if (!result.length) {
      return res.status(404).send('Core account delegations data not found');
    }

    return res.json(result);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Server error');
  }
});

module.exports = router;
