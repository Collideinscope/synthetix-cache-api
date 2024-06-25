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
 *                 type: integer
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
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                   account_id:
 *                     type: string
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
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                   account_id:
 *                     type: string
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
 *                   ts:
 *                     type: string
 *                     format: date-time
 *                   account_id:
 *                     type: string
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
