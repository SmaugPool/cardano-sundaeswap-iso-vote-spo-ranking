# Cardano SundaeSwap ISO SPOs vote ranking

This Python 3 script uses the database populated by [cardano-db-sync](https://github.com/input-output-hk/cardano-db-sync) from the [Cardano](https://cardano.org/) blockchain to generate an unofficial ranking of the [SundaeSwap](https://sundaeswap.finance) ISO (Initial Stakepool Offering) stake pool operators vote that occured epoch 302.

You need to use the [`12.0.0-pre5`](https://github.com/input-output-hk/cardano-db-sync/releases/tag/12.0.0-pre5) tag of `cardano-db-sync` to get the exact same results as prior versions miss some rewards.

The script produces a `sundae_votes.json` JSON dictionary output whose keys are stake addresses and values an array with the following fields:
* [0]: wallet balance at beginning of epoch 302
* [1]: vote 1 id
* [2]: vote 2 id
* [3]: transaction hash

Votes SPOs ids can be found in the `sundae.json` file included and imported from SundaeSwap website.
