#!/usr/bin/env python3

import sys
import json
import psycopg2
import time
from datetime import datetime

# Valid vote rules:
# - Vote during epoch 302
# - Vote transaction output value must be > 2 and < 3
# - Vote 1 must be for a listed or disqualified pool
# - Vote 2 must be 0 or for a listed or disqualified pool
# - All vote transaction inputs & outputs must have the same non-null stake address
# - Vote transaction output must only include ADA (no other native assets)
# - Vote transaction must include only one valid vote output (else it is considered ambiguous and ignored)

vote_epoch=302

def wallet_stake_before(connection, stake_addr_id, tx_id):
    cursor = connection.cursor()
    cursor.execute("""SELECT SUM(value)
        FROM
            (SELECT tx_out.value, tx_out.tx_id, tx_out.stake_address_id
                FROM tx_out
                 LEFT JOIN tx_in ON tx_out.tx_id = tx_in.tx_out_id
                    AND tx_out.index::smallint = tx_in.tx_out_index::smallint
              WHERE (tx_in.tx_in_id IS NULL OR tx_in.tx_in_id > {1})
             ) utxo
        WHERE stake_address_id = '{0}'
        AND tx_id < {1};
    """.format(stake_addr_id, tx_id))

    res = cursor.fetchone()
    cursor.close()
    if res[0] != None:
        return int(res[0])
    else:
        return 0

def reward_at(connection, stake_addr_id, tx_id):
    rewards = 0
    cursor = connection.cursor()

    # Rewards earned at the beginning of vote epoch
    cursor.execute("""SELECT SUM(amount)
        FROM reward
        WHERE addr_id = {}
        AND reward.spendable_epoch <=
            (SELECT block.epoch_no
             FROM tx, block
             WHERE tx.block_id = block.id
             AND tx.id = {}
            )
    """.format(stake_addr_id, tx_id))

    res = cursor.fetchone()
    if res[0] != None:
        rewards = int(res[0])

    # Withdrawals before vote epoch
    cursor.execute("""SELECT SUM(amount)
        FROM withdrawal
        WHERE addr_id = {}
        AND tx_id < {}
    """.format(stake_addr_id, tx_id))

    res = cursor.fetchone()
    cursor.close()
    if res[0] != None:
        rewards -= int(res[0])

    return rewards


connection = psycopg2.connect(database = "cardanext")
cursor = connection.cursor()

cursor.execute("""SELECT MIN(tx.id), MAX(tx.id)
    FROM tx,block
    WHERE tx.block_id=block.id
    AND epoch_no=%s
""", (vote_epoch,))
(min_tx_id, max_tx_id,) = cursor.fetchone()

pools = {}
votes = {}
with open('sundae.json', "r") as file:
    data = json.load(file)
    spos = data["data"]["spos"]
    for spo in spos:
        id = spo['id']
        votes[id] = {}
        votes[id][0] = []
        votes[id][1] = []
        pools[id] = {}
        pools[id]['ticker'] = spo['ticker']
        if len(spo['ticker']) == 0:
            pools[id]['ticker'] = spo['name']

# Preselect transactions from the following conditions
# - was included in vote epoch transactions
# - contains at least one output with (2105000 <= value <=  2993993) and no native assets
# - all inputs and outputs have the same non-null stake address
cursor.execute("""SELECT DISTINCT ON (tx.id)
    tx.id,
    tx.hash,
    tx_out.stake_address_id,
    stake_address.view
    FROM tx_out
    INNER JOIN tx ON tx.id=tx_out.tx_id
    INNER JOIN stake_address ON stake_address.id=tx_out.stake_address_id
    INNER JOIN tx_in ON tx_in.tx_in_id=tx_out.tx_id
    INNER JOIN tx_out tx_out_in
        ON tx_out_in.tx_id=tx_in.tx_out_id
        AND tx_out_in.index=tx_out_index
        AND tx_out_in.stake_address_id=tx_out.stake_address_id
    WHERE tx_out.tx_id >= %s
    AND tx_out.tx_id <= %s
    AND tx_out.value >= 2105000
    AND tx_out.value <= 2993993
    AND NOT EXISTS (
        SELECT TRUE FROM tx_out
        WHERE tx_out.tx_id=tx.id
        AND (tx_out.stake_address_id IS NULL OR tx_out.stake_address_id != stake_address.id)
    )
    AND NOT EXISTS (
        SELECT TRUE FROM ma_tx_out
        WHERE ma_tx_out.tx_out_id=tx_out.id
    )
    AND NOT EXISTS (
        SELECT TRUE
        FROM tx_in
        INNER JOIN tx_out tx_out_in
            ON tx_out_in.tx_id = tx_in.tx_out_id
            AND tx_out_in.index = tx_in.tx_out_index
            AND (tx_out_in.stake_address_id IS NULL OR tx_out_in.stake_address_id != tx_out.stake_address_id)
        WHERE tx_in.tx_in_id = tx.id
    )
    ORDER BY tx.id DESC
""", (min_tx_id, max_tx_id))

nvotes0 = 0
nvotes1 = 0
nvotes2 = 0
dump = {}
ambiguous_votes = 0
ambiguous_votes_value = 0
for record in cursor:
    (tx_id, tx_hash, addr_id, addr) = record

    if addr in dump:
        # A more recent valid vote exists, ignore
        continue

    # Stake at the start of epoch vote
    stake = wallet_stake_before(connection, addr_id, min_tx_id)
    # Reward spendable during vote epoch
    reward = reward_at(connection, addr_id, min_tx_id)
    if reward < 0:
        print("negative reward balance:", addr, reward, file=sys.stderr)
        reward = 0
    total = stake + reward

    # Check that there is a single valid vote output
    #
    # We check again that the vote output does not contain non-ADA assets
    # as the output checked in the query might be another one.
    c = connection.cursor()
    c.execute("""SELECT tx_out.value
        FROM tx_out
        WHERE tx_out.tx_id=%s
        AND NOT EXISTS (
            SELECT TRUE FROM ma_tx_out
            WHERE ma_tx_out.tx_out_id=tx_out.id
        )
    """, (tx_id,))
    vote1, votes2, matches = 0, 0, 0
    for r in c:
        (value,) = r
        # Check that the pools are part of the voting list
        v1 = int(value)//1000 - 2000
        v2 = int(value) - int(value)//1000*1000
        if v1 in pools and (v2 == 0 or v2 in pools):
            matches += 1
            vote1 = v1
            vote2 = v2
    if matches == 0:
        continue
    elif matches > 1:
        # Several outputs matching a vote, ignoring
        print("ambiguous tx", tx_hash.hex(), file=sys.stderr)
        ambiguous_votes += 1
        ambiguous_votes_value += total
        continue

    dump[addr] = {}
    dump[addr] = [total, vote1, vote2, tx_hash.hex()]

    votes[vote1][0].append(total)
    # Ignore unrelevant second vote
    if vote2 != 0 and vote1 != vote2:
        votes[vote1][1].append((vote2,total))

    if total > 0:
        nvotes1 += 1
    else:
        nvotes0 += 1
    if total > 0 and vote2 != 0 and vote2 != vote1:
        nvotes2 += 1

total = round(sum([sum(votes[id][0]) for id in votes]) / 1000000)
print("\nTotal: {} ₳".format(total))
print("Votes 1:", nvotes1)
print("Votes 2:", nvotes2)
print("0₳ votes:", nvotes0)
print("Ambiguous votes: {} votes for {:,} ₳".format(ambiguous_votes, round(ambiguous_votes_value/1000000)))

with open('sundae_votes.json', 'w') as file:
    json.dump(dump, file)

# Apply the Ranked Choice Voting algorithm
# From https://iso.sundaeswap.finance/#/how-to:
#    The ISO SPO Vote will use a system known as ranked choice voting.
#    Participants vote for their #1 and #2 choices. At the end of the voting
#    period, votes will be tallied and winners selected as follows:
#    
#     1. Participants 1st choice will be tallied and the SPOs will be sorted
#        according to this vote count.
#     2. The SPO with the fewest votes will be removed.
#     3. Any participants who voted for that SPO will have their votes
#        transferred to their 2nd choice.
#     4. If a participant's 2nd choice has already been eliminated, then no
#        votes will be transferred.
#     5. The process repeats from step 2 until only 40 SPOs (30 winners, 10
#        on a wait list) remain.
ranking = []
disqualified_pools = [663, 875] # SUNNY, CHEFF
while len(votes) > 0:
    if len(disqualified_pools) > 0:
        # Rank last disqualified pools (but keep them to use 2nd votes)
        last_pool_id = disqualified_pools.pop(0)
        last_pool = votes[last_pool_id]
    else:
        # Sort remaining pools by votes and use the worst
        results = sorted(votes.items(), key=lambda pool: sum(pool[1][0]))
        (last_pool_id, last_pool) = results[0]

    ranking.insert(0, (last_pool_id, pools[last_pool_id]['ticker'], last_pool))
    votes.pop(last_pool_id)

    # Transfer 2nd votes to remaining pools until we have a list of 40 pools
    if len(votes) > 40:
        for (vote2_id, vote2_total) in last_pool[1]:
            if vote2_id in votes:
                votes[vote2_id][0].append(vote2_total)
                #print("adding", vote2_id, vote2_total, file=sys.stderr)
            else:
                pass
                #print("ignoring", vote2_id, vote2_total, file=sys.stderr)

print("\nRanking:");
r = 0
previous_lovelaces = None
for (id, ticker, votes) in ranking:
    lovelaces = sum(votes[0])
    if previous_lovelaces is None or lovelaces < previous_lovelaces:
        r += 1
    print("{:>3}\t{:,.0f}\t{}\t{}".format(r,
            lovelaces/1000000,
            len([v for v in votes[0] if v > 0]),
            ticker))
    previous_lovelaces = lovelaces
