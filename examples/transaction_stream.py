import datetime
import random
from concurrent.futures import ThreadPoolExecutor

import redgrease
from redgrease.data import ExecutionStatus

user_count = 5
min_start_balance = 100
max_start_balance = 1000


r = redgrease.RedisGears()


def cleanup(r):
    # # Cleanup
    # Unregister all registrations
    for reg in r.gears.dumpregistrations():
        r.gears.unregister(reg.id)

    # Remove all executions
    for exe in r.gears.dumpexecutions():
        r.gears.dropexecution(str(exe.executionId))

    # Clear all keys
    r.flushall()

    # Check that there are no keys
    r.keys()


cleanup(r)


# Create some 'user' accounts with some existing balance
for user_id in range(user_count):
    start_balance = random.randint(min_start_balance, max_start_balance)
    r.hset(
        f"/user/{user_id}",
        mapping={
            "id": user_id,
            "balance": start_balance,
            "start_balance": start_balance,
        },
    )


# Helper function for sending transaction requests.
def attempt_random_transaction(
    channel,
    max_amount=100,
    message="This is a random transaction",
):
    r.xadd(
        f"transactions:{channel}",
        {
            "msg": message,
            "from": random.randint(0, user_count - 1),
            "to": random.randint(0, user_count - 1),
            "amount": random.randint(1, max_amount),
        },
    )


# Print a summary balance sheet for all users
def balance_sheet():
    sum_balance = 0
    for user_id in range(user_count):
        current_balance, start_balance = map(
            int, r.hmget(f"/user/{user_id}", "balance", "start_balance")
        )
        print(
            f"User {user_id} balance: {current_balance} "
            f" ({current_balance-start_balance})"
        )
        sum_balance += current_balance
    print("----------------------------")
    print(f"Total balance : {sum_balance}")
    return sum_balance


print()
print("Initial user balance distribution")
start_total_balance = balance_sheet()


# #  Transaction handling
# Transform a key-space event to a transaction
def initialize_transaction(event):
    transaction = event["value"]
    transaction["timestamp"] = datetime.datetime.utcnow().isoformat()
    transaction["channel"] = event["key"]
    transaction["id"] = event["id"]
    transaction["status"] = "pending"
    return transaction


# Handle the transaction safely
def handle_transaction(transaction):

    # Log the transaction event to the Redis engine log
    redgrease.log(f"Procesing transaction {transaction['id']}: {transaction}")

    sender = transaction["from"]
    recipient = transaction["to"]

    # Perform a sequence of commands atomically
    with redgrease.atomic():

        # Check if the 'sender' has sufficient balance
        sender_balance = redgrease.cmd.hget(f"/user/{sender}", "balance")
        amount = int(transaction.get("amount", 0))

        if not sender_balance or amount > int(sender_balance):
            # If balance is not sufficient, the transaction is marked as failed.
            transaction["status"] = f"FAILED: Missing {int(sender_balance)-amount}"

        else:
            # If there is sufficient balance,
            # remove the amount from sender and add it to the recipient
            # and mark as successful
            redgrease.cmd.hincrby(f"/user/{sender}", "balance", -amount)
            redgrease.cmd.hincrby(f"/user/{recipient}", "balance", amount)
            transaction["status"] = "successful"

            # If successful, add the transaction to the statement of the recipient
            redgrease.cmd.xadd(f"/user/{recipient}/statement", transaction)

        # Regardless of status, add the transaction to the statement of the sender
        redgrease.cmd.xadd(f"/user/{sender}/statement", transaction)

    redgrease.log(
        f"Done processing transaction {transaction['id']}: {transaction['status']}"
    )
    return transaction


# Transaction processing pipeline
transsaction_pipe = (
    redgrease.StreamReader()  # Listen to streams
    .map(
        initialize_transaction
    )  # Map stream events to a 'transaction' dict, and adds default.
    .map(handle_transaction)  # Execute the transaction
    .register(
        prefix="transactions:*", batch=10, duration=30
    )  # Listen to transaction stream and use batching
)


# Register the processing pipeline
print()
print("Register Gear function.")
transsaction_pipe.on(r)

# # Check Balance
for registration in r.gears.dumpregistrations():
    print(
        f"Registered Gear function {registration.id} has been "
        f"triggered {registration.RegistrationData.numTriggered} times."
    )

print()
print("Do one single random transaction.")
attempt_random_transaction("sample")
print("Balance after first transaction.")
balance_sheet()

# # Parallel Transactions
# Run a bunch of transactions in parallell
parallell_transaction_job_count = 100
sequential_transactions_count = 100
max_transaction_amount = 500


def sequential_transactions(channel="foo"):
    def attempt_transactions():
        for transaction_id in range(sequential_transactions_count):
            attempt_random_transaction(
                channel,
                max_amount=max_transaction_amount,
                message=f"This is a transaction #{transaction_id} on channel {channel}",
            )

    return attempt_transactions


def run_in_parallell(jobs):
    with ThreadPoolExecutor() as worker:
        for job in jobs:
            worker.submit(job)


print()
print(
    f"Run {parallell_transaction_job_count} batches of "
    f"{sequential_transactions_count} transactions, in paralell."
)
run_in_parallell(
    [sequential_transactions(nm) for nm in range(parallell_transaction_job_count)]
)

print()
print("Please wait for all transactions to complete...")
while not (
    all([exe.status == ExecutionStatus.done for exe in r.gears.dumpexecutions()])
):
    pass

print("Done!")
print()

# # Final Balance
print("Final Balance:")
end_total_balance = balance_sheet()
print(f"Total difference: {start_total_balance - end_total_balance}")
print()
for registration in r.gears.dumpregistrations():
    print(
        f"Registered Gear function {registration.id} has been "
        f"triggered {registration.RegistrationData.numTriggered} times."
    )
