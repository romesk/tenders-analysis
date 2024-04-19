def start_sync(run_ids_to_process: list[str]) -> None:

    # TODO: Implement the logic to sync the OLTP and OLAP databases
    for run_id in run_ids_to_process:

        # TODO: sync runs one by one (not to lose the data mofifications order)
        # for each run_id select all rows from the run_logs collection
        # process each row based on collection name and operation type
        # for each collection there must be a separate mapper function/class

        # e.g:
        # sync_tenders(run_id)  <-- inside get all rows from run_logs where collection is TENDERS and map
        # sync_entities(run_id) <-- inside get all rows from run_logs where collection is ENTITIES and map
        # sync_accounts(run_id) <-- inside get all rows from run_logs where collection is ACCOUNTS and map

        # etc...

        pass
