import configparser
import asyncio
from curiosity.app.services.explorer_service import ExplorerService
from curiosity.helpers import (
    init_beam_rds_gateway,
    init_houston_rds_gateway,
    init_swapper_rds_gateway,
    init_syncer_rds_gateway,
)
import queries
import os
import pandas as pd

print(os.getcwd())
CONFIG = configparser.ConfigParser()
CONFIG.read('../../config.ini')


async def get_beam_messages(start_date, end_date):
    beam_explorer = ExplorerService(init_beam_rds_gateway(CONFIG))
    query = queries.get_beam_messages_query(
        start_date, end_date)
    print(query)
    return await beam_explorer.execute_query(query)


async def get_swap_data(start_date, end_date):
    swapper_explorer = ExplorerService(init_swapper_rds_gateway(CONFIG))
    return await swapper_explorer.execute_query(queries.get_swaps_query(start_date,
                                                                        end_date))


async def get_tx_data(start_date, end_date):
    syncer_explorer = ExplorerService(init_syncer_rds_gateway(CONFIG))
    return await syncer_explorer.execute_query(queries.get_transactions_query(
        start_date, end_date))


async def get_user_and_sessions_data(user_creation_date_start, user_creation_date_end):
    houston_explorer = ExplorerService(init_houston_rds_gateway(CONFIG))
    return await houston_explorer.execute_query(queries.get_users_and_sessions_query(
        user_creation_date_start, user_creation_date_end))


async def get_onchain_operations(start_date, end_date):
    houston_explorer = ExplorerService(init_houston_rds_gateway(CONFIG))
    return await houston_explorer.execute_query(queries.get_onchain_transactions_query(
        start_date, end_date))


async def get_distinct_channels():
    beam_explorer = ExplorerService(init_beam_rds_gateway(CONFIG))
    results_notifications = await beam_explorer.execute_query(
        queries.get_distinct_channels_from_notifications_query())
    results_archived_notifications = await beam_explorer.execute_query(
        queries.get_distinct_channels_from_archived_notifications_query())

    return set(results_notifications.channel).union(
        results_archived_notifications.channel)


async def get_balance_history_for_channels(channels):
    beam_explorer = ExplorerService(init_beam_rds_gateway(CONFIG))
    return await beam_explorer.execute_query(
        queries.get_balance_history_for_channels_query(channels))


async def get_user_id_to_channel_id():
    houston_explorer = ExplorerService(init_houston_rds_gateway(CONFIG))
    return await houston_explorer.execute_query(queries.get_user_id_to_channel_id_query())


async def get_diff_balances(channels):
    beam_explorer = ExplorerService(init_beam_rds_gateway(CONFIG))
    notifications_task = beam_explorer.execute_query(queries.get_diff_balances_query('notifications', channels))
    archived_notifications_task = beam_explorer.execute_query(
        queries.get_diff_balances_query('archived_notifications', channels))
    notifications_balances, archived_notifications_balances = await asyncio.gather(notifications_task, archived_notifications_task)

    return pd.concat([notifications_balances, archived_notifications_balances], ignore_index=True)


async def download_data(table_name, start_date, end_date):
    beam_explorer = ExplorerService(init_beam_rds_gateway(CONFIG))
    return await beam_explorer.execute_query(queries.get_download_data_query(table_name, start_date, end_date))


async def download_data_by_channel(table_name, min_channel, max_channel):
    beam_explorer = ExplorerService(init_beam_rds_gateway(CONFIG))
    return await beam_explorer.execute_query(queries.get_download_data_by_channel_query(table_name, min_channel, max_channel))


async def download_inputs(min_spent_output, max_spent_output):
    syncer_explorer = ExplorerService(init_syncer_rds_gateway(CONFIG))
    return await syncer_explorer.execute_query(queries.get_inputs_query(min_spent_output, max_spent_output))


async def download_outputs(min_id, max_id):
    syncer_explorer = ExplorerService(init_syncer_rds_gateway(CONFIG))
    return await syncer_explorer.execute_query(queries.get_outputs_query(min_id, max_id))