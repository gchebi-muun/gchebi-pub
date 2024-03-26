import configparser
from curiosity.app.services.explorer_service import ExplorerService
from curiosity.helpers import (
    init_beam_rds_gateway,
    init_houston_rds_gateway,
    init_swapper_rds_gateway,
    init_syncer_rds_gateway,
)
import queries
import os

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
