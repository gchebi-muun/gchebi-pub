def get_beam_messages_query(start_date, end_date):
    return f'''
            with simplified as (
            select
                channel,
                session,
                (message::json -> 'nextTransactionSize' ->> 'sizeProgression')::jsonb as 
                size_progression,
--                 message::json -> 'nextTransactionSize' as size_progression,

                "createdAt" as timestamp
            from notifications
            where message::json -> 'nextTransactionSize' is not null
            and "createdAt" between '{start_date}' and '{end_date}'
        ),

        extracted_fields as (select
                                 channel,
                                 session,
                                 timestamp,
                                 (jsonb_path_query_array(size_progression, 
                                 '$.amountInSatoshis'))::varchar as amounts,
                                 (jsonb_path_query_array(size_progression, 
                                 '$.sizeInBytes'))::varchar      as sizes,
                                 (jsonb_path_query_array(size_progression, 
                                 '$.status'))::varchar           as statuses
                             from simplified)
        select * from extracted_fields
--         limit 10000
    '''
    # return '''select * from notifications limit 10'''


def get_swaps_query(start_date, end_date):

    return f'''
        select
            uuid as swap_uuid,
            "senderUuid" as sender_uuid,
            "destinationNodeAlias" as destination_node_alias,
            status,
            "createdAt" as timestamp,
            "debtType" as debt_type,
            "outputAmountInSat" as output_amount,
            "amountInSat" as amount,
            "debtAmountInSats" as debt_amount,
            "outputDustPaddingInSat" as output_dust_padding,
            "actualFeeInSat" as actual_lightning_fee,
            "estimatedFeeInSat" as estimated_lightning_fee,
            "transactionId" as tx_hash,
            "outputIndex" as tx_output_index
    
        from swaps
        left join funding_outputs fo on fo.id = swaps."fundingOutput"
        left join lightning_payments lp on swaps."lightningPayment" = lp.id
        where "createdAt" between '{start_date}' and '{end_date}'
    '''


def get_transactions_query(start_date, end_date):
    return f'''
    select
        hash as tx_hash,
        "amountInSat" as tx_amount,
        "feeInSat" as tx_fee,
        "createdAt" as tx_timestamp,
        status as tx_status,
        "sizeInVbytes" as tx_size,
        index as tx_output_index
    from transactions
    left join transaction_outputs t on transactions.id = t.transaction
    where "createdAt" between '{start_date}' and '{end_date}'
    '''


def get_users_and_sessions_query(user_creation_date_start, user_creation_date_end):
    return f'''
        select
        id as user_id,
        u.creation_date as user_creation_date,
        u.uuid as user_uuid,
        s.uuid as session_uuid
    from users u
    left join sessions s on u.id = s.user_id
    where u.creation_date between '{user_creation_date_start}' and '{user_creation_date_end}'
    '''


def get_onchain_transactions_query(start_date, end_date):
    return f'''
        select 
        sender as user_id,
        creation_date as timestamp,
        amount_in_satoshis as amount,
        fee_in_satoshis as onchain_fee
        from operations
        where creation_date between '{start_date}' and '{end_date}' 
        and sender is not null
        and swap_uuid is null
    '''


def get_distinct_channels_from_notifications_query():
    return f'''
    select distinct channel from notifications
    '''


def get_distinct_channels_from_archived_notifications_query():
    return f'''
    select distinct channel from archived_notifications
    '''


def get_balance_history_for_channels_query(channels):
    return f'''
    select
        channel,
        "createdAt" as timestamp,
        (jsonb_path_query_array((message::json -> 'nextTransactionSize' ->> 'sizeProgression')::jsonb,
                                     '$.amountInSatoshis') ->> -1)::int as balance
    from notifications
    where channel in ({channels})
    and message::json -> 'nextTransactionSize' is not null
    
    union
    
        select
        channel,
        "createdAt" as timestamp,
        (jsonb_path_query_array((message::json -> 'nextTransactionSize' ->> 'sizeProgression')::jsonb,
                                     '$.amountInSatoshis') ->> -1)::int as balance
    from archived_notifications
    where channel in ({channels})
    and message::json -> 'nextTransactionSize' is not null
    '''


def get_user_id_to_channel_id_query():
    return '''
    select distinct
        uuid as channel,
        user_id as user_id
    from sessions
    '''


def get_diff_balances_query(table_name, channels):
    return f'''
        with all_notifications as (
        select * from {table_name}
        where channel in ({channels})
    ),
    simplified as (
                select
                    channel,
                    "createdAt" as timestamp,
                    coalesce((jsonb_path_query_array((message::json -> 'nextTransactionSize' ->> 'sizeProgression')::jsonb,
                                     '$.amountInSatoshis') ->> -1)::bigint, 0) as balance
    
                from all_notifications
                where message::json -> 'nextTransactionSize' is not null
            ),
    
    with_lags as(
    select *, lag(balance) over (partition by channel order by timestamp desc) as prev_balance from simplified)
    
    select * from with_lags
    where prev_balance is null or prev_balance != balance
    '''

def get_download_data_query(table_name, start_date, end_date):
    return f'''
    select
        channel,
        "createdAt" as timestamp,
        coalesce((jsonb_path_query_array((message::json -> 'nextTransactionSize' ->> 'sizeProgression')::jsonb,
                                     '$.amountInSatoshis') ->> -1)::bigint, 0) as balance
    from {table_name}
    where message::json -> 'nextTransactionSize' is not null
    and "createdAt" between '{start_date}' and '{end_date}'
    '''


def get_download_data_by_channel_query(table_name, min_channel, max_channel):
    return f'''
    select
        channel,
        "createdAt" as timestamp,
        coalesce((jsonb_path_query_array((message::json -> 'nextTransactionSize' ->> 'sizeProgression')::jsonb,
                                     '$.amountInSatoshis') ->> -1)::bigint, 0) as balance
    from {table_name}
    where message::json -> 'nextTransactionSize' is not null
    and channel between '{min_channel}' and '{max_channel}'
    '''


def get_inputs_query(min_spent_output, max_spent_output):
    return f'''
    select
        t_o."ownerUuid" as owner_uuid,
        - t_o."amountInSat" as amount_in_sat,
        t."createdAt" as timestamp,
        t.hash as hash
    from transaction_inputs t_i
    left join transaction_outputs t_o on t_i."spentOutput" = t_o.id
    left join transactions t on t_i.transaction = t.id
    where t_i."spentOutput" between {min_spent_output} and {max_spent_output}
    and t.status not in ('FAILED', 'DROPPED', 'IRREVERSIBLY_DROPPED')
    '''


def get_outputs_query(min_id, max_id):
    return f'''
    select
        "ownerUuid" as onwer_uuid,
        "amountInSat" as amount_in_sat,
        t."createdAt" as timestamp,
        t.hash as hash
    from transaction_outputs t_o
    left join transactions t on t_o.transaction = t.id
    where t_o.id between {min_id} and {max_id} 
    and "ownerUuid" is not null
    and t.status not in ('FAILED', 'DROPPED', 'IRREVERSIBLY_DROPPED')
    '''
