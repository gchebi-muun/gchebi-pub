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
