with unnested as (
select user_id, TIMESTAMP_MICROS(event_timestamp) as ts, event_name, key as k, value.string_value as v
from `muun-58bf8.analytics_151919984.events_202402*`
CROSS JOIN UNNEST(event_params) AS events
and event_name like '%new_op%'
and user_id is not null
),

w_concat as (
  select user_id, ts, concat(event_name, '-', k) as event_key, v
  from unnested
)

select * from w_concat
pivot (min(SPLIT(v, ';')[OFFSET(0)]) for event_key in ('s_new_op_confirmation-routingFeeInSat','s_new_op_confirmation-onchainFee','s_new_op_confirmation-sats_per_virtual_byte', 's_new_op_confirmation-fee', 's_new_op_confirmation-amount','s_new_op_confirmation-type','s_new_op_confirmation-outputAmountInSat','s_new_op_confirmation-total','s_new_op_confirmation-outputPaddingInSat', 's_new_op_error-type', 'e_new_op_action-type', 's_new_op_loading-type'))
order by user_id, ts