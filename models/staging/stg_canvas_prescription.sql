select
      id
    , medication_id
    , created
    , modified
    , written_date
    , sig_original_input
    , dispense_quantity
    , count_of_refills_allowed
    , days_supply
    , committer_id
    , prescriber_id
from {{ source('canvas', 'api_prescription') }}
