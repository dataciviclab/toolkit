select
  TIME_PERIOD,
  REF_AREA,
  REF_AREA_label,
  try_cast(value as bigint) as residenti
from raw_input
where TIME_PERIOD = '2024'
