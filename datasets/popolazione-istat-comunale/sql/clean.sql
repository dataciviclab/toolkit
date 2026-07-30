select
  {year}::INTEGER                         as anno,
  normalize_string("Codice comune")       as codice_comune,
  normalize_string("Comune")              as comune,
  cast_int("Età")                         as eta,
  case
    when cast_int("Età") between 0 and 14 then '0-14'
    when cast_int("Età") between 15 and 29 then '15-29'
    when cast_int("Età") between 30 and 44 then '30-44'
    when cast_int("Età") between 45 and 59 then '45-59'
    when cast_int("Età") between 60 and 74 then '60-74'
    when cast_int("Età") >= 75 then '75+'
  end                                     as fascia_eta,
  cast_int("Celibi")                      as celibi,
  cast_int("Coniugati")                   as coniugati,
  cast_int("Divorziati")                  as divorziati,
  cast_int("Vedovi")                      as vedovi,
  cast_int("Uniti civilmente")            as uniti_civilmente_maschi,
  cast_int("Maschi già in unione civile (per scioglimento unione)") as maschi_gia_unione_civile_scioglimento,
  cast_int("Maschi già in unione civile (per decesso del partner)") as maschi_gia_unione_civile_decesso,
  cast_int("Totale maschi")               as totale_maschi,
  cast_int("Nubili")                      as nubili,
  cast_int("Coniugate")                   as coniugate,
  cast_int("Divorziate")                  as divorziate,
  cast_int("Vedove")                      as vedove,
  cast_int("Unite civilmente")            as unite_civilmente_femmine,
  cast_int("Femmine già in unione civile (per scioglimento unione)") as femmine_gia_unione_civile_scioglimento,
  cast_int("Femmine già in unione civile (per decesso del partner)") as femmine_gia_unione_civile_decesso,
  cast_int("Totale femmine")              as totale_femmine,
  cast_int("Totale")                      as popolazione_residente
from raw_input
where cast_int("Età") <> 999
