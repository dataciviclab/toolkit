with
comuni_registry as (
  select codice_istat, regione, denominazione, superficie_km2
  from read_parquet('https://storage.googleapis.com/dataciviclab-clean/comuni_master/2026/comuni_master_2026_clean.parquet')
),
-- Mappa provincia (prime 3 cifre) -> regione, per fallback su comuni non in registry
provincia_map as (
  select distinct left(codice_istat, 3) as codice_provincia, regione
  from comuni_registry
  where codice_istat is not null
),
per_comune as (
  select
    codice_comune,
    comune,
    sum(popolazione_residente) as pop_residente,
    sum(totale_maschi) as pop_maschile,
    sum(totale_femmine) as pop_femminile
  from clean_input
  where codice_comune is not null
  group by codice_comune, comune
),
per_comune_arricchito as (
  select
    pc.*,
    -- Match: 1) per codice ISTAT, 2) per denominazione, 3) per provincia
    coalesce(r1.regione, r2.regione, pr.regione, 'Altro') as regione,
    coalesce(r1.superficie_km2, r2.superficie_km2, 0.0) as superficie_km2
  from per_comune pc
  left join comuni_registry r1 on pc.codice_comune = r1.codice_istat
  left join comuni_registry r2 on lower(trim(pc.comune)) = lower(trim(r2.denominazione))
  left join provincia_map pr on left(pc.codice_comune, 3) = pr.codice_provincia
)
select
  {year} as anno_riferimento,
  regione,
  sum(pop_residente) as popolazione_residente,
  sum(pop_maschile) as popolazione_maschile,
  sum(pop_femminile) as popolazione_femminile,
  count(*) as numero_comuni,
  round(sum(superficie_km2), 2) as superficie_km2,
  round(sum(pop_residente)::double / nullif(sum(superficie_km2), 0), 1) as densita_ab_km2
from per_comune_arricchito
group by regione
order by regione
