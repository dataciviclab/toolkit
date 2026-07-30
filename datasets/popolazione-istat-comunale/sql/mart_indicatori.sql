with
base as (
  select
    codice_comune,
    comune,
    eta,
    popolazione_residente
  from clean_input
  where codice_comune is not null
    and comune is not null
    and eta is not null
    and eta <> 999
),
fasce as (
  select
    codice_comune,
    comune,
    sum(case when eta between 0 and 14 then popolazione_residente else 0 end) as pop_0_14,
    sum(case when eta between 15 and 64 then popolazione_residente else 0 end) as pop_15_64,
    sum(case when eta >= 65 then popolazione_residente else 0 end) as pop_65_plus,
    sum(case when eta < 18 then popolazione_residente else 0 end) as pop_under_18,
    sum(case when eta >= 75 then popolazione_residente else 0 end) as pop_75_plus,
    sum(popolazione_residente) as popolazione_totale
  from base
  group by codice_comune, comune
)
select
  {year} as anno_riferimento,
  codice_comune,
  comune,
  popolazione_totale,
  pop_0_14,
  pop_15_64,
  pop_65_plus,
  pop_under_18,
  pop_75_plus,
  -- Indice di vecchiaia: pop 65+ / pop 0-14 * 100
  case when pop_0_14 > 0
    then round((pop_65_plus::double / pop_0_14::double) * 100, 1)
  end as indice_vecchiaia,
  -- Rapporto di dipendenza: (pop 0-14 + pop 65+) / pop 15-64 * 100
  case when pop_15_64 > 0
    then round(((pop_0_14 + pop_65_plus)::double / pop_15_64::double) * 100, 1)
  end as rapporto_dipendenza,
  -- Percentuale under 18
  case when popolazione_totale > 0
    then round((pop_under_18::double / popolazione_totale::double) * 100, 1)
  end as pct_under_18,
  -- Percentuale over 65
  case when popolazione_totale > 0
    then round((pop_65_plus::double / popolazione_totale::double) * 100, 1)
  end as pct_over_65,
  -- Percentuale over 75
  case when popolazione_totale > 0
    then round((pop_75_plus::double / popolazione_totale::double) * 100, 1)
  end as pct_over_75
from fasce
where popolazione_totale > 0
