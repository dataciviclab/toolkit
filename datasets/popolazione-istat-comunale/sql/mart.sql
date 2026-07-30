select
  {year} as anno_riferimento,
  codice_comune,
  comune,
  sum(popolazione_residente) as popolazione_residente,
  sum(totale_maschi) as popolazione_maschile,
  sum(totale_femmine) as popolazione_femminile
from clean_input
where codice_comune is not null
  and comune is not null
group by codice_comune, comune
