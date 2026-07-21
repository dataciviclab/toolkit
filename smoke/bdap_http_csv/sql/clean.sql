WITH base AS (
  SELECT
    cast_int("ANNO") AS anno,
    cast_double("RISPARMIO_PUBBLICO") AS risparmio_pubblico,
    cast_double("SALDO_NETTO") AS saldo_netto,
    cast_double("INDEBITAMENTO_NETTO") AS indebitamento_netto,
    cast_double("RICORSO_MERCATO") AS ricorso_mercato,
    cast_double("AVANZO_PRIMARIO") AS avanzo_primario,
    cast_double("SPESE_CORRENTI") AS spese_correnti,
    cast_double("SPESE_INTERESSI") AS spese_interessi,
    cast_double("SPESE_CONTO_CAPITALE") AS spese_conto_capitale,
    cast_double("SPESE_ACQ_ATT_FINE") AS spese_acq_att_fin,
    cast_double("SPESE_RIMBORSO_PRESTITI") AS spese_rimborso_prestiti,
    cast_double("SPESE_COMPLESSIVE") AS spese_complessive,
    cast_double("SPESE_FINALI") AS spese_finali,
    cast_double("SPESE_FIN_NETTO_ATT_FIN") AS spese_fin_netto_att_fin,
    cast_double("ENTRATE_TRIBUTARIE") AS entrate_tributarie,
    cast_double("ENTRATE_EXTRA_TRIBUTARIE") AS entrate_extra_tributarie,
    cast_double("ENTR_ALIEN_PATR_RISCOS") AS entr_alien_patr_riscos,
    cast_double("RISCOSSIONE_CREDITI") AS riscossione_crediti,
    cast_double("ENTR_ACCENSIONE_PRESTITI") AS entr_accensione_prestiti,
    cast_double("ENTRATE_FINALI") AS entrate_finali,
    cast_double("ENTR_FIN_NETTO_RISCO_CRED") AS entr_fin_netto_risco_cred,
    cast_double("ENTRATE_CORRENTI") AS entrate_correnti
  FROM raw_input
)

SELECT *
FROM base
WHERE anno IS NOT NULL;
