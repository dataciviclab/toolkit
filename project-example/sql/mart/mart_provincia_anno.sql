SELECT
  anno,
  regione,
  provincia,
  COUNT(*) AS n_comuni,
  AVG(pct_rd) AS pct_rd_avg,
  SUM(ru_tot_t) AS ru_tot_t_sum
FROM clean_input
GROUP BY 1,2,3;