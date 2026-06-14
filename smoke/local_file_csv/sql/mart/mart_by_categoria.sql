SELECT
    COALESCE(categoria, 'non_classificato') AS categoria,
    COUNT(*) AS num_record,
    ROUND(SUM(valore), 1) AS totale,
    ROUND(AVG(valore), 2) AS media
FROM clean_input
GROUP BY categoria
ORDER BY totale DESC
