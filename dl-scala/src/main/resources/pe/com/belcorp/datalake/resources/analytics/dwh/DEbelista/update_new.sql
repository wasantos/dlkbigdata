UPDATE ?tableName
SET
  ANIOCAMPANAPRIMERPEDIDOWEB = update_source.ANIOCAMPANA,
  FLAGCORREOVALIDADO = update_source.FLAGCORREOVALIDADO
FROM (
  SELECT
    tmp.CODEBELISTA, tmp.CODPAIS,
    agg_flagcorreovalidado.FLAGCORREOVALIDADO,
    agg_aniocampanaprimerpedidoweb.ANIOCAMPANA
  FROM
    ?tableName tmp
  LEFT JOIN ?functionalSchema.dwh_debelista dwh_debelista
    ON tmp.CODEBELISTA = dwh_debelista.CODEBELISTA
    AND tmp.CODPAIS = dwh_debelista.CODPAIS
  INNER JOIN  (
    SELECT *
    FROM (
      SELECT
        CODEBELISTA,
        ANIOCAMPANA,
        PT_COUNTRY AS CODPAIS,
        ROW_NUMBER() OVER(PARTITION BY CODEBELISTA ORDER BY ANIOCAMPANA ASC) AS RK
      FROM ?landingSchema.tbpq_sicc_dnrodocumento
      WHERE CANALINGRESO IN ('WEB', 'WMK')
        AND PT_COUNTRY = :country
    ) ordered
    WHERE RK = 1
  ) agg_aniocampanaprimerpedidoweb
    ON tmp.CODEBELISTA = agg_aniocampanaprimerpedidoweb.CODEBELISTA
    AND tmp.CODPAIS = agg_aniocampanaprimerpedidoweb.CODPAIS
  INNER JOIN (
    SELECT
      CODEBELISTA, PT_COUNTRY AS CODPAIS,
      CAST(BOOL_OR(CORREOVALIDADO = 1) AS SMALLINT) AS FLAGCORREOVALIDADO
    FROM ?landingSchema.tbpq_digital_flogingresoportal
    GROUP BY CODEBELISTA, PT_COUNTRY
  ) agg_flagcorreovalidado
    ON tmp.CODEBELISTA = agg_flagcorreovalidado.CODEBELISTA
    AND tmp.PT_COUNTRY = agg_flagcorreovalidado.CODPAIS
  WHERE
    dwh_debelista.CODEBELISTA IS NULL AND
    dwh_debelista.CODPAIS IS NULL
) update_source
WHERE ?tableName.CODEBELISTA = update_source.CODEBELISTA
  AND ?tableName.CODPAIS = update_source.CODPAIS
