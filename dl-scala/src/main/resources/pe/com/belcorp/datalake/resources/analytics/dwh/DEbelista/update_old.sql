UPDATE ?tableName
SET
  ANIOCAMPANAPRIMERPEDIDOWEB = dwh_debelista.ANIOCAMPANA,
  FLAGCORREOVALIDADO = dwh_debelista.FLAGCORREOVALIDADO
FROM
  ?tableName tmp
INNER JOIN ?functionalSchema.dwh_debelista dwh_debelista
  ON tmp.CODEBELISTA = dwh_debelista.CODEBELISTA
  AND tmp.CODPAIS = dwh_debelista.CODPAIS
WHERE dwh_debelista.CODPAIS = :country
