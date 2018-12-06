INSERT INTO ?tableName
SELECT
  CAST(CODORIGENPEDIDOWEB AS INTEGER),
  DESORIGENPEDIDOWEB,
  CAST(CODPOPUP AS BIGINT),
  DESPOPUP,
  CAST(FLAGPERSONALIZACION AS SMALLINT),
  CAST(CODAREA AS BIGINT),
  DESAREA,
  CAST(CODMEDIOTEC AS BIGINT),
  DESMEDIOTEC,
  CAST(CODESPACIO AS BIGINT),
  DESESPACIO,
  PT_COUNTRY
FROM ?landingSchema.tbpq_digital_dorigenpedidoweb
WHERE PT_COUNTRY = :country