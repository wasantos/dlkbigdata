INSERT INTO ?tableName
SELECT
  PT_COUNTRY,
  CAST(CODCOMPORTAMIENTO AS SMALLINT),
  DESNIVELCOMPORTAMIENTO,
  DESCOMPORTAMIENTO,
  DESABRCOMPORTAMIENTO,
  CAST(FLAGPERIODO AS SMALLINT)
FROM ?landingSchema.tbpq_bi_dcomportamientorolling
WHERE PT_COUNTRY = :country