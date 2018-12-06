INSERT INTO ?tableName
SELECT
  ANIOCAMPANA,
  TO_DATE(FECHA, 'YYYYMMDD'),
  PT_COUNTRY,
  GETDATE()
FROM ?landingSchema.tbpq_sicc_dcampcer
WHERE PT_COUNTRY = :country
