INSERT INTO ?tableName
SELECT
  NULL AS ANIOCAMPANA,
  CODCONTROL,
  PT_COUNTRY,
  GETDATE()
FROM ?landingSchema.tbpq_planit_dcontrol
WHERE PT_COUNTRY = :country
