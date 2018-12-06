INSERT INTO ?tableName
SELECT
  PT_COUNTRY,
  ANIOCAMPANA,
  CODVENTAAPOYADOR,
  CODTIPOOFERTAAPOYADOR,
  CODPRODUCTOAPOYADOR,
  CODCANALVENTAAPOYADOR,
  CODVENTAAPOYADO,
  CODTIPOOFERTAAPOYADO,
  CODPRODUCTOAPOYADO,
  CODCANALVENTAAPOYADO
FROM ?landingSchema.tbpq_sicc_dapoyoproducto
WHERE PT_COUNTRY = :country
  AND ANIOCAMPANA IN (:campaign)
