INSERT INTO ?tableName
SELECT
  sicc_dgeografiacampana.ANIOCAMPANA,
  sicc_dgeografiacampana.CODGERENTEREGIONAL,
  sicc_dgeografiacampana.CODGERENTEZONA,
  sicc_dgeografiacampana.CODLIDER,
  sicc_dgeografiacampana.CODREGION,
  sicc_dgeografiacampana.CODSECCION,
  sicc_dgeografiacampana.CODTERRITORIO,
  sicc_dgeografiacampana.CODZONA,
  sicc_dgeografiacampana.DESREGION,
  sicc_dgeografiacampana.DESZONA,
  sicc_dgeografiacampana.PT_COUNTRY,
  bi_dpais.DESPAIS,
  sicc_dgeografia.DESDEPARTAMENTO,
  sicc_dgeografia.DESCIUDAD,
  sicc_dgeografia.DESDISTRITO,
  bi_findsociaemp.DESNIVEL,
  bi_findsociaemp.RENDIMIENTOETAPA,
  sicc_dgeografia.DESLIDER,
  sicc_dgeografia.DESGERENTEREGIONAL,
  sicc_dgeografia.DESGERENTEZONA
FROM
  ?landingSchema.tbpq_sicc_dgeografiacampana sicc_dgeografiacampana
  LEFT JOIN ?landingSchema.tbpq_sicc_dgeografia sicc_dgeografia
    ON sicc_dgeografiacampana.CODTERRITORIO = sicc_dgeografia.CODTERRITORIO
    AND sicc_dgeografiacampana.PT_COUNTRY = sicc_dgeografia.PT_COUNTRY
  LEFT JOIN ?landingSchema.tbpq_bi_findsociaemp bi_findsociaemp
    ON sicc_dgeografiacampana.ANIOCAMPANA = bi_findsociaemp.ANIOCAMPANA
    AND sicc_dgeografiacampana.PT_COUNTRY = bi_findsociaemp.PT_COUNTRY
    AND (
      trim(sicc_dgeografiacampana.CODZONA) ||
      trim(sicc_dgeografiacampana.CODSECCION)
    ) = trim(bi_findsociaemp.CODSECCION)
  LEFT JOIN ?landingSchema.tbpq_bi_dpais bi_dpais
    ON bi_dpais.CODPAIS = sicc_dgeografiacampana.PT_COUNTRY
WHERE
  sicc_dgeografiacampana.PT_COUNTRY = :country
  AND sicc_dgeografiacampana.ANIOCAMPANA IN (:campaign)
