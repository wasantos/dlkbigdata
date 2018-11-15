INSERT INTO ?tableName
SELECT
  sicc_dmatrizcampana.ANIOCAMPANA,
  sicc_dmatrizcampana.CODCANALVENTA,
  sicc_dmatrizcampana.CODCATALOGO,
  sicc_dmatrizcampana.CODESTRATEGIA,
  sicc_dmatrizcampana.CODTIPOOFERTA,
  sicc_dmatrizcampana.CODVENTA,
  sicc_dmatrizcampana.DESCATALOGO,
  sicc_dmatrizcampana.DESTIPOOFERTA,
  CAST(sicc_dmatrizcampana.NROPAGINA AS BIGINT),
  CAST(sicc_dmatrizcampana.NUMOFERTA AS BIGINT),
  CAST(sicc_dmatrizcampana.PRECIONORMALMN AS DECIMAL(15,5)),
  CAST(sicc_dmatrizcampana.PRECIOOFERTA AS DECIMAL(15,5)),
  CAST(sicc_dmatrizcampana.PrecioVtaPropuesto AS DECIMAL(15,5)),
  planit_dmatrizcampana.CODTIPOCATALOGO,
  planit_dmatrizcampana.DESARGVENTA,
  planit_dmatrizcampana.DESEXPOSICION,
  planit_dmatrizcampana.DESLADOPAG,
  planit_dmatrizcampana.DESTIPOCATALOGO,
  planit_dmatrizcampana.DESUBICACIONCATALOGO,
  CAST(COALESCE(planit_dmatrizcampana.FOTOMODELO, '') != '' AS SMALLINT) AS FLAGFOTOMODELO,
  CAST(COALESCE(planit_dmatrizcampana.FOTOPRODUCTO, '') != '' AS SMALLINT) AS FLAGFOTOPRODUCTO,
  CAST(planit_dmatrizcampana.NROPAGINAS AS BIGINT),
  CAST(planit_dmatrizcampana.PAGINACATALOGO AS BIGINT),
  planit_dmatrizcampana.DESOBSERVACIONES,
  bi_dcatalogovehiculo.VEHICULOVENTA,
  sicc_dmatrizcampana.PT_COUNTRY,
  sicc_dmatrizcampana.CODPRODUCTO,
  sicc_dmatrizcampana.CODTIPOMEDIOVENTA,
  CAST(planit_dmatrizcampana.DEMANDAANORMALNPLAN AS DECIMAL(15,5)),
  sicc_dmatrizcampana.DESESTRATEGIA,
  planit_dmatrizcampana.DESTIPODIAGRAMACION,
  sicc_dmatrizcampana.FACTORCUADRE,
  sicc_dmatrizcampana.FACTORREPETICION,
  planit_dmatrizcampana.FLAGDISCOVER,
  sicc_dmatrizcampana.FLAGESTADISTICABLE,
  sicc_dmatrizcampana.FLAGPRODUCTOSEBE,
  sicc_dmatrizcampana.INDCUADRE,
  sicc_dmatrizcampana.INDPADRE,
  CAST(planit_dmatrizcampana.PRECIONORMALDOLPLAN AS DECIMAL(15,5)) AS PRECIONORMALDOLPLAN,
  CAST(planit_dmatrizcampana.PRECIONORMALMNPLAN AS DECIMAL(15,5)) AS PRECIONORMALMNPLAN,
  CAST(planit_dmatrizcampana.PRECIOOFERTADOLPLAN AS DECIMAL(15,5)) AS PRECIOOFERTADOLPLAN,
  CAST(planit_dmatrizcampana.PRECIOOFERTAMNPLAN AS DECIMAL(15,5)) AS PRECIOOFERTAMNPLAN
FROM
  ?landingSchema.tbpq_sicc_dmatrizcampana sicc_dmatrizcampana
    LEFT JOIN ?landingSchema.tbpq_planit_dmatrizcampana planit_dmatrizcampana
      ON sicc_dmatrizcampana.ANIOCAMPANA = planit_dmatrizcampana.ANIOCAMPANA
      AND sicc_dmatrizcampana.CODTIPOOFERTA = planit_dmatrizcampana.CODTIPOOFERTA
      AND sicc_dmatrizcampana.CODPRODUCTO = planit_dmatrizcampana.CODPRODUCTO
      AND sicc_dmatrizcampana.PT_COUNTRY = planit_dmatrizcampana.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_bi_dcatalogovehiculo bi_dcatalogovehiculo
      ON sicc_dmatrizcampana.CODCATALOGO = bi_dcatalogovehiculo.CODCATALOGO
      AND sicc_dmatrizcampana.PT_COUNTRY = bi_dcatalogovehiculo.PT_COUNTRY
WHERE
  sicc_dmatrizcampana.PT_COUNTRY = :country
  AND sicc_dmatrizcampana.ANIOCAMPANA IN (:campaign)
