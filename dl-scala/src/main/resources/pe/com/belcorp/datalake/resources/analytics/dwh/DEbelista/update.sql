INSERT INTO ?tableName
SELECT
  sicc_debelista.ANIOCAMPANAINGRESO,
  NULL AS ANIOCAMPANAPRIMERPEDIDO,
  sicc_debelista.ANIOCAMPANAULTIMOPEDIDO,
  sicc_debelista.CODEBELISTA,
  sicc_debelista.DESESTADOCIVIL,
  sicc_debelista.DESNSE,
  sicc_debelista.DESAPEMATERNO,
  sicc_debelista.DESAPEPATERNO,
  sicc_debelista.DESNOMBRE,
  TO_DATE(CAST(sicc_debelista.FECHANACIMIENTO AS VARCHAR), 'YYYYMMDD'),
  CAST(sicc_debelista.FLAGGERENTEZONA AS SMALLINT),
  bi_debelista.ANIOCAMPANAPRIMERPEDIDO,
  sicc_debelista.PT_COUNTRY,
  sicc_debelista.DESAPENOM,
  sicc_debelista.DESLIDER,
  sicc_debelistadatosadic.TELEFONOMOVIL,
  CAST(sicc_debelista.FLAGDIGITAL AS SMALLINT),
  sicc_debelista.TIPODOCIDENTIDAD,
  sicc_debelista.DOCIDENTIDAD,
  NULL AS FLAGCORREOVALIDADO,
  sicc_debelista.DESDIRECCION,
  sicc_debelistadatosadic.CORREOELECTRONICO,
  DATEDIFF(
    years,
    TO_DATE(CAST(sicc_debelista.FECHANACIMIENTO AS VARCHAR), 'YYYYMMDD'),
    CURRENT_DATE
  ),
  CAST(
    (COALESCE(sicc_debelistadatosadic.TELEFONOMOVIL, '') != '')
  AS SMALLINT),
  sicc_debelista.ANIOCAMPANAREGISTRO,
  TO_DATE(sicc_debelista.FECHAREGISTRO, 'YYYYMMDD') AS FECHAREGISTRO
FROM
  ?landingSchema.tbpq_sicc_debelista sicc_debelista
  LEFT JOIN ?landingSchema.tbpq_sicc_debelistadatosadic sicc_debelistadatosadic
    ON sicc_debelista.CODEBELISTA = sicc_debelistadatosadic.CODEBELISTA
    AND sicc_debelista.PT_COUNTRY = sicc_debelistadatosadic.PT_COUNTRY
  LEFT JOIN ?landingSchema.tbpq_bi_debelista bi_debelista
    ON sicc_debelista.CODEBELISTA = bi_debelista.CODEBELISTA
    AND sicc_debelista.PT_COUNTRY = bi_debelista.PT_COUNTRY
  LEFT JOIN agg_aniocampanaprimerpedidoweb
    ON sicc_debelista.CODEBELISTA = agg_aniocampanaprimerpedidoweb.CODEBELISTA
    AND sicc_debelista.PT_COUNTRY = agg_aniocampanaprimerpedidoweb.PT_COUNTRY
  LEFT JOIN agg_flagcorreovalidado
    ON sicc_debelista.CODEBELISTA = agg_flagcorreovalidado.CODEBELISTA
    AND sicc_debelista.PT_COUNTRY = agg_flagcorreovalidado.PT_COUNTRY
WHERE
  sicc_debelista.PT_COUNTRY = :country
