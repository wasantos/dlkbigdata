INSERT INTO ?tableName
WITH
  agg_aniocampanaprimerpedidoweb AS (
    SELECT *
    FROM (
      SELECT
        CODEBELISTA,
        ANIOCAMPANA,
        PT_COUNTRY,
        ROW_NUMBER() OVER(PARTITION BY CODEBELISTA ORDER BY ANIOCAMPANA ASC) AS RK
      FROM ?landingSchema.tbpq_sicc_dnrodocumento
      WHERE CANALINGRESO IN ('WEB', 'WMK')
        AND PT_COUNTRY = :country 
    ) ordered
    WHERE RK = 1
  ),
  agg_flagcorreovalidado AS (
    SELECT
      CODEBELISTA, PT_COUNTRY,
      CAST(BOOL_OR(CORREOVALIDADO = 1) AS SMALLINT) AS FLAGCORREOVALIDADO
    FROM ?landingSchema.tbpq_digital_flogingresoportal
    GROUP BY CODEBELISTA, PT_COUNTRY
  )
SELECT
  sicc_debelista.ANIOCAMPANAINGRESO,
  agg_aniocampanaprimerpedidoweb.ANIOCAMPANA,
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
  agg_flagcorreovalidado.FLAGCORREOVALIDADO,
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
