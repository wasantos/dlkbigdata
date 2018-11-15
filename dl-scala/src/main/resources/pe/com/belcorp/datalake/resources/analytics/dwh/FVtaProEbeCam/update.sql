INSERT INTO ?tableName
SELECT
  sicc_fvtaproebecam.ANIOCAMPANA,
  sicc_fvtaproebecam.CODCANALVENTA,
  sicc_fvtaproebecam.CODEBELISTA,
  sicc_fvtaproebecam.CODTERRITORIO,
  sicc_fvtaproebecam.CODTIPODOCUMENTO,
  sicc_fvtaproebecam.CODTIPOOFERTA,
  sicc_fvtaproebecam.CODVENTA,
  CAST(sicc_fvtaproebecam.DESCUENTO AS DECIMAL(15,5)),
  CAST(sicc_fvtaproebecam.OPORTUNIDADAHORRO AS DECIMAL(15,5)),
  sicc_fvtaproebecam.NRODOCUMENTO,
  TO_DATE(
    CAST(sicc_fvtaproebecam.FECHAPROCESO AS VARCHAR), 'YYYYMMDD'
  ),
  CAST(sicc_fvtaproebecam.REALANULMNNETO AS DECIMAL(15,5)),
  CAST(sicc_fvtaproebecam.REALDEVMNNETO AS DECIMAL(15,5)),
  CAST(sicc_fvtaproebecam.REALUUANULADAS AS BIGINT),
  CAST(sicc_fvtaproebecam.REALUUDEVUELTAS AS BIGINT),
  CAST(sicc_fvtaproebecam.REALUUFALTANTES AS BIGINT),
  CAST(sicc_fvtaproebecam.REALUUVENDIDAS AS BIGINT),
  CAST(sicc_fvtaproebecam.REALVTAMNFACTURA AS DECIMAL(15,5)),
  CAST(sicc_fvtaproebecam.REALVTAMNFALTNETO AS DECIMAL(15,5)),
  CAST(sicc_fvtaproebecam.REALVTAMNNETO AS DECIMAL(15,5)),
  CAST(sicc_fvtaproebecam.REALUUANULADAS AS DECIMAL(15,5))
    * CAST(sicc_dmatrizcampana.PRECIOOFERTA AS DECIMAL(15,5)) AS PPREALUUANULADAS,
  CAST(sicc_fvtaproebecam.REALUUDEVUELTAS AS DECIMAL(15,5))
    * CAST(sicc_dmatrizcampana.PRECIOOFERTA AS DECIMAL(15,5)) AS PPREALUUDEVUELTAS,
  CAST(sicc_fvtaproebecam.REALUUVENDIDAS AS DECIMAL(15,5))
    * CAST(sicc_dmatrizcampana.PRECIOOFERTA AS DECIMAL(15,5)) AS PPREALUUVENDIDAS,
  CAST(sicc_fvtaproebecam.REALUUFALTANTES AS DECIMAL(15,5))
    * CAST(sicc_dmatrizcampana.PRECIOOFERTA AS DECIMAL(15,5)) AS PPREALUUFALTANTES,
  CAST(planit_dcostoproductocampana.COSTOREPOSICIONMN AS DECIMAL(15,5)),
  CAST(planit_fnumpedcam.REALTC AS DECIMAL(15,5)),
  CAST(planit_fnumpedcam.ESTTC AS DECIMAL(15,5)),
  CAST(sicc_fvtaproebecam.CANALINGRESO AS INTEGER),
  sicc_fvtaproebecam.PT_COUNTRY,
  sicc_fvtaproebecam.CODPRODUCTO,
  sicc_fvtaproebecam.CODIGOPALANCA,
  digital_dorigenpedidoweb.DESORIGENPEDIDOWEB,
  COALESCE(sicc_fvtaproebecam.ANIOCAMPANAREF, sicc_fvtaproebecam.ANIOCAMPANA) AS ANIOCAMPANAREF,
  NULL AS MEDIOVENTA
FROM
  ?landingSchema.tbpq_sicc_fvtaproebecam sicc_fvtaproebecam
    LEFT JOIN ?landingSchema.tbpq_planit_dcostoproductocampana planit_dcostoproductocampana
      ON sicc_fvtaproebecam.ANIOCAMPANA = planit_dcostoproductocampana.ANIOCAMPANA
      AND sicc_fvtaproebecam.CODPRODUCTO = planit_dcostoproductocampana.CODPRODUCTO
      AND sicc_fvtaproebecam.PT_COUNTRY = planit_dcostoproductocampana.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_planit_fnumpedcam planit_fnumpedcam
      ON sicc_fvtaproebecam.ANIOCAMPANA = planit_fnumpedcam.ANIOCAMPANA
      AND sicc_fvtaproebecam.PT_COUNTRY = planit_fnumpedcam.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_digital_dorigenpedidoweb digital_dorigenpedidoweb
      ON sicc_fvtaproebecam.CANALINGRESO = digital_dorigenpedidoweb.CODORIGENPEDIDOWEB
      AND sicc_fvtaproebecam.PT_COUNTRY = digital_dorigenpedidoweb.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_sicc_dmatrizcampana sicc_dmatrizcampana
      ON (COALESCE(sicc_fvtaproebecam.ANIOCAMPANAREF, sicc_fvtaproebecam.ANIOCAMPANA) =
        sicc_dmatrizcampana.ANIOCAMPANA)
      AND sicc_fvtaproebecam.CODTIPOOFERTA = sicc_dmatrizcampana.CODTIPOOFERTA
      AND sicc_fvtaproebecam.CODPRODUCTO = sicc_dmatrizcampana.CODPRODUCTO
      AND sicc_fvtaproebecam.CODVENTA = sicc_dmatrizcampana.CODVENTA
      AND sicc_fvtaproebecam.PT_COUNTRY = sicc_dmatrizcampana.PT_COUNTRY
WHERE
  sicc_fvtaproebecam.PT_COUNTRY = :country
  AND sicc_fvtaproebecam.ANIOCAMPANA IN (:campaign)
