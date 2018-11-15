INSERT INTO ?tableName
WITH
  agg_flags_factura AS (
    SELECT
      ANIOCAMPANA, CODEBELISTA, PT_COUNTRY,

      -- BOOL_OR is simpler and faster than COUNT(CASE ...) > 0
      -- CAST AS INTEGER is shorter than CASE WHEN ... THEN 1 ELSE 0 END
      CAST(
        BOOL_OR(CANALINGRESO IN ('WEB', 'WMX', 'APP', 'APM', 'APW'))
      AS SMALLINT) AS FLAGPASOPEDIDOWEB,

      CAST(
        BOOL_OR(CODTIPODOCUMENTO = 'N' AND FLAGORDENANULADO = 1)
      AS SMALLINT) AS FLAGPEDIDOANULADO
     FROM ?landingSchema.tbpq_sicc_dnrodocumento
     GROUP BY ANIOCAMPANA, CODEBELISTA, PT_COUNTRY
  ),
  agg_flags_palancas AS (
    SELECT
      sicc_fvtaproebecam.ANIOCAMPANA,
      sicc_fvtaproebecam.CODEBELISTA,
      sicc_fvtaproebecam.PT_COUNTRY,
      -- BOOL_OR is simpler and faster than COUNT(CASE ...) > 0
      -- CAST AS INTEGER is shorter than CASE WHEN ... THEN 1 ELSE 0 END
      CAST(
        BOOL_OR(bi_fresultadopalancas.CODPALANCA = 'FDC')
      AS SMALLINT) AS FLAGCOMPRAFDC,
      CAST(
        BOOL_OR(bi_fresultadopalancas.CODPALANCA = 'ODD')
      AS SMALLINT) AS FLAGCOMPRAODD,
      CAST(
        BOOL_OR(bi_fresultadopalancas.CODPALANCA = 'OF')
      AS SMALLINT) AS FLAGCOMPRAOF,
      CAST(
        BOOL_OR(bi_fresultadopalancas.CODPALANCA = 'OPT')
      AS SMALLINT) AS FLAGCOMPRAOPT,
      CAST(
        BOOL_OR(bi_fresultadopalancas.CODPALANCA = 'SR')
      AS SMALLINT) AS FLAGCOMPRASR,
      CAST(
        BOOL_OR(
          bi_fresultadopalancas.CODPALANCA IN ('FDC', 'ODD', 'OF', 'OPT', 'SR')
        )
      AS SMALLINT) AS FLAGOFERTADIGITAL,

      CAST(
        BOOL_OR(bi_fresultadopalancas.EXPUESTAFDC = '1')
      AS SMALLINT) AS FLAGEXPUESTAFDC,
      CAST(
        BOOL_OR(bi_fresultadopalancas.EXPUESTAODD = '1')
      AS SMALLINT) AS FLAGEXPUESTAODD,
      CAST(
        BOOL_OR(bi_fresultadopalancas.EXPUESTAOF = '1')
      AS SMALLINT) AS FLAGEXPUESTAOF,
      CAST(
        BOOL_OR(bi_fresultadopalancas.EXPUESTAOPT = '1')
      AS SMALLINT) AS FLAGEXPUESTAOPT,
      CAST(
        BOOL_OR(bi_fresultadopalancas.EXPUESTASR = '1')
      AS SMALLINT) AS FLAGEXPUESTASR
    FROM
      ?landingSchema.tbpq_sicc_fvtaproebecam sicc_fvtaproebecam
        LEFT JOIN ?landingSchema.tbpq_bi_fresultadopalancas bi_fresultadopalancas
          ON sicc_fvtaproebecam.ANIOCAMPANA = bi_fresultadopalancas.ANIOCAMPANA
          AND sicc_fvtaproebecam.CODEBELISTA = bi_fresultadopalancas.CODEBELISTA
          AND sicc_fvtaproebecam.CODIGOPALANCA = bi_fresultadopalancas.CODPALANCA
          AND sicc_fvtaproebecam.PT_COUNTRY = bi_fresultadopalancas.PT_COUNTRY
    GROUP BY
      sicc_fvtaproebecam.ANIOCAMPANA,
      sicc_fvtaproebecam.CODEBELISTA,
      sicc_fvtaproebecam.PT_COUNTRY
  ),
  agg_flags_indicadores AS (
    SELECT
      sicc_fvtaproebecam.ANIOCAMPANA AS ANIOCAMPANA,
      sicc_fvtaproebecam.CODEBELISTA AS CODEBELISTA,
      sicc_fvtaproebecam.PT_COUNTRY AS PT_COUNTRY,
      CASE WHEN
        COUNT(CASE WHEN
          sap_dproducto.CODMARCA IN ('01', '02', '03', '06')
          AND sicc_fvtaproebecam.CODTIPODOCUMENTO = 'N'
          AND sicc_fvtaproebecam.REALUUVENDIDAS + sicc_fvtaproebecam.REALUUFALTANTES > 0
          AND sicc_fvtaproebecam.ANIOCAMPANA = sicc_fvtaproebecam.ANIOCAMPANAREF
        THEN 1 END) > 1
      THEN 1 ELSE 0 END AS FLAGMULTIMARCA,

      CAST(
        BOOL_OR(
          sicc_fvtaproebecam.CODTIPODOCUMENTO = 'N'
          AND sicc_fvtaproebecam.REALUUVENDIDAS + sicc_fvtaproebecam.REALUUFALTANTES > 0
          AND sicc_fvtaproebecam.ANIOCAMPANA = sicc_fvtaproebecam.ANIOCAMPANAREF)
      AS SMALLINT) AS FLAGACTIVA,

      CAST(
        BOOL_OR(
          SUBSTRING(sap_dproducto.CODCLASE, 3, 2) = '11'
          AND sicc_fvtaproebecam.CODTIPODOCUMENTO = 'N'
          AND sicc_fvtaproebecam.REALUUVENDIDAS + sicc_fvtaproebecam.REALUUFALTANTES > 0
          AND sicc_fvtaproebecam.ANIOCAMPANA = sicc_fvtaproebecam.ANIOCAMPANAREF)
      AS SMALLINT) AS FLAGPASOPEDIDOFRAGANCIA,

      CAST(
        BOOL_OR(
          SUBSTRING(sap_dproducto.CODCLASE, 3, 2) = '13'
          AND sicc_fvtaproebecam.CODTIPODOCUMENTO = 'N'
          AND sicc_fvtaproebecam.REALUUVENDIDAS + sicc_fvtaproebecam.REALUUFALTANTES > 0
          AND sicc_fvtaproebecam.ANIOCAMPANA = sicc_fvtaproebecam.ANIOCAMPANAREF)
      AS SMALLINT) AS FLAGPASOPEDIDOCUIDADOPERSONAL,

      CAST(
        BOOL_OR(
          SUBSTRING(sap_dproducto.CODCLASE, 3, 2) = '12'
          AND sicc_fvtaproebecam.CODTIPODOCUMENTO = 'N'
          AND sicc_fvtaproebecam.REALUUVENDIDAS + sicc_fvtaproebecam.REALUUFALTANTES > 0
          AND sicc_fvtaproebecam.ANIOCAMPANA = sicc_fvtaproebecam.ANIOCAMPANAREF)
      AS SMALLINT) AS FLAGPASOPEDIDOMAQUILLAJE,

      CAST(
        BOOL_OR(
          SUBSTRING(sap_dproducto.CODCLASE, 3, 2) = '15'
          AND sicc_fvtaproebecam.CODTIPODOCUMENTO = 'N'
          AND sicc_fvtaproebecam.REALUUVENDIDAS + sicc_fvtaproebecam.REALUUFALTANTES > 0
          AND sicc_fvtaproebecam.ANIOCAMPANA = sicc_fvtaproebecam.ANIOCAMPANAREF)
      AS SMALLINT) AS FLAGPASOPEDIDOTRATAMIENTOCORPORAL,

      CAST(
        BOOL_OR(
          SUBSTRING(sap_dproducto.CODCLASE, 3, 2) = '14'
          AND sicc_fvtaproebecam.CODTIPODOCUMENTO = 'N'
          AND sicc_fvtaproebecam.REALUUVENDIDAS + sicc_fvtaproebecam.REALUUFALTANTES > 0
          AND sicc_fvtaproebecam.ANIOCAMPANA = sicc_fvtaproebecam.ANIOCAMPANAREF)
      AS SMALLINT) AS FLAGPASOPEDIDOTRATAMIENTOFACIAL
    FROM
      ?landingSchema.tbpq_sicc_fvtaproebecam sicc_fvtaproebecam
        LEFT JOIN ?landingSchema.tbpq_sap_dproducto sap_dproducto
          ON sicc_fvtaproebecam.CODPRODUCTO = sap_dproducto.CODPRODUCTO
    GROUP BY
      sicc_fvtaproebecam.ANIOCAMPANA,
      sicc_fvtaproebecam.CODEBELISTA,
      sicc_fvtaproebecam.PT_COUNTRY
  )
SELECT
  sicc_fstaebecam.ANIOCAMPANA,
  sicc_fstaebecam.CODCANALVENTA,
  sicc_fstaebecam.CODEBELISTA,
  CAST(sicc_fstaebecam.CODSTATUS AS SMALLINT),
  sicc_fstaebecam.CODTERRITORIO,
  CAST(sicc_fstaebecam.FLAGPASOPEDIDO AS SMALLINT),
  CAST(sicc_fstaebecam.REALNROORDENES AS SMALLINT),
  agg_flags_indicadores.FLAGACTIVA,
  agg_flags_indicadores.FLAGPASOPEDIDOCUIDADOPERSONAL,
  agg_flags_indicadores.FLAGPASOPEDIDOMAQUILLAJE,
  agg_flags_indicadores.FLAGPASOPEDIDOTRATAMIENTOCORPORAL,
  agg_flags_indicadores.FLAGPASOPEDIDOTRATAMIENTOFACIAL,
  agg_flags_factura.FLAGPEDIDOANULADO,
  agg_flags_indicadores.FLAGPASOPEDIDOFRAGANCIA,
  sicc_fstaebecam.PT_COUNTRY,
  sicc_dnrodocumento.CANALINGRESO,
  sicc_fstaebeadic.CODCANALORIGEN,
  agg_flags_indicadores.FLAGMULTIMARCA,
  bi_fstaebecam.CONSTANCIA,
  CAST(bi_fstaebecam.FRECUENCIACOMPRA AS BIGINT),
  CAST(bi_fstaebecam.CODCOMPORTAMIENTOROLLINGREP AS SMALLINT),
  bi_fstaebecam.DESCOMPORTAMIENTOROLLING,
  agg_flags_factura.FLAGPASOPEDIDOWEB,
  COALESCE(
    CAST(digital_fingresosconsultoraportal.INGRESOSTOTALES AS BIGINT)
  , 0),
  CAST(digital_fcompdigcon.FLAGIPUNICOZONA AS SMALLINT),
  agg_flags_palancas.FLAGEXPUESTAODD,
  agg_flags_palancas.FLAGEXPUESTAOF,
  agg_flags_palancas.FLAGEXPUESTAFDC,
  agg_flags_palancas.FLAGEXPUESTASR,
  agg_flags_palancas.FLAGCOMPRAOPT,
  agg_flags_palancas.FLAGCOMPRAODD,
  agg_flags_palancas.FLAGCOMPRAOF,
  agg_flags_palancas.FLAGCOMPRAFDC,
  agg_flags_palancas.FLAGCOMPRASR,
  agg_flags_palancas.FLAGEXPUESTAOPT,
  CAST(sicc_fstaebecam.FLAGDIGITAL AS SMALLINT),
  agg_flags_palancas.FLAGOFERTADIGITAL,
  CAST(NULL AS SMALLINT) AS FLAGREVISTADIGITALSUSCRIPCION,
  CAST(NULL AS SMALLINT) AS FLAGEXPERIENCIAGANAMAS
FROM 
  ?landingSchema.tbpq_sicc_fstaebecam sicc_fstaebecam
    LEFT JOIN ?landingSchema.tbpq_bi_dstatus bi_dstatus
      ON sicc_fstaebecam.CODSTATUS = bi_dstatus.CODSTATUS
      AND sicc_fstaebecam.PT_COUNTRY = bi_dstatus.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_bi_fstaebecam bi_fstaebecam
      ON sicc_fstaebecam.ANIOCAMPANA = bi_fstaebecam.ANIOCAMPANA
      AND sicc_fstaebecam.CODEBELISTA = bi_fstaebecam.CODEBELISTA
      AND sicc_fstaebecam.PT_COUNTRY = bi_fstaebecam.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_sicc_dnrodocumento sicc_dnrodocumento
      ON sicc_fstaebecam.ANIOCAMPANA = sicc_dnrodocumento.ANIOCAMPANA
      AND sicc_fstaebecam.CODEBELISTA = sicc_dnrodocumento.CODEBELISTA
      AND sicc_fstaebecam.PT_COUNTRY = sicc_dnrodocumento.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_sicc_fstaebeadic sicc_fstaebeadic
      ON sicc_fstaebecam.ANIOCAMPANA = sicc_fstaebeadic.ANIOCAMPANA
      AND sicc_fstaebecam.CODEBELISTA = sicc_fstaebeadic.CODEBELISTA
      AND sicc_fstaebecam.PT_COUNTRY = sicc_fstaebeadic.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_digital_fcompdigcon digital_fcompdigcon
      ON sicc_fstaebecam.ANIOCAMPANA = digital_fcompdigcon.ANIOCAMPANA
      AND sicc_fstaebecam.CODEBELISTA = digital_fcompdigcon.CODEBELISTA
      AND sicc_fstaebecam.PT_COUNTRY = digital_fcompdigcon.PT_COUNTRY
    LEFT JOIN ?landingSchema.tbpq_digital_fingresosconsultoraportal digital_fingresosconsultoraportal
      ON sicc_fstaebecam.ANIOCAMPANA = digital_fingresosconsultoraportal.ANIOCAMPANAWEB
      AND sicc_fstaebecam.CODEBELISTA = digital_fingresosconsultoraportal.CONSULTORA
      AND sicc_fstaebecam.PT_COUNTRY = digital_fingresosconsultoraportal.PT_COUNTRY
    LEFT JOIN agg_flags_palancas
      ON sicc_fstaebecam.ANIOCAMPANA = agg_flags_palancas.ANIOCAMPANA
      AND sicc_fstaebecam.CODEBELISTA = agg_flags_palancas.CODEBELISTA
      AND sicc_fstaebecam.PT_COUNTRY = agg_flags_palancas.PT_COUNTRY
    LEFT JOIN agg_flags_factura
      ON sicc_fstaebecam.ANIOCAMPANA = agg_flags_factura.ANIOCAMPANA
      AND sicc_fstaebecam.CODEBELISTA = agg_flags_factura.CODEBELISTA
      AND sicc_fstaebecam.PT_COUNTRY = agg_flags_factura.PT_COUNTRY
    LEFT JOIN agg_flags_indicadores
      ON sicc_fstaebecam.ANIOCAMPANA = agg_flags_indicadores.ANIOCAMPANA
      AND sicc_fstaebecam.CODEBELISTA = agg_flags_indicadores.CODEBELISTA
      AND sicc_fstaebecam.PT_COUNTRY = agg_flags_indicadores.PT_COUNTRY
WHERE
  COALESCE(bi_dstatus.DESSTATUSCORP, '') NOT IN ('Retiradas', 'Registradas')
  AND sicc_fstaebecam.PT_COUNTRY = :country
  AND sicc_fstaebecam.ANIOCAMPANA IN (:campaign)
