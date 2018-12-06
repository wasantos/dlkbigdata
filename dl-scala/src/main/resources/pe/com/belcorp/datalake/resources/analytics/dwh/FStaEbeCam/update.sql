INSERT INTO ?tableName
WITH
  agg_bi_dstatus AS (
    SELECT 
      PT_COUNTRY, 
      CODSTATUS_SICC, 
      MIN(CODSTATUS) AS CODSTATUS, 
      MIN(DESSTATUS) AS DESSTATUS, 
      MIN(CODSTATUSCORP) AS CODSTATUSCORP, 
      MIN(DESSTATUSCORP) AS DESSTATUSCORP
    FROM ?landingSchema.tbpq_bi_dstatus
	  WHERE CODSTATUS_SICC IS NOT NULL
    GROUP BY PT_COUNTRY, CODSTATUS_SICC
  ),
  agg_flags_factura AS (
    SELECT
      PT_COUNTRY, 
      ANIOCAMPANA, 
      CODEBELISTA, 
      MAX(FLAGORDENANULADO) AS FLAGPEDIDOANULADO, 
      CASE
        WHEN MAX(CANALINGRESO) IN ('WEB', 'WMX', 'APP', 'APM', 'APW')
        THEN 1 ELSE 0
      END AS FLAGPASOPEDIDOWEB,
      MAX(CANALINGRESO) AS CANALINGRESO
    FROM  ?landingSchema.tbpq_sicc_dnrodocumento
    WHERE PT_COUNTRY = :country
		AND ANIOCAMPANA IN (:campaign)
    GROUP BY PT_COUNTRY, ANIOCAMPANA, CODEBELISTA;
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
      sicc_fvtaproebecam.ANIOCAMPANA,
      sicc_fvtaproebecam.CODEBELISTA,
      sicc_fvtaproebecam.PT_COUNTRY,
      MAX(CASE WHEN TRIM(sap_dproducto.CODMARCA) IN ('01', '02', '03', '06') THEN 1 ELSE 0 END) AS FLAGMULTIMARCA,
      MAX(CASE WHEN TRIM(sap_dproducto.CODCLASE) = '11' THEN 1 ELSE 0 END) AS FLAGPASOPEDIDOFRAGANCIA,
      MAX(CASE WHEN TRIM(sap_dproducto.CODCLASE) = '13' THEN 1 ELSE 0 END) AS FLAGPASOPEDIDOCUIDADOPERSONAL,
      MAX(CASE WHEN TRIM(sap_dproducto.CODCLASE) = '12' THEN 1 ELSE 0 END) AS FLAGPASOPEDIDOMAQUILLAJE,
      MAX(CASE WHEN TRIM(sap_dproducto.CODCLASE) = '15' THEN 1 ELSE 0 END) AS FLAGPASOPEDIDOTRATAMIENTOCORPORAL,
      MAX(CASE WHEN TRIM(sap_dproducto.CODCLASE) = '14' THEN 1 ELSE 0 END) AS FLAGPASOPEDIDOTRATAMIENTOFACIAL
    FROM ?landingSchema.tbpq_sicc_fvtaproebecam sicc_fvtaproebecam
    LEFT JOIN ?landingSchema.tbpq_sap_dproducto sap_dproducto
      ON TRIM(sicc_fvtaproebecam.CODPRODUCTO) = TRIM(sap_dproducto.CODSAP)
    WHERE sicc_fvtaproebecam.CODTIPODOCUMENTO = 'N'
    	AND sicc_fvtaproebecam.REALUUVENDIDAS + sicc_fvtaproebecam.REALUUFALTANTES > 0
      AND sicc_fvtaproebecam.ANIOCAMPANAREF IS NULL
      AND sicc_fvtaproebecam.PT_COUNTRY = :country
		  AND sicc_fvtaproebecam.ANIOCAMPANA IN (:campaign)
    GROUP BY
      sicc_fvtaproebecam.ANIOCAMPANA,
      sicc_fvtaproebecam.CODEBELISTA,
      sicc_fvtaproebecam.PT_COUNTRY
  )
SELECT
  sicc_fstaebecam.ANIOCAMPANA,
  sicc_fstaebecam.CODCANALVENTA,
  sicc_fstaebecam.CODEBELISTA,
  CAST(agg_bi_dstatus.CODSTATUS AS SMALLINT),
  sicc_fstaebecam.CODTERRITORIO,
  CAST(sicc_fstaebecam.FLAGPASOPEDIDO AS SMALLINT),
  CAST(sicc_fstaebecam.REALNROORDENES AS SMALLINT),
  CASE WHEN agg_bi_dstatus.CODSTATUSCORP IN (1,2,3)  THEN 1 ELSE 0 END FLAGACTIVA,
  agg_flags_indicadores.FLAGPASOPEDIDOCUIDADOPERSONAL,
  agg_flags_indicadores.FLAGPASOPEDIDOMAQUILLAJE,
  agg_flags_indicadores.FLAGPASOPEDIDOTRATAMIENTOCORPORAL,
  agg_flags_indicadores.FLAGPASOPEDIDOTRATAMIENTOFACIAL,
  agg_flags_factura.FLAGPEDIDOANULADO,
  agg_flags_indicadores.FLAGPASOPEDIDOFRAGANCIA,
  sicc_fstaebecam.PT_COUNTRY,
  agg_flags_factura.CANALINGRESO,
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
  CASE WHEN digital_fcompdigcon.ESTADOREVISTADIGITALSUSCRIPCION = '1' THEN 1 ELSE 0 END AS FLAGREVISTADIGITALSUSCRIPCION,
  CASE WHEN digital_fcompdigcon.ESTADOREVISTADIGITALSUSCRIPCION in ('1','2') THEN 1 ELSE 0 END AS FLAGEXPERIENCIAGANAMAS
FROM 
  ?landingSchema.tbpq_sicc_fstaebecam sicc_fstaebecam
    LEFT JOIN ?landingSchema.tbpq_bi_fstaebecam bi_fstaebecam
      ON sicc_fstaebecam.ANIOCAMPANA = bi_fstaebecam.ANIOCAMPANA
      AND sicc_fstaebecam.CODEBELISTA = bi_fstaebecam.CODEBELISTA
      AND sicc_fstaebecam.PT_COUNTRY = bi_fstaebecam.PT_COUNTRY
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
    LEFT JOIN agg_bi_dstatus
      ON sicc_fstaebecam.CODSTATUS = agg_bi_dstatus.CODSTATUS_SICC
      AND sicc_fstaebecam.PT_COUNTRY = agg_bi_dstatus.PT_COUNTRY
WHERE
  COALESCE(agg_bi_dstatus.DESSTATUSCORP, '') NOT IN ('Retiradas', 'Registradas')
  AND sicc_fstaebecam.PT_COUNTRY = :country
  AND sicc_fstaebecam.ANIOCAMPANA IN (:campaign)
