INSERT INTO ?tableName
WITH
  agg_maxaniocampana AS (
    SELECT PT_COUNTRY, MAX(ANIOCAMPANA) AS MAXANIOCAMPANA
    FROM ?landingSchema.tbpq_planit_fnumpedcam
    GROUP BY PT_COUNTRY
  )
SELECT
  agg_maxaniocampana.MAXANIOCAMPANA,
  planit_dcontrol.CODCONTROL,
  planit_dcontrol.PT_COUNTRY,
  GETDATE()
FROM
  ?landingSchema.tbpq_planit_dcontrol planit_dcontrol
  INNER JOIN agg_maxaniocampana
    ON agg_maxaniocampana.PT_COUNTRY = planit_dcontrol.PT_COUNTRY
WHERE planit_dcontrol.PT_COUNTRY = :country
