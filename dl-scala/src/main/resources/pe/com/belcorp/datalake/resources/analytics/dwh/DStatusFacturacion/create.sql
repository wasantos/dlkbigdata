CREATE TABLE IF NOT EXISTS ?tableName (
  ANIOCAMPANA VARCHAR(6),
  CODREGION VARCHAR(3),
  CODZONA VARCHAR(6),
  FLAGSTATUSFACTSC BIGINT,
  FECHA TIMESTAMP,
  CODPAIS VARCHAR(2) DISTKEY,
  PRIMARY KEY(ANIOCAMPANA, CODREGION, CODZONA)
) COMPOUND SORTKEY(
  ANIOCAMPANA,
  CODREGION,
  CODZONA
)


