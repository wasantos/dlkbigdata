CREATE TABLE IF NOT EXISTS ?tableName (
  CODPAIS VARCHAR(2) DISTKEY,
  ANIOCAMPANA VARCHAR(6),
  CODREGION VARCHAR(3),
  CODZONA VARCHAR(6),
  CODACTIVIDAD VARCHAR(2),
  DESACTIVIDAD VARCHAR(MAX),
  FECHA TIMESTAMP,
  NUMDIA SMALLINT,
  PRIMARY KEY(ANIOCAMPANA, CODREGION, CODZONA, CODACTIVIDAD)
) COMPOUND SORTKEY(ANIOCAMPANA, CODREGION, CODZONA, CODACTIVIDAD)
