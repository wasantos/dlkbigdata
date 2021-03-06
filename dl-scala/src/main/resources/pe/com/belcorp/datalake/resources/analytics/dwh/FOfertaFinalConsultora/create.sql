CREATE TABLE IF NOT EXISTS ?tableName (
  ANIOCAMPANAWEB VARCHAR(6),
  CANTIDAD BIGINT,
  CODEBELISTA VARCHAR(10),
  CODVENTA VARCHAR(5),
  TIPOOFERTAFINAL VARCHAR(8),
  FECHACREACION TIMESTAMP,
  REALMNGAP DECIMAL(15,5),
  TIPOEVENTO VARCHAR(10),
  CODPAIS VARCHAR(2) DISTKEY,
  PRIMARY KEY(ANIOCAMPANAWEB, CODEBELISTA, CODVENTA)
) COMPOUND SORTKEY(
  ANIOCAMPANAWEB,
  CODEBELISTA,
  CODVENTA
)


