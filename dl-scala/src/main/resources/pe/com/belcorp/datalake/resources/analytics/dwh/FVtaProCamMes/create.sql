CREATE TABLE IF NOT EXISTS ?tableName (
  CODVERSION VARCHAR(2),
  DESVERSION VARCHAR(20),
  FECHAVERSIONAMIENTO TIMESTAMP,
  CODCANALVENTA VARCHAR(2),
  ANIOCONTABLE INTEGER,
  ANIOCAMPANA VARCHAR(6),
  MES SMALLINT,
  CODSAP VARCHAR(18),
  CODTIPOOFERTA VARCHAR(4),
  ESTUUVENDIDAS INTEGER,
  ESTVTAMNNETO DECIMAL(18,3),
  ESTVTADOLNETO DECIMAL(18,3),
  ESTPUP DECIMAL(18,4),
  ESTUTILIDAD DECIMAL(18,3),
  PRECIOOFERTAMN DECIMAL(18,3),
  PRECIOOFERTADOL DECIMAL(18,3),
  PRECIONORMALMN DECIMAL(18,3),
  PRECIONORMALDOL DECIMAL(18,3),
  DEMANDAANORMAL VARCHAR(20),
  CODPAIS VARCHAR(2) DISTKEY,
  PRIMARY KEY(ANIOCAMPANA, CODCANALVENTA, CODSAP, CODTIPOOFERTA)
) COMPOUND SORTKEY(ANIOCAMPANA, CODCANALVENTA, CODSAP, CODTIPOOFERTA)
