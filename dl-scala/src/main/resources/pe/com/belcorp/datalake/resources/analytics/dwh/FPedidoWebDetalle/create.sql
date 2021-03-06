CREATE TABLE IF NOT EXISTS ?tableName (
  ANIOCAMPANAWEB VARCHAR(6),
  CANTIDAD BIGINT,
  CODEBELISTA VARCHAR(10),
  CODVENTA VARCHAR(5),
  FECHACREACION TIMESTAMP,
  FLAGOFERTAWEB SMALLINT,
  FLAGPROCESADO SMALLINT,
  IMPORTETOTAL DECIMAL(15,5),
  ORDENPEDIDOWD INTEGER,
  ORIGENPEDIDOWEB INTEGER,
  PEDIDODETALLEID INTEGER,
  PEDIDOID INTEGER,
  CODPAIS VARCHAR(2) DISTKEY,
  PRIMARY KEY(ANIOCAMPANAWEB, CODEBELISTA, PEDIDOID, PEDIDODETALLEID)
) COMPOUND SORTKEY(
  ANIOCAMPANAWEB,
  CODEBELISTA,
  PEDIDOID,
  PEDIDODETALLEID
)
