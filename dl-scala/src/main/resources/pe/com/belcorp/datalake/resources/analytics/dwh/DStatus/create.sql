CREATE TABLE IF NOT EXISTS ?tableName (
  CODPAIS VARCHAR(2) DISTKEY,
  CODSTATUS SMALLINT,
  DESSTATUS VARCHAR(MAX),
  CODSTATUSCORP SMALLINT,
  DESSTATUSCORP VARCHAR(MAX),
  CODSTATUS_SICC SMALLINT,
  DESSTATUS_SICC VARCHAR(MAX),
  PRIMARY KEY(CODSTATUS)
) SORTKEY(CODSTATUS)
