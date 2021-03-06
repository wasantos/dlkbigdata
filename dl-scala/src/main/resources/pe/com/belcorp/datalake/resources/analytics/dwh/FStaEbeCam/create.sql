CREATE TABLE IF NOT EXISTS ?tableName (
  ANIOCAMPANA VARCHAR(6),
  CODCANALVENTA VARCHAR(2),
  CODEBELISTA VARCHAR(10),
  CODSTATUS SMALLINT,
  CODTERRITORIO VARCHAR(10),
  FLAGPASOPEDIDO SMALLINT,
  REALNROORDENES SMALLINT,
  FLAGACTIVA SMALLINT,
  FLAGPASOPEDIDOCUIDADOPERSONAL SMALLINT,
  FLAGPASOPEDIDOMAQUILLAJE SMALLINT,
  FLAGPASOPEDIDOTRATAMIENTOCORPORAL SMALLINT,
  FLAGPASOPEDIDOTRATAMIENTOFACIAL SMALLINT,
  FLAGPEDIDOANULADO SMALLINT,
  FLAGPASOPEDIDOFRAGANCIAS SMALLINT,
  CODPAIS VARCHAR(2) DISTKEY,
  CODIGOFACTURAINTERNET VARCHAR(10),
  CODCANALORIGEN VARCHAR(2),
  FLAGMULTIMARCA SMALLINT,
  CONSTANCIA VARCHAR(5),
  FRECUENCIACOMPRA BIGINT,
  CODCOMPORTAMIENTOROLLING SMALLINT,
  DESCRIPCIONROLLING VARCHAR(MAX),
  FLAGPASOPEDIDOWEB SMALLINT,
  NROLOGUEOS BIGINT,
  FLAGIPUNICOZONA SMALLINT,
  FLAGEXPUESTAODD SMALLINT,
  FLAGEXPUESTAOF SMALLINT,
  FLAGEXPUESTAFDC SMALLINT,
  FLAGEXPUESTASR SMALLINT,
  FLAGCOMPRAOPT SMALLINT,
  FLAGCOMPRAODD SMALLINT,
  FLAGCOMPRAOF SMALLINT,
  FLAGCOMPRAFDC SMALLINT,
  FLAGCOMPRASR SMALLINT,
  FLAGEXPUESTAOPT SMALLINT,
  FLAGDIGITAL SMALLINT,
  FLAGOFERTADIGITAL SMALLINT,
  FLAGREVISTADIGITALSUSCRIPCION SMALLINT,
  FLAGEXPERIENCIAGANAMAS SMALLINT,
  PRIMARY KEY(ANIOCAMPANA, CODEBELISTA)
) COMPOUND SORTKEY(ANIOCAMPANA, CODEBELISTA)
