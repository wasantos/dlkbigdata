CREATE TABLE IF NOT EXISTS ?tableName (
  ANIOCAMPANA VARCHAR(6),
  CODCANALVENTA VARCHAR(2),
  CODCATALOGO VARCHAR(2),
  CODESTRATEGIA VARCHAR(3),
  CODTIPOOFERTA VARCHAR(4),
  CODVENTA VARCHAR(5),
  DESCATALOGO VARCHAR(MAX),
  DESTIPOOFERTA VARCHAR(MAX),
  NROPAGINA BIGINT,
  NUMOFERTA BIGINT,
  PRECIONORMALMN DECIMAL(15,5),
  PRECIOOFERTA DECIMAL(15,5),
  PRECIOVTAPROPUESTOMN DECIMAL(15,5),
  CODTIPOCATALOGO VARCHAR(4),
  DESARGVENTA VARCHAR(MAX),
  DESEXPOSICION VARCHAR(MAX),
  DESLADOPAG VARCHAR(MAX),
  DESTIPOCATALOGO VARCHAR(MAX),
  DESUBICACIONCATALOGO VARCHAR(MAX),
  FOTOMODELO VARCHAR(2),
  FOTOPRODUCTO VARCHAR(2),
  NROPAGINAS BIGINT,
  PAGINACATALOGO BIGINT,
  DESOBSERVACIONES VARCHAR(MAX),
  VEHICULOVENTA VARCHAR(MAX),
  CODPAIS VARCHAR(2) DISTKEY,
  CODSAP CHAR(18),
  CODTIPOMEDIOVENTA VARCHAR(3),
  DEMANDAANORMALPLAN DECIMAL(15,5),
  DESESTRATEGIA VARCHAR(50),
  DESTIPODIAGRAMACION VARCHAR(100),
  FACTORCUADRE VARCHAR(6),
  FACTORREPETICION VARCHAR(6),
  FLAGDISCOVER VARCHAR(100),
  FLAGESTADISTICABLE VARCHAR(1),
  FLAGPRODUCTOSEBE VARCHAR(1),
  INDCUADRE VARCHAR(40),
  INDPADRE VARCHAR(1),
  PRECIONORMALDOLPLAN DECIMAL(15,5),
  PRECIONORMALMNPLAN DECIMAL(15,5),
  PRECIOOFERTADOLPLAN DECIMAL(15,5),
  PRECIOOFERTAMNPLAN DECIMAL(15,5),
  PRIMARY KEY(ANIOCAMPANA, CODCANALVENTA, CODTIPOOFERTA, CODSAP, CODVENTA)
) COMPOUND SORTKEY(
  ANIOCAMPANA,
  CODCANALVENTA,
  CODTIPOOFERTA,
  CODSAP,
  CODVENTA
)
