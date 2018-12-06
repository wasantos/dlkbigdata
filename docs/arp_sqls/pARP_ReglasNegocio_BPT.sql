CREATE PROCEDURE [dbo].[pARP_ReglasNegocio_BPT] 
@CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoARP CHAR(1),
@FlagCarga INT,@FlagMC INT,@Perfil VARCHAR(1),@FlagOF INT

AS
BEGIN

/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoARP						CHAR(1)
DECLARE @FlagCarga						INT
DECLARE @FlagMC							INT
DECLARE @Perfil							VARCHAR(1)
DECLARE @FlagOF							INT

SET @CodPais				= 'EC'	    -- Código de país
SET @AnioCampanaProceso		= '201704'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201706'	-- Campaña de Venta
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
SET @FlagCarga				= 1			-- 1: Carga/Personalización | 0: Estimación
SET @FlagMC					= 1			-- 1: Con Motor de Canibalización | 0: Sin Motor de Canibalización
SET @Perfil					= 'X'		-- Número de Perfil | 'X': Sin Perfil
SET @FlagOF					= 1			-- 1: Incluir OF | 0: No incluir OF*/



--Declare @TipoPersonalizacion			VARCHAR(3)
--DECLARE @CodPais						VARCHAR(2)
--DECLARE @AnioCampanaProceso				CHAR(6)
--DECLARE @AnioCampanaExpo				CHAR(6)
--DECLARE @TipoARP						CHAR(1)
--DECLARE @FlagCarga						INT
--DECLARE @FlagMC							INT
--DECLARE @Perfil							VARCHAR(1)
--DECLARE @FlagOF							INT

 
--SET @CodPais                      = 'PE'       -- Codigo de país
--SET @AnioCampanaProceso           = '201707'   -- Última campaña cerrada
--SET @AnioCampanaExpo       = '201710'   -- Campaña de Venta
--SET @TipoPersonalizacion   = 'BPT'             -- 'OPT','ODD','SR'
--SET @TipoARP                      = 'N'        -- 'N': Nuevas | 'E': Establecidas
--SET @FlagCarga                          = 1                 -- 1: Carga/Personalización | 0: Estimación
--SET @FlagMC                             = 0                 -- 1: Con Motor de Canibalización | 0: Sin Motor de Canibalización
--SET @Perfil                             ='X'          -- Número de Perfil | 'X': Sin Perfil
--SET @FlagOF                             = 0                 -- 1: Incluir OF | 0: No incluir OF
----ET @TipoGP                             = 2         -- 1: Segmento y Región | 2: Perfil




DECLARE @EspaciosOF						INT
SET @EspaciosOF				= 12		-- Número de espacios para Oferta Final

DECLARE @NumEspaciosFijos INT
DECLARE @NumEspaciosLibres INT
DECLARE @NumEspaciosTop INT
DECLARE @i INT
DECLARE @NumEspacios INT
DECLARE @NumProd INT
DECLARE @PosicionOF_1 INT
DECLARE @PosicionOF_2 INT
DECLARE @PosicionOF_3 INT

SET @i=1  
/** Inicio: Variables Log **/
DECLARE @FechaInicio 	DATETIME 
DECLARE @Procedimiento	VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_ReglasNegocio_BPT_Modify'
/** Fin: Variables Log **/

IF OBJECT_ID('ListadoProductos1') is not null
DROP TABLE ListadoProductos1

IF OBJECT_ID('ListadoRegalos1') is not null
DROP TABLE ListadoRegalos1

IF OBJECT_ID('EspaciosForzados1') is not null
DROP TABLE EspaciosForzados1

IF OBJECT_ID('CampaniaExpoEspacios1') is not null
DROP TABLE CampaniaExpoEspacios1

 /** Carga - Personalización **/
IF (@FlagCarga = 1) 
BEGIN

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,CodVinculoOF,
	ISNULL(TipoRevistaDigital,'') AS  TipoRevistaDigital
	INTO ListadoProductos1 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='BPT' 
	AND Perfil=@Perfil --AND ISNULL(TipoRevistaDigital,'') <> 'LAN'

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,
	ISNULL(TipoRevistaDigital,'') AS  TipoRevistaDigital 
	INTO ListadoRegalos1
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='BPT' 
	AND Perfil=@Perfil --AND ISNULL(TipoRevistaDigital,'') <> 'LAN'

	--Se Guardan el números de espacios forzados
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio
	INTO EspaciosForzados1  
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados
	FROM BD_ANALITICO.dbo.ARP_EspaciosForzados (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='BPT'
	AND Perfil=@Perfil 

	--Se guarda el números de espacios total
	SELECT DISTINCT AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios1 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='BPT'
	AND Perfil=@Perfil AND ISNULL(TipoRevistaDigital,'') <> 'LAN'

END


SELECT * INTO #ListadoProductos1 FROM ListadoProductos1 WHERE TipoRevistaDigital <> 'LAN'
SELECT * INTO #ListadoRegalos1 FROM ListadoRegalos1 WHERE TipoRevistaDigital <> 'LAN'
SELECT * INTO #EspaciosForzados1 FROM EspaciosForzados1
SELECT * INTO #CampaniaExpoEspacios1 FROM CampaniaExpoEspacios1

SELECT @NumEspacios = ISNULL(Espacios,0), 
       @NumEspaciosTop = ISNULL(EspaciosTop,0) 
FROM CampaniaExpoEspacios1

--Se guardan los espacios Fijos
SELECT DISTINCT Marca,Categoria,TipoForzado,VinculoEspacio 
INTO #EspaciosFijos 
FROM EspaciosForzados1 
WHERE TipoForzado=1

--Se guardan las marcas y categorias que no se deben repetir en los espacios no forzados
SELECT DISTINCT Marca,Categoria,TipoForzado
INTO #MarcaCategoriaNoRepetir 
FROM EspaciosForzados1 
WHERE TipoForzado=0

SELECT @NumEspaciosFijos = ISNULL(MAX(VinculoEspacio),0) 
FROM #EspaciosFijos

SET @NumEspaciosLibres=@NumEspacios-@NumEspaciosFijos-@NumEspaciosTop

/* Se evalua que el número de espacios fijos sea menor o igual al número de espacios totales */  
IF  (@NumEspacios<@NumEspaciosFijos+@NumEspaciosTop)  
BEGIN 
  RETURN  
END 

--Se obtienen los productos a nivel CUC
SELECT B.CodCUC CodProducto,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre,FlagTop,CAST('' AS VARCHAR) CodSAP,
       MAX(B.DesProductoCUC) DesProducto,
	   MAX(B.DesMarca) DesMarca,
	   MAX(B.DESCategoria) DESCategoria,
       MAX(LimUnidades) LimUnidades,
	   MAX(FlagUltMinuto) FlagUltMinuto, 
	   A.TipoRevistaDigital 
INTO #ProductosCUC 
FROM ListadoProductos1 A   
--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
WHERE B.DesProductoCUC IS NOT NULL AND TipoRevistaDigital <> 'LAN'
GROUP BY B.CodCUC,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre,FlagTop, A.TipoRevistaDigital

UPDATE B
SET CodSAP = C.CodSAP
--FROM DMATRIZCAMPANA A INNER JOIN #ProductosCUC B 
FROM DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA A (NOLOCK) INNER JOIN #ProductosCUC B 
ON A.CodVenta=B.CodVenta AND AnioCampana= @AnioCampanaExpo
--INNER JOIN DPRODUCTO C ON A.PKProducto=C.PKProducto
INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO C (NOLOCK) ON A.PKProducto=C.PKProducto AND C.CodPais=@CodPais
WHERE AnioCampana = @AnioCampanaExpo
AND A.CodPais=@CodPais

--Se crea la tabla para cargar los resultados finales
CREATE TABLE #ListadoInterfazFinal(
	[CodPais] [varchar](5) NOT NULL,
	[Tipo] [varchar](3) NOT NULL,
	[AnioCampanaVenta] [varchar](6) NOT NULL,
	[CodEbelista] [varchar](15) NOT NULL,
	[CodProducto] [varchar](20) NULL,
	[CodSAP] [varchar](18) NULL,
	[CodVenta] [varchar](5) NOT NULL,
	[Portal] [varchar](3) NULL,
	[DiaInicio] [int] NULL,
	[DiaFin] [int] NULL,
	[Orden] [int] NULL,
	[FlagManual] [int] NULL,
	[TipoARP] [varchar](1) NOT NULL,
	[CodVinculo] [int] NULL,
	[PPU] [float] NULL,
	[LimUnidades] [int] NULL,
	[FlagUltMinuto] [int] NULL,
	[Perfil] [varchar](1) NULL)


SELECT PKEBELISTA INTO #TMP
FROM DWH_ANALITICO.DBO.DWH_FSTAEBECAM A INNER JOIN DWH_ANALITICO.DBO.DWH_DGEOGRAFIACAMPANA B 
ON A.CODPAIS = B.CODPAIS AND A.ANIOCAMPANA = B.ANIOCAMPANA AND A.PKTERRITORIO = B.PKTERRITORIO
WHERE A.ANIOCAMPANA = @AnioCampanaProceso AND A.CODPAIS = @CodPais
AND CODZONA IN ('1405', '1406')
--AND CODZONA = '8024'
--AND DESREGION IN ('LIMA 6')

SELECT DISTINCT PKEbelista,CodTactica,FlagTop,Probabilidad 
INTO #ListadoConsultora_Total
--FROM BDDM01.[DATAMARTANALITICO].[dbo].[ARP_ListadoProbabilidades] 
FROM ARP_ListadoProbabilidades_Temporal (NOLOCK)
WHERE PKEbelista IN (SELECT PKEBELISTA FROM #TMP)

SELECT A.*, B.CodProducto AS CodCUC
INTO #ListadoConsultora_Proceso
FROM #ListadoConsultora_Total A INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica

SELECT CodCUC, COUNT(1) AS Cantidad, 0 AS Orden
INTO #TMP_Productos
FROM #ListadoConsultora_Proceso
GROUP BY CodCUC
ORDER BY COUNT(1) DESC

SELECT CodCUC, Cantidad,
       ROW_NUMBER()OVER(ORDER BY Cantidad DESC) AS Orden
INTO #TMP_ProdOrden
FROM #TMP_Productos

SELECT PkEbelista,CodTactica, CodCUC, Probabilidad, 'N' AS FlagQueda
INTO #BaseProceso
FROM #ListadoConsultora_Proceso

SET @i = 1

SELECT @NumProd = COUNT(1) FROM #TMP_ProdOrden

WHILE (@i <= @NumProd)
BEGIN

	SELECT PKEbelista, SUM(CASE WHEN FlagQueda='S' THEN 1 ELSE 0 END) Cantidad
	  INTO #TMP_Ebelista
	  FROM #BaseProceso A INNER JOIN #TMP_ProdOrden B ON A.CodCUC = B.CodCUC AND B.Orden=@i
	 GROUP BY PKEbelista

	SELECT A.PKEbelista,
		   A.CodCUC,
		   MAX(Probabilidad) AS Probabilidad
	INTO #TMP_MaxProb
	FROM #BaseProceso A INNER JOIN #TMP_ProdOrden B ON A.CodCUC = B.CodCUC AND B.Orden=@i
		                INNER JOIN #TMP_Ebelista C ON A.PKEbelista=C.PKEbelista AND C.Cantidad=0
	WHERE A.FlagQueda = 'N'
	AND Probabilidad != 0
	GROUP BY A.PKEbelista, A.CodCUC

	SELECT A.PKEbelista, A.CodTactica
	INTO #TMP_Tactica
	FROM #BaseProceso A INNER JOIN #TMP_MaxProb B ON A.PKEbelista=B.PKEbelista AND A.CodCUC=B.CodCUC 
	AND A.Probabilidad=B.Probabilidad

	UPDATE A
	SET A.FlagQueda = 'S'
	FROM #BaseProceso A INNER JOIN #TMP_Tactica B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica

	SELECT PKEbelista, CodTactica
	INTO #TMP_TacticaEli1
	FROM #BaseProceso A INNER JOIN #TMP_ProdOrden B ON A.CodCUC = B.CodCUC AND B.Orden=@i
	WHERE FlagQueda = 'N'

	UPDATE A
	SET FlagQueda = 'X'
	FROM #BaseProceso A INNER JOIN #TMP_TacticaEli1 B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica

	SELECT Pkebelista,CodCUC
	INTO #TMP_ProdEli
	FROM #BaseProceso
	WHERE FlagQueda = 'S'

	SELECT A.PKEbelista, A.CodTactica
	INTO #TMP_TacticaEli2
	FROM #BaseProceso A INNER JOIN #TMP_ProdEli B ON A.PKEbelista=B.PKEbelista AND A.CodCUC=B.CodCUC
	WHERE FlagQueda='N'

	UPDATE A
	SET FlagQueda = 'X'
	FROM #BaseProceso A INNER JOIN #TMP_TacticaEli2 B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica
	WHERE FlagQueda='N'

	DROP TABLE #TMP_Ebelista
	DROP TABLE #TMP_MaxProb
	DROP TABLE #TMP_Tactica
	DROP TABLE #TMP_TacticaEli1
    DROP TABLE #TMP_TacticaEli2
	DROP TABLE #TMP_ProdEli

	SET @i = @i + 1

END

DELETE A
FROM #ListadoConsultora_Total A INNER JOIN #BaseProceso B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica 
AND B.FlagQueda IN ('N','X')

/** Fin: Eliminación de CUCs repetidos **/

--Tipo: N (No Forzado) F(Forzado) T(Top)
/** 1.1. TOPS **/
--Se Ordenan las tácticas según la probabilidad: 
SELECT DISTINCT pkebelista,CodTactica,Probabilidad,
	   ROW_NUMBER()OVER(PARTITION BY PkEbelista ORDER BY Probabilidad DESC) as Posicion
INTO #Temporal_Tops
FROM #ListadoConsultora_Total
WHERE Probabilidad!=0 
AND FlagTop=1
ORDER BY pkebelista,Probabilidad DESC

--Tipo T: TOPS 
SELECT DISTINCT PKEbelista,CodTactica,Probabilidad,'T' Tipo,0 Prioridad, 0 Orden 
INTO #ListaRecomendados
FROM #Temporal_Tops
WHERE Posicion<=@NumEspaciosTop

--Se guardan las tácticas que no son Top
SELECT DISTINCT PKEbelista,CodTactica, Probabilidad 
INTO #Temporal_0
FROM #ListadoConsultora_Total
WHERE Probabilidad!=0 
AND FlagTop=0

--Tipo F y N: Forzados y No Forzados
SELECT A.PKEbelista,A.CodTactica,B.CodProducto,B.DesMarca,B.DESCategoria,Probabilidad,0 FlagQueda, 0 Ranking,'N' Tipo 
INTO #ListadoPropuestos
FROM #Temporal_0 A INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica

/** 1.2. Espacios Forzados **/

--Se obtienen los productos y tácticas de los espacios forzados
SET @i=1

WHILE @i<=@NumEspaciosFijos
BEGIN
		
	--Se busca el Top de productos disponibles y se guarda en una tabla temporal
	SELECT PKEbelista,CodTactica,CodProducto,Probabilidad,
	ROW_NUMBER() OVER (PARTITION BY PkEbelista ORDER BY Probabilidad DESC) as Posicion 
	INTO #Forzado_Top 
	FROM #ListadoPropuestos A INNER JOIN #EspaciosFijos B
	ON A.DesMarca=B.Marca AND A.DESCategoria=B.Categoria AND B.VinculoEspacio=@i
	WHERE A.FlagQueda=0 
	ORDER BY PKEbelista,Probabilidad DESC

	--Extraigo el registro a evaluar (El de mayor probabilidad)
	SELECT Pkebelista,CodTactica,CodProducto 
	INTO #TOP1 
	FROM #Forzado_Top 
	WHERE Posicion=1

	--Se actualiza la tabla inicial ListadoPropuestos 
	UPDATE #ListadoPropuestos
	SET Ranking = @i,
		FlagQueda = 1, 
		Tipo = 'F' --F: Tipo Forzado
	FROM #ListadoPropuestos A INNER JOIN #TOP1 B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica 
	WHERE FlagQueda=0

	SET @i=@i+1
	DROP TABLE #Forzado_Top
	DROP TABLE #TOP1

END

--Elimino los productos que son de la misma Marca y Categoría de los Espacios No Forzados
DELETE A
FROM #ListadoPropuestos A INNER JOIN #MarcaCategoriaNoRepetir B ON A.DesMarca=B.Marca AND A.DESCategoria=B.Categoria 
WHERE FlagQueda=0

/** 1.3. Espacios No Forzados **/
SET @i=1
WHILE @i<=@NumEspaciosLibres
BEGIN

	--Se busca el top de productos disponibles y se guarda en una tabla temporal
	SELECT pkebelista,CodTactica,CodProducto, Probabilidad,
	ROW_NUMBER() OVER (PARTITION BY PkEbelista ORDER BY Probabilidad DESC) AS Posicion
	INTO #Producto_Top1 
	FROM #ListadoPropuestos 
	WHERE FlagQueda=0
	ORDER BY pkebelista,Probabilidad DESC

	--Extraigo el registro a evaluar (El Top)
	SELECT Pkebelista,CodTactica,CodProducto 
	INTO #TOP1_1 
	FROM #Producto_Top1 
	WHERE Posicion = 1

	--Se actualiza la tabla inicial ListadoPropuestos 
	UPDATE #ListadoPropuestos
	SET Ranking = @i,
	    FlagQueda = 1
	FROM #ListadoPropuestos A INNER JOIN #TOP1_1 B 
	ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
	WHERE FlagQueda=0

	--Se guarda las consultoras y las tácticas donde aparece el producto evaluado
	SELECT DISTINCT A.PKEbelista,A.CodTactica 
	INTO #AEliminar 
	FROM #ListadoPropuestos A INNER JOIN #TOP1_1 B ON A.PKEbelista=B.PKEbelista
	AND A.CodProducto=B.CodProducto AND A.FlagQueda=0

	--Se elimina todas las tácticas que contengan el producto evaluado
	DELETE A
	FROM #ListadoPropuestos A INNER JOIN #AEliminar B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica
	AND A.FlagQueda=0

	SET @i=@i+1
	DROP TABLE #Producto_Top1
	DROP TABLE #TOP1_1
	DROP TABLE #AEliminar

END

/** 1.4. Espacios que sobran **/
SET @i=1

--Se eliminan los productos repetidos
WHILE @i<=@NumEspaciosFijos
BEGIN

	--Consultoras CEI: Consultoras con Espacios Incompletos
	SELECT PkEbelista, 
		   COUNT(DISTINCT CodTactica) NumEspacios 
	INTO #ConsultorASCEI 
	FROM #ListadoPropuestos
	WHERE FlagQueda=1
	GROUP BY PkEbelista
	HAVING COUNT(DISTINCT CodTactica)<@NumEspacios-@NumEspaciosTop

	--Se busca el top de productos disponibles y se guarda en una tabla temporal
	SELECT A.PKEbelista,A.CodTactica,A.CodProducto, A.Probabilidad,
	ROW_NUMBER() OVER(PARTITION BY A.PkEbelista ORDER BY Probabilidad DESC) as Posicion 
	INTO #Producto_Top1_ES 
	FROM #ListadoPropuestos A INNER JOIN #ConsultorASCEI B ON A.PKEbelista=B.PKEbelista
	WHERE FlagQueda=0
	ORDER BY A.PKEbelista,Probabilidad DESC

	--Extraigo el Registro a evaluar (el Top)
	SELECT Pkebelista,CodTactica,CodProducto 
	INTO #TOP1_1_ES 
	FROM #Producto_Top1_ES 
	WHERE Posicion = 1

	--Se actualiza la tabla tnicial ListadoPropuestos 
	UPDATE #ListadoPropuestos
	SET Ranking = @i,
		FlagQueda = 1
	FROM #ListadoPropuestos A INNER JOIN #TOP1_1_ES B 
	ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
	WHERE FlagQueda=0

	--Se guarda las consultoras y las tácticas donde aparece el producto evaluado
	SELECT DISTINCT A.PKEbelista,A.CodTactica 
	INTO #AEliminar_ES
	FROM #ListadoPropuestos A INNER JOIN #TOP1_1_ES B ON A.PKEbelista=B.PKEbelista
	AND A.CodProducto=B.CodProducto AND A.FlagQueda=0

	--Se elimina todas las tácticas que contengan el producto evaluado
	DELETE A
	FROM #ListadoPropuestos A INNER JOIN #AEliminar_ES B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica
	AND A.FlagQueda=0

	SET @i=@i+1
	DROP TABLE #Producto_Top1_ES
	DROP TABLE #TOP1_1_ES
	DROP TABLE #AEliminar_ES
	DROP TABLE #ConsultorASCEI

END

--Extraigo las tácticas de acuerdo a la cantidad de espacios
INSERT INTO #ListaRecomendados
SELECT DISTINCT PkEbelista,CodTactica,Probabilidad,Tipo,0,0 
FROM #ListadoPropuestos 
WHERE FlagQueda=1

--Se actualiza la prioridad de los tipos para hacer el ordenamiento
UPDATE #ListaRecomendados
SET Prioridad = 1 
WHERE Tipo='T'
	
UPDATE #ListaRecomendados
SET Prioridad = 2 
WHERE Tipo='N'
	
UPDATE #ListaRecomendados
SET Prioridad = 3 
WHERE Tipo='F'

--Se ordena los productos: Prioridad Top, No Forzado, Forzado
SELECT PKEbelista,CodTactica,Probabilidad, Tipo,
ROW_NUMBER()OVER(PARTITION BY PkEbelista ORDER BY Prioridad ASC,Probabilidad DESC) as Orden 
INTO #ListadoBPT
FROM #ListaRecomendados 

SELECT DISTINCT @CodPais CodPais, CASE WHEN TipoRevistaDigital = 'BPT' THEN 'OPM' ELSE TipoRevistaDigital END AS Tipo,
@AnioCampanaExpo AnioCampanaVenta,CodEbelista,
CodProducto,CodSAP,CodVenta,'IDP' Portal,0 DiaIni,0 DiaFin, Orden,0 FlagManual,@TipoARP as TipoARP,
0 CodVinculo, 0.0 PPU,B.LimUnidades, B.FlagUltMinuto, @Perfil Perfil
INTO #ListadoInterfazBPT
FROM #ListadoBPT A 
INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1
--INNER JOIN DEBELISTA C ON A.PKEbelista=C.PKEbelista
INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA C (NOLOCK) ON A.PKEbelista=C.PKEbelista AND C.CodPais=@CodPais

INSERT INTO #ListadoInterfazFinal
SELECT * 
FROM #ListadoInterfazBPT


-- Nuevas BPT Sin Lanzamientos
--=========================0
SELECT DISTINCT CODTACTICA,TIPOREVISTADIGITAL
INTO #DistintoLAN
FROM ListadoProductos1 WHERE    TipoRevistaDigital <> 'LAN'

 IF (@TipoARP='N')
BEGIN

	SELECT A.*
	INTO #ListadoVariablesProductos
	--FROM BDDM01.[DATAMARTANALITICO].[dbo].[ARP_ListadoVariablesProductos] 
	FROM BD_ANALITICO.dbo.ARP_ListadoVariablesProductos A (NOLOCK)  JOIN #DistintoLAN B 
	ON A.CODTACTICA = B.CODTACTICA
	WHERE CodPais=@CodPais 
	AND AnioCampanaProceso= @AnioCampanaProceso
	AND AnioCampanaExpo=@AnioCampanaExpo 
	and TipoARP= @TipoARP  
	AND TipoPersonalizacion='BPT' 

	--Tipo: N (No Forzado) F(Forzado) T(Top)
	/** 1. TOPS  **/
	--Se ordenan las tácticas según la probabilidad: 
	SELECT CodTactica, ProbabilidadCompra Probabilidad,
		   ROW_NUMBER()OVER(ORDER BY ProbabilidadCompra DESC) as Posicion 
	INTO #Temporal_Tops_Prod
	FROM #ListadoVariablesProductos 
	WHERE ProbabilidadCompra!=0 
	AND FlagTop=1

	--Tipo T: TOPS 
	SELECT CodTactica,Probabilidad,'T' Tipo, 0 Prioridad, 0 Orden 
	INTO #ListaRecomendadosProd
	FROM #Temporal_Tops_Prod
	WHERE Posicion<=@NumEspaciosTop

	--Se Ordenan las Tácticas  según la probabilidad: 
	SELECT CodTactica, ProbabilidadCompra Probabilidad,
		   ROW_NUMBER()OVER(ORDER BY ProbabilidadCompra DESC) as Posicion
	INTO #Temporal_0_Prod
	FROM #ListadoVariablesProductos 
	WHERE ProbabilidadCompra!=0 
	AND FlagTop=0

	--Tipo F y N: Forzados y No Forzados
	SELECT A.CodTactica,B.CodProducto,B.DesMarca,B.DESCategoria,Probabilidad,Posicion,0 FlagQueda, 0 Ranking, 'N' Tipo 
	INTO #ListadoPropuestosProd
	FROM #Temporal_0_Prod A INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica

	/** Espacios Forzados **/
	--Se obtienen los productos y Tácticas de los espacios forzados
	SET @i=1
	WHILE @i<=@NumEspaciosFijos
	BEGIN

		--Se busca el Top de Productos Disponibles y se guarda en una tabla temporal
		SELECT CodTactica,CodProducto, Probabilidad,
		ROW_NUMBER()OVER(ORDER BY Probabilidad DESC) as Posicion
		INTO #Forzado_Top_Prod 
		FROM #ListadoPropuestosProd A INNER JOIN #EspaciosFijos B
		ON A.DesMarca=B.Marca AND A.DESCategoria=B.Categoria AND B.VinculoEspacio=@i
		WHERE A.FlagQueda=0
		ORDER BY CodTactica,Probabilidad DESC

		--Extraigo el Registro a evaluar (el Top)
		SELECT CodTactica,CodProducto 
		INTO #TOP1_Prod 
		FROM #Forzado_Top_Prod 
		WHERE Posocion=1

		--Se actualiza la tabla Inicial ListadoPropuestos 
		UPDATE #ListadoPropuestosProd
		SET Ranking=@i,FlagQueda=1, Tipo='F'
		FROM #ListadoPropuestosProd A INNER JOIN #TOP1_Prod B 
		ON A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
		WHERE  FlagQueda=0

		SET @i=@i+1
		DROP TABLE #Forzado_Top_Prod
		DROP TABLE #TOP1_Prod

	END

	--Elimino los productos que son de la misma Marca y Categoría de los Espacios No Forzados
	DELETE A
	FROM #ListadoPropuestosProd A INNER JOIN #MarcaCategoriaNoRepetir B
	ON A.DesMarca=B.Marca AND A.DESCategoria=B.Categoria 
	WHERE FlagQueda=0

	/** Espacios No Forzados **/

	--Se eliminan los productos repetidos
	SET @i=1
	WHILE @i<=@NumEspaciosLibres
	BEGIN
			
		--Se busca el Top de Productos Disponibles y se guarda en una tabla temporal
		SELECT CodTactica,CodProducto, Probabilidad,
			   ROW_NUMBER() OVER (ORDER BY Probabilidad DESC) as Posicion
		INTO #Producto_Top1_Prod 
		FROM #ListadoPropuestosProd 
		WHERE FlagQueda=0
		ORDER BY CodTactica,Probabilidad DESC

		--Extraigo el registro a evaluar (el Top)
		SELECT CodTactica,CodProducto 
		INTO #TOP1_1_Prod 
		FROM #Producto_Top1_Prod 
		WHERE Posicion=1

		--Se actualiza la tabla Inicial ListadoPropuestos 
		UPDATE #ListadoPropuestosProd
		SET Ranking = @i,
			FlagQueda = 1
		FROM #ListadoPropuestosProd A INNER JOIN #TOP1_1_Prod B 
		ON A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
		WHERE FlagQueda=0

		--Se Guarda las consultoras y las Tácticas donde aparece el producto evaluado
		SELECT DISTINCT A.CodTactica 
		INTO #AEliminar_Prod 
		FROM #ListadoPropuestosProd A INNER JOIN #TOP1_1_Prod B ON 
		A.CodProducto=B.CodProducto AND A.FlagQueda=0

		--Se Elimina todas las tácticas que contengan el producto evaluado
		DELETE A
		FROM #ListadoPropuestosProd A INNER JOIN #AEliminar_Prod B 
		ON A.CodTactica=B.CodTactica AND A.FlagQueda=0

		SET @i=@i+1
		DROP TABLE #Producto_Top1_Prod
		DROP TABLE #TOP1_1_Prod
		DROP TABLE #AEliminar_Prod

	END

	--Extraigo las tácticas de acuerdo a la cantidad de espacios
	INSERT INTO #ListaRecomendadosProd
	SELECT DISTINCT CodTactica,Probabilidad,Tipo,0,0
	FROM #ListadoPropuestosProd 
	WHERE flagqueda=1

	--Se actualiza la prioridad de los tipos para hacer el ordenamiento
	UPDATE #ListaRecomendadosProd
	SET Prioridad = 1 
	WHERE Tipo='T'  --Top

	UPDATE #ListaRecomendadosProd
	SET Prioridad = 2 
	WHERE Tipo='N'  -- No forzados

	UPDATE #ListaRecomendadosProd
	SET Prioridad = 3 
	WHERE Tipo='F'  -- Forzados

	--Se Ordena los productos:Prioridad Top,No Forzado, Forzado
	SELECT CodTactica,Probabilidad, Tipo,
		   ROW_NUMBER()OVER(ORDER BY Prioridad ASC,Probabilidad DESC) AS Orden 
	INTO #ListadoOPT_Complemento
	FROM #ListaRecomendadosProd 

	SELECT DISTINCT @CodPais CodPais,CASE WHEN b.TipoRevistaDigital = 'BPT' THEN 'OPM' ELSE TipoRevistaDigital END AS Tipo ,@AnioCampanaExpo AnioCampanaVenta,'XXXXXXXXX' CodEbelista,
	B.CodProducto,B.CodSAP,B.CodVenta,'IDP' Portal,0 DiaIni,0 DiaFin, Orden,0 FlagManual,@TipoARP AS TipoARP,
	0 CodVinculo, 0.0 PPU,B.LimUnidades, B.FlagUltMinuto, @Perfil Perfil
	INTO #ListadoInterfaz_Complemento 
	FROM #ListadoOPT_Complemento A  
	INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1

	--Agregando a la tabla final
	INSERT INTO #ListadoInterfazFinal
	SELECT * 
	FROM #ListadoInterfaz_Complemento


END



-- fin
-- Nuevas OPT Sin Lanzamientos
--=========================0



/* LANZAMIENTOS - Inicio */

--Se obtienen los productos a nivel CUC
SELECT B.CodCUC CodProducto,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre,FlagTop,CAST('' AS VARCHAR) CodSAP,
       MAX(B.DesProductoCUC) DesProducto,
	   MAX(B.DesMarca) DesMarca,
	   MAX(B.DESCategoria) DESCategoria,
       MAX(LimUnidades) LimUnidades,
	   MAX(FlagUltMinuto) FlagUltMinuto,
	   A.TipoRevistaDigital 
INTO #ProductosCUCLanzamientos 
FROM ListadoProductos1 A 
--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
WHERE B.DesProductoCUC IS NOT NULL AND TipoRevistaDigital = 'LAN'
GROUP BY B.CodCUC,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre,FlagTop, A.TipoRevistaDigital 

 
UPDATE B
SET CodSAP = C.CodSAP
--FROM DMATRIZCAMPANA A INNER JOIN #ProductosCUC B 
FROM DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA A (NOLOCK) INNER JOIN #ProductosCUCLanzamientos B 
ON A.CodVenta=B.CodVenta AND AnioCampana= @AnioCampanaExpo
--INNER JOIN DPRODUCTO C ON A.PKProducto=C.PKProducto
INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO C (NOLOCK) ON A.PKProducto=C.PKProducto AND C.CodPais=@CodPais
WHERE AnioCampana = @AnioCampanaExpo
AND A.CodPais=@CodPais

SELECT A.*, B.CodProducto AS CodCUC
INTO #ListadoConsultora_ProcesoLanzamientos
FROM #ListadoConsultora_Total A INNER JOIN #ProductosCUCLanzamientos B ON A.CodTactica=B.CodTactica


SELECT CodCUC, COUNT(1) AS Cantidad, 0 AS Orden
INTO #TMP_ProductosLanzamientos
FROM #ListadoConsultora_ProcesoLanzamientos
GROUP BY CodCUC
ORDER BY COUNT(1) DESC

SELECT CodCUC, Cantidad,
       ROW_NUMBER()OVER(ORDER BY Cantidad DESC) AS Orden
INTO #TMP_ProdOrdenLanzamientos
FROM #TMP_ProductosLanzamientos

SELECT PkEbelista,CodTactica, CodCUC, Probabilidad, 'N' AS FlagQueda
INTO #BaseProcesoLanzamientos
FROM #ListadoConsultora_ProcesoLanzamientos

SET @i = 1

SELECT @NumProd = COUNT(1) FROM #TMP_ProdOrdenLanzamientos


WHILE (@i <= @NumProd)
BEGIN

	SELECT PKEbelista, SUM(CASE WHEN FlagQueda='S' THEN 1 ELSE 0 END) Cantidad
	 INTO #TMP_EbelistaLanzamientos
	 FROM #BaseProcesoLanzamientos A INNER JOIN #TMP_ProdOrdenLanzamientos B ON A.CodCUC = B.CodCUC AND B.Orden=@i
	 GROUP BY PKEbelista

	SELECT A.PKEbelista,
		   A.CodCUC,
		   MAX(Probabilidad) AS Probabilidad
	INTO #TMP_MaxProbLanzamiento
	FROM #BaseProcesoLanzamientos A INNER JOIN #TMP_ProdOrdenLanzamientos B ON A.CodCUC = B.CodCUC AND B.Orden=@i
		                INNER JOIN #TMP_EbelistaLanzamientos C ON A.PKEbelista=C.PKEbelista AND C.Cantidad=0
	WHERE A.FlagQueda = 'N'
	AND Probabilidad != 0
	GROUP BY A.PKEbelista, A.CodCUC

	SELECT A.PKEbelista, A.CodTactica
	INTO #TMP_TacticaLanzamientos
	FROM #BaseProcesoLanzamientos A INNER JOIN #TMP_MaxProbLanzamiento B ON A.PKEbelista=B.PKEbelista AND A.CodCUC=B.CodCUC 
	AND A.Probabilidad=B.Probabilidad

	UPDATE A
	SET A.FlagQueda = 'S'
	FROM #BaseProcesoLanzamientos A INNER JOIN #TMP_TacticaLanzamientos B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica

	SELECT PKEbelista, CodTactica
	INTO #TMP_TacticaEli1Lanzamiento
	FROM #BaseProcesoLanzamientos A INNER JOIN #TMP_ProdOrdenLanzamientos B ON A.CodCUC = B.CodCUC AND B.Orden=@i
	WHERE FlagQueda = 'N'
	
	UPDATE A
	SET FlagQueda = 'X'
	FROM #BaseProcesoLanzamientos A INNER JOIN #TMP_TacticaEli1Lanzamiento B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica

	SELECT Pkebelista,CodCUC
	INTO #TMP_ProdEliLanzamientos
	FROM #BaseProcesoLanzamientos
	WHERE FlagQueda = 'S'

	SELECT A.PKEbelista, A.CodTactica
	INTO #TMP_TacticaEli2Lanzamientos
	FROM #BaseProcesoLanzamientos A INNER JOIN #TMP_ProdEliLanzamientos B ON A.PKEbelista=B.PKEbelista AND A.CodCUC=B.CodCUC
	WHERE FlagQueda='N'

	UPDATE A
	SET FlagQueda = 'X'
	FROM #BaseProcesoLanzamientos A INNER JOIN #TMP_TacticaEli2Lanzamientos B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica
	WHERE FlagQueda='N'

	DROP TABLE #TMP_EbelistaLanzamientos
	DROP TABLE #TMP_MaxProbLanzamiento
	DROP TABLE #TMP_TacticaLanzamientos
	DROP TABLE #TMP_TacticaEli1Lanzamiento
    DROP TABLE #TMP_TacticaEli2Lanzamientos
	DROP TABLE #TMP_ProdEliLanzamientos

	SET @i = @i + 1

END

DELETE A
FROM #ListadoConsultora_ProcesoLanzamientos A INNER JOIN #BaseProcesoLanzamientos B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica 
AND B.FlagQueda IN ('N','X')

/** Fin: Eliminación de CUCs repetidos **/
SELECT PKEbelista,CodTactica,MAX(Probabilidad) AS Probabilidad, 
ROW_NUMBER()OVER(PARTITION BY PkEbelista ORDER BY  MAX(Probabilidad)  DESC) as Orden 
INTO #ListadoBPTLanzamientos 
FROM #ListadoConsultora_ProcesoLanzamientos 
GROUP BY PKEbelista,CodTactica


SELECT DISTINCT @CodPais CodPais, B.TipoRevistaDigital AS Tipo,@AnioCampanaExpo AnioCampanaVenta,CodEbelista,
CodProducto,CodSAP,CodVenta,'IDP' Portal,0 DiaIni,0 DiaFin, Orden,0 FlagManual,@TipoARP as TipoARP,
0 CodVinculo, 0.0 PPU,B.LimUnidades, B.FlagUltMinuto, @Perfil Perfil
INTO #ListadoInterfazBPTLanzamientos
FROM #ListadoBPTLanzamientos A 
INNER JOIN #ProductosCUCLanzamientos B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1
--INNER JOIN DEBELISTA C ON A.PKEbelista=C.PKEbelista
INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA C (NOLOCK) ON A.PKEbelista=C.PKEbelista AND C.CodPais=@CodPais


INSERT INTO #ListadoInterfazFinal
SELECT * 
FROM #ListadoInterfazBPTLanzamientos

-- Si es Nuevas Agregando Consultora Dummy para Lanzamientos
select A.* 
into #ListadoVariablesProductos_LAN
from ARP_ListadoVariablesProductos a join #ProductosCUCLanzamientos b 
ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1 

IF (@TipoARP = 'N') 
BEGIN
		SELECT DISTINCT A.CodEbelista,B.CodProducto,B.CodSAP,B.CodVenta,Orden,0 CodVinculo,0 PPU,B.LimUnidades,B.FlagUltMinuto 
		INTO #ListadoBPTLanzamientosDUMMY
	FROM (
					SELECT CodEbelista ,  CodTactica, ProbabilidadCompra,
					ROW_NUMBER()OVER( ORDER BY ProbabilidadCompra DESC) AS Orden  
					FROM (
								select  'XXXXXXXXX' AS CodEbelista ,  CodTactica, Max(ProbabilidadCompra) as ProbabilidadCompra
								from  #ListadoVariablesProductos_LAN  
								where codpais = @codpais
								AND ANIOCAMPANAPROCESO  = @AnioCampanaProceso
								AND AnioCampanaExpo = @AnioCampanaExpo
								AND TIPOPERSONALIZACION = 'BPT'
								GROUP BY  CodTactica 
						) AS T 
			) A INNER JOIN #ProductosCUCLanzamientos B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1 

--select * from #ListadoBPTLanzamientosDUMMY

SELECT DISTINCT @CodPais CodPais, B.TipoRevistaDigital AS Tipo,@AnioCampanaExpo AnioCampanaVenta,CodEbelista,
A.CodProducto,A.CodSAP,A.CodVenta,'IDP' Portal,0 DiaIni,0 DiaFin, Orden,0 FlagManual,@TipoARP as TipoARP,
0 CodVinculo, 0.0 PPU,B.LimUnidades, B.FlagUltMinuto, @Perfil Perfil
into #Result
FROM #ListadoBPTLanzamientosDUMMY A 
INNER JOIN #ProductosCUCLanzamientos B ON A.CODPRODUCTO=B.CODPRODUCTO AND B.IndicadorPadre=1


INSERT INTO #ListadoInterfazFinal
select * from #Result
END

/* LANZAMIENTOS - Fin*/

/** Carga - Personalización **/
IF (@FlagCarga = 1)
BEGIN
			
	--DELETE BD_ANALITICO.dbo.ARP_OfertaPersonalizada_Pruebas
	--DELETE ARP_OfertaPersonalizadaC01
	--WHERE CodPais=@CodPais AND AnioCampanaVenta=@AnioCampanaExpo and TipoARP=@TipoARP AND TipoPersonalizacion in ('OPM', 'LAN', 'PAD')

	--INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizada_Pruebas
	-- truncate table ARP_OfertaPersonalizadaC01_Resultado
	INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizadaC01_Resultado
	(CodPais,TipoPersonalizacion,AnioCampanaVenta,CodEbelista,CodCUC,CodSAP,CodVenta,ZonaPortal,DiaInicio,DiaFin,Orden,FlagManual,
	TipoARP,CodVinculo,PPU,LimUnidades,FlagUltMinuto,Perfil)
	SELECT * FROM #ListadoInterfazFinal

	----Generación de la Interfaz
	--INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizada
	--(CodPais,TipoPersonalizacion,AnioCampanaVenta,CodEbelista,CodCUC,CodSAP,CodVenta,ZonaPortal,DiaInicio,DiaFin,Orden,
	--CodVinculo,PPU,LimUnidades,FlagUltMinuto)
	--SELECT CodPais,Tipo,AnioCampanaVenta,rtrim(CodEbelista) CodEbelista,rtrim(CodProducto) CodCUC,
	--       rtrim(CodSAP) CodSAP,rtrim(CodVenta) CodVenta,Portal,DiaInicio,DiaFin,Orden,CodVinculo,PPU,LimUnidades,FlagUltMinuto
	--FROM #ListadoInterfazFinal
END

/** Estimación **/
ELSE 
BEGIN

	SELECT CodTactica, 
	       COUNT(DISTINCT PKEbelista) TotalConsultoras 
	INTO #TotalConsultorasTactica 
	FROM #ListadoBPT
	GROUP BY CodTactica

	SELECT B.CodCUC CodProducto,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre, 
	       MAX(B.DesProductoCUC) DesProducto,
	       MAX(B.DesMarca) DesMarca, 
		   MAX(B.DesCategoria) DesCategoria, 
		   MAX(B.DesTipoSolo) DesTipoSolo
	INTO #ProductosCUCTotales 
	FROM #ProductosTotales A 
	--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais= @CodPais
	WHERE B.DesProductoCUC IS NOT NULL
	GROUP BY B.CodCUC,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre 

	--DELETE BDDM01.DATAMARTANALITICO.dbo.ARP_TotalProductosEstimados
	DELETE BD_ANALITICO.dbo.ARP_TotalProductosEstimados
	WHERE Codpais=@Codpais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo
	AND TipoARP=@TipoARP AND TipoPersonalizacion='BPT'

	--INSERT INTO BDDM01.DATAMARTANALITICO.dbo.ARP_TotalProductosEstimados
	INSERT INTO BD_ANALITICO.dbo.ARP_TotalProductosEstimados
	SELECT @Codpais,'BPT' AS TipoPersonalizacion, @AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@Perfil,B.TipoTactica,A.CodTactica,
	       B.DesMarca,B.DesCategoria,B.DesTipoSolo,B.CodProducto,DesProducto,B.Unidades,
		   SUM(A.TotalConsultoras) TotalConsultoras
	FROM #TotalConsultorasTactica A INNER JOIN #ProductosCUCTotales B ON A.CodTactica=B.CodTactica
	GROUP BY B.TipoTactica,A.CodTactica,B.DesMarca,B.DesCategoria,B.DesTipoSolo,B.CodProducto,DesProducto,B.Unidades
		
END

 --select distinct tipopersonalizacion from ARP_OfertaPersonalizadaC01_Resultado
 /** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,TipoGP,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,'BPT',@TipoARP,@FlagCarga,@FlagMC,@Perfil,@FlagOF,NULL,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

END





