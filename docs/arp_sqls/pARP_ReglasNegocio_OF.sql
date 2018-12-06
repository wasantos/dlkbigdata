CREATE PROCEDURE pARP_ReglasNegocio_OF @CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoARP CHAR(1),
@FlagMC INT,@Perfil VARCHAR(1)
AS
BEGIN

/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoARP						CHAR(1)
DECLARE @FlagCarga						INT
DECLARE @FlagMC							INT
DECLARE @Perfil							VARCHAR(1)

SET @CodPais				= 'CL'	    -- Código de país
SET @AnioCampanaProceso		= '201703'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201704'	-- Campaña de Venta
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
SET @FlagCarga				= 1			-- 1: Carga/Personalización | 0: Estimación
SET @FlagMC					= 1			-- 1: Con Motor de Canibalización | 0: Sin Motor de Canibalización
SET @Perfil					= '1'		-- Número de Perfil | 'X': Sin Perfil*/

DECLARE @EspaciosOF						INT
SET @EspaciosOF				= 12		-- Número de espacios para Oferta Final

DECLARE @AnioCampanaInicio6UC CHAR(6)  
DECLARE @NumEspaciosFijos INT
DECLARE @NumEspaciosLibres INT
DECLARE @NumEspaciosTop INT
DECLARE @i INT
DECLARE @NumEspacios INT
DECLARE @NumProd INT

SET @i=1  
SET @AnioCampanaInicio6UC = dbo.CalculaAnioCampana(@AnioCampanaProceso, -5)  

/** Inicio: Variables Log **/
DECLARE @FechaInicio 	DATETIME 
DECLARE @Procedimiento	VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_ReglasNegocio_OF'
/** Fin: Variables Log **/

IF OBJECT_ID('ListadoProductos') is not null
DROP TABLE ListadoProductos

IF OBJECT_ID('ListadoRegalos') is not null
DROP TABLE ListadoRegalos

IF OBJECT_ID('EspaciosForzados') is not null
DROP TABLE EspaciosForzados

IF OBJECT_ID('CampaniaExpoEspacios') is not null
DROP TABLE CampaniaExpoEspacios

 /** Carga - Personalización **/
/*IF (@FlagCarga = 1) 
BEGIN
*/
	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,CodVinculoOF
	INTO ListadoProductos 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='OF'
	AND Perfil=@Perfil

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta 
	INTO ListadoRegalos
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='OF'
	AND Perfil=@Perfil

	--Se Guardan el números de espacios forzados
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio 
	INTO EspaciosForzados  
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados
	FROM BD_ANALITICO.dbo.ARP_EspaciosForzados (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='OF'
	AND Perfil=@Perfil

	--Se guarda el números de espacios total
	SELECT DISTINCT AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='OF'
	AND Perfil=@Perfil

/*END

/** Estimación **/
ELSE 
BEGIN

    --Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,Agrupador
	                NULL AS Agrupador
	INTO ListadoProductos 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='OF' AND Perfil=@Perfil

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta 
	INTO ListadoRegalos
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='OF' AND Perfil=@Perfil

	--Se Guardan el números de espacios
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio 
	INTO EspaciosForzados  
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados_Est
	FROM BD_ANALITICO.dbo.ARP_EspaciosForzados_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='OF' AND Perfil=@Perfil

	--Se guarda el numeros de espacios
	SELECT DISTINCT AnioCampanaExpo AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='OF' AND Perfil=@Perfil

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop 
	INTO #ProductosTotales
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo = @AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='OF' AND Perfil=@Perfil

END
*/
SELECT * INTO #ListadoProductos FROM ListadoProductos
SELECT * INTO #ListadoRegalos FROM ListadoRegalos
SELECT * INTO #EspaciosForzados FROM EspaciosForzados
SELECT * INTO #CampaniaExpoEspacios FROM CampaniaExpoEspacios

SELECT @NumEspacios = ISNULL(Espacios,0), 
       @NumEspaciosTop = ISNULL(EspaciosTop,0) 
FROM CampaniaExpoEspacios

--Se guardan los espacios Fijos
SELECT DISTINCT Marca,Categoria,TipoForzado,VinculoEspacio 
INTO #EspaciosFijos 
FROM EspaciosForzados 
WHERE TipoForzado=1

--Se guardan las marcas y categorias que no se deben repetir en los espacios no forzados
SELECT DISTINCT Marca,Categoria,TipoForzado
INTO #MarcaCategoriaNoRepetir 
FROM EspaciosForzados 
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
	   MAX(B.DesCategoria) DesCategoria,
       MAX(LimUnidades) LimUnidades,
	   MAX(FlagUltMinuto) FlagUltMinuto,
	   MAX(A.CodVinculoOF) CodVinculoOF
INTO #ProductosCUC 
FROM ListadoProductos A 
--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
WHERE B.DesProductoCUC IS NOT NULL
GROUP BY B.CodCUC,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre,FlagTop 

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

SELECT DISTINCT PKEbelista,CodTactica,FlagTop,Probabilidad 
INTO #ListadoConsultora_Total
--FROM BDDM01.[DATAMARTANALITICO].[dbo].[ARP_ListadoProbabilidades] 
FROM BD_ANALITICO.dbo.ARP_ListadoProbabilidades (NOLOCK)
WHERE CodPais=@CodPais 
AND AnioCampanaProceso=@AnioCampanaProceso 
AND AnioCampanaExpo=@AnioCampanaExpo 
AND TipoARP=@TipoARP 
AND TipoPersonalizacion='OF'
AND FlagMC=@FlagMC
AND Perfil=@Perfil

/*--Táctica Bundle
SELECT A.PKEbelista,B.CodTactica,A.FlagTop,A.Probabilidad 
INTO #Listado_Bundle
FROM #ListadoConsultora_Total2 A INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica
WHERE B.TipoTactica='Bundle' 
AND A.Probabilidad!=0

--Táctica Individual: Se filtra el registro con mayor score
SELECT a.Pkebelista,A.TipoTactica,B.CodProducto,
	   MAX(A.Probabilidad) AS Probabilidad
INTO #MaxScore
FROM #ListadoConsultora_Total2 A 
INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica
WHERE B.TipoTactica='Individual' 
AND A.Probabilidad!=0 
GROUP BY A.Pkebelista,A.TipoTactica,B.CodProducto

SELECT A.PKEbelista,B.CodTactica,A.FlagTop,A.Probabilidad  
INTO #Listado_Individual
FROM #ListadoConsultora_Total2 A
INNER JOIN #MaxScore B ON A.PkEbelista=B.PkEbelista AND A.Probabilidad=B.Probabilidad
WHERE A.TipoTactica='Individual' 
GROUP BY A.pkebelista,A.TipoTactica,B.CodTactica,A.FlagTop,A.Probabilidad 

--Unión de tácticas
SELECT x.* 
INTO #ListadoConsultora_Final
FROM
(SELECT * 
FROM #Listado_Individual
UNION ALL
SELECT * 
FROM #Listado_Bundle) x*/

/** Inicio: Eliminación de CUCs repetidos **/

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

/** 1. OF para Establecidas **/
IF (@TipoARP='E')
BEGIN

	/** 1.1. Rank OF **/

	SELECT A.PKEbelista,A.CodTactica, A.FlagTop,A.Probabilidad
	INTO #ListadoConsultora_Total_OF
    FROM #ListadoConsultora_Total A INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica
	WHERE B.TipoTactica='Individual' 
	and A.Probabilidad!=0

	SELECT DISTINCT PKEbelista,CodTactica,Probabilidad,
	                ROW_NUMBER()OVER(PARTITION BY PkEbelista ORDER BY Probabilidad DESC) AS Posicion
	INTO #Temporal_OF
	FROM #ListadoConsultora_Total_OF 
	WHERE Probabilidad!=0 
	GROUP BY pkebelista,CodTactica,Probabilidad
	ORDER BY pkebelista,Probabilidad DESC

	SELECT DISTINCT PKEbelista,CodTactica,Probabilidad, 0 Orden 
	INTO #ListaRecomendadosOF
	FROM #Temporal_OF
	WHERE Posicion<=@NumEspacios

	SELECT A.pkebelista,B.Codebelista,A.CodTactica,A.Probabilidad,
	       ROW_NUMBER()OVER(PARTITION BY A.PkEbelista ORDER BY A.Probabilidad DESC) AS Orden
	INTO #ListadoOFFinal 
	FROM #ListaRecomendadosOF A INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA B ON A.PKEbelista=B.PKEbelista AND B.CodPais=@CodPais

	/** 1.2. Proceso PPU**/

	SELECT DISTINCT PkEbelista 
	INTO #ConsultorasOF 
	FROM #ListadoOFFinal

	SELECT B.CodMarca, B.CodCategoria, B.CodTipo 
	INTO #MCC
	--FROM DMATRIZCAMPANA A
	FROM DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA A (NOLOCK)
	--INNER JOIN DPRODUCTO B ON A.PKProducto = B.PKProducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.PKProducto=B.PKProducto AND B.CodPais=@CodPais
	WHERE A.AnioCampana = @AnioCampanaExpo 
	AND B.CodMarca IN ('A','B','C')
	AND B.DesCategoria IN ('CUIDADO PERSONAL','FRAGANCIAS','MAQUILLAJE','TRATAMIENTO CORPORAL','TRATAMIENTO FACIAL')
	AND A.CodPais=@CodPais
	GROUP BY B.CodMarca, B.CodCategoria, B.CodTipo

	--SIN CONSULTORA
	SELECT C.CodMarca, C.CodCategoria, C.CodTipo, 
	       AVG(A.RealVtaMNCatalogo/A.RealUUVendidas) AS PPU, 
	       STDEVP(A.RealVtaMNCatalogo/A.RealUUVendidas) STDeva,
	       (1.5*STDEVP(A.RealVtaMNCatalogo/A.RealUUVendidas) + AVG(A.RealVtaMNCatalogo/A.RealUUVendidas) ) AS SDePPU
	INTO #Tempoa
	--FROM FVTAPROEBECAMC01 A
	FROM DWH_ANALITICO.dbo.DWH_FVTAPROEBECAM A (NOLOCK)
	--INNER JOIN DPRODUCTO C on A.pkproducto = C.pkproducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO C (NOLOCK) ON A.PKProducto=C.PKProducto AND C.CodPais=@CodPais
	INNER JOIN #MCC D ON C.CodMarca = D.CodMarca AND C.CodCategoria = D.CodCategoria AND C.CodTipo = D.CodTipo
	WHERE A.AnioCampana = A.AnioCampanaRef 
    AND A.RealUUVendidas > 0 
	AND A.RealVtaMNCatalogo > 0
	AND A.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso
	GROUP BY C.CodMarca, C.CodCategoria, C.CodTipo

	---POR CONSULTORA CON MARCA,CATEGORIA Y TIPO CUANTO PPU TOCA
	SELECT A.PKEbelista,C.CodMarca, C.CodCategoria, C.CodTipo, 
	       AVG(A.RealVtaMNCatalogo/A.RealUUVendidas) AS PPU, 
	       STDEVP(a.RealVtaMNCatalogo/A.RealUUVendidas) STDeva,
	       (1.5*STDEVP(A.RealVtaMNCatalogo/A.RealUUVendidas) + AVG(A.RealVtaMNCatalogo/A.RealUUVendidas) ) AS SDePPU
	INTO #Tempo
	--FROM FVTAPROEBECAMC01 A
	FROM DWH_ANALITICO.dbo.DWH_FVTAPROEBECAM A (NOLOCK)
	INNER JOIN #ConsultorasOF B on a.PKEbelista = B.PKEbelista
	--INNER JOIN DPRODUCTO C on A.pkproducto = C.pkproducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO C (NOLOCK) ON A.PKProducto=C.PKProducto AND C.CodPais=@CodPais
	INNER JOIN #MCC D ON C.CodMarca = D.CodMarca AND C.CodCategoria = D.CodCategoria AND C.CodTipo = D.CodTipo
	WHERE A.AnioCampana = a.AnioCampanaRef 
	AND A.RealUUVendidas > 0 
	AND A.RealVtaMNCatalogo > 0 
	AND A.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso
	GROUP BY A.PKEbelista,C.CodMarca, C.CodCategoria, C.CodTipo

	/** 1.3. Tabla Final **/

	SELECT @CodPais AS CodPais,'OF' Tipo,@AnioCampanaExpo AnioCampanaVenta,CodEbelista,
	       CodProducto,CodSAP,CodVenta,Portal,DiaInicio,
	       DiaFin,Orden,FlagManual,TipoARP,CodVinculo,PPU,LimUnidades,FlagUltMinuto
	INTO #CTE_OFFINAL FROM
	(
	SELECT DISTINCT A.CodEbelista CodEbelista, B.CodProducto CodProducto,B.CodSAP,B.CodVenta, 'IDP' Portal,
	                0 DiaInicio,0 DiaFin,Orden,0 FlagManual,'E' TipoARP,0 CodVinculo, 
	                X.SDePPU AS PPU ,B.LimUnidades, B.FlagUltMinuto,C.CodMarca,C.CodCategoria,C.CodTipo
	FROM #ListadoOFFinal A
	INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica 
	--INNER JOIN DPRODUCTO C ON B.CODPRODUCTO=C.codCUC 
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO C (NOLOCK) ON B.CodProducto=C.CodCUC AND C.CodPais=@CodPais
	--INNER JOIN DMATRIZCAMPANA D ON C.PkProducto=D.PkProducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA D (NOLOCK) ON C.PkProducto=D.PkProducto AND D.CodPais=@CodPais
	INNER JOIN #Tempo X ON A.PkEbelista=X.PkEbelista AND C.CodMarca=X.CodMArca AND C.CodCategoria=X.CodCategoria AND C.CodTipo=X.CodTipo
	WHERE B.IndicadorPadre=1 
	AND D.AnioCampana=@AnioCampanaExpo
	AND X.SDePPU IS NOT NULL
	UNION ALL
	SELECT DISTINCT A.CodEbelista CodEbelista, B.CodProducto CodProducto,B.CodSAP,B.CodVenta, 'IDP' Portal,
	                0 DiaInicio,0 DiaFin,Orden,0 FlagManual,'E' TipoARP,0 CodVinculo, 
	                Z.SDePPU AS PPU ,B.LimUnidades, B.FlagUltMinuto,C.CodMarca,C.CodCategoria,C.CodTipo
	FROM #ListadoOFFinal A
	INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica
	--INNER JOIN DPRODUCTO C ON B.CODPRODUCTO=C.CodCUC 
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO C (NOLOCK) ON B.CodProducto=C.CodCUC AND C.CodPais=@CodPais
	--INNER JOIN DMATRIZCAMPANA D ON C.PkProducto=D.PkProducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA D (NOLOCK) ON C.PkProducto=D.PkProducto AND D.CodPais=@CodPais
	INNER JOIN #Tempoa Z ON C.CodMarca=Z.CodMArca AND C.CodCategoria=Z.CodCategoria AND C.CodTipo=Z.CodTipo
	LEFT JOIN #Tempo X ON A.PkEbelista=X.PkEbelista AND C.CodMarca=X.CodMArca AND C.CodCategoria=X.CodCategoria AND C.CodTipo=X.CodTipo
	WHERE B.IndicadorPadre=1 
	AND D.AnioCampana= @AnioCampanaExpo
	AND x.SDePPU IS NULL) PF

	INSERT INTO #ListadoInterfazFinal 
	SELECT DISTINCT @CodPais CodPais,'OF' Tipo,@AnioCampanaExpo AnioCampanaVenta,A.CodEbelista,
	                A.CodProducto,A.CodSAP,A.CodVenta,Portal,DiaInicio AS DiaIni,DiaFin, Orden,FlagManual ,'E' TipoARP,
	                B.CodVinculoOF as CodVinculo, CAST(PPU AS DECIMAL(18,4)) AS PPU,A.LimUnidades, A.FlagUltMinuto,
					@Perfil Perfil
	FROM #CTE_OFFINAL A
	LEFT JOIN #ProductosCUC B ON A.CodProducto=B.CodProducto AND A.CodVenta=B.CodVenta AND B.IndicadorPadre=1
	WHERE B.TipoTactica='Individual' 
	ORDER BY CodEbelista,Orden

END

/** Carga - Personalización **/
/*IF (@FlagCarga = 1)
BEGIN*/
			
	--DELETE BDDM01.DATAMARTANALITICO.dbo.ARP_OfertaPersonalizadaC01
	--DELETE BD_ANALITICO.dbo.ARP_OfertaPersonalizada_Pruebas
	--WHERE CodPais=@CodPais AND AnioCampanaVenta=@AnioCampanaExpo and TipoARP=@TipoARP AND TipoPersonalizacion='OF'

	--INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizada_Pruebas
	INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizadaC01
	(CodPais,TipoPersonalizacion,AnioCampanaVenta,CodEbelista,CodCUC,CodSAP,CodVenta,ZonaPortal,DiaInicio,DiaFin,Orden,FlagManual,
	TipoARP,CodVinculo,PPU,LimUnidades,FlagUltMinuto,Perfil)
	SELECT * FROM #ListadoInterfazFinal

	--Generación de la Interfaz
	--INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizada
	--(CodPais,TipoPersonalizacion,AnioCampanaVenta,CodEbelista,CodCUC,CodSAP,CodVenta,ZonaPortal,DiaInicio,DiaFin,Orden,
	--CodVinculo,PPU,LimUnidades,FlagUltMinuto)
	--SELECT CodPais,Tipo,AnioCampanaVenta,rtrim(CodEbelista) CodEbelista,rtrim(CodProducto) CodCUC,
	--       rtrim(CodSAP) CodSAP,rtrim(CodVenta) CodVenta,Portal,DiaInicio,DiaFin,Orden,CodVinculo,PPU,LimUnidades,FlagUltMinuto
	--FROM #ListadoInterfazFinal

/*END

/** Estimación **/
ELSE 
BEGIN

	SELECT CodTactica, 
	       COUNT(DISTINCT PKEbelista) TotalConsultoras 
	INTO #TotalConsultorasTactica 
	FROM #ListadoOPT
	GROUP BY CodTactica

	SELECT B.CodCUC CodProducto,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre, 
	       MAX(B.DesProductoCUC) DesProducto,
	       MAX(B.DesMarca) DesMarca, 
		   MAX(B.DesCategoria) DesCategoria, 
		   MAX(B.DesTipoSolo) DesTipoSolo
	INTO #ProductosCUCTotales 
	FROM #ProductosTotales A 
	--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
	WHERE B.DesProductoCUC IS NOT NULL
	GROUP BY B.CodCUC,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre 

	--DELETE BDDM01.DATAMARTANALITICO.dbo.ARP_TotalProductosEstimados
	--DELETE BD_ANALITICO.dbo.ARP_TotalProductosEstimados_Pruebas
	--WHERE Codpais=@Codpais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo
	--AND TipoARP=@TipoARP AND TipoPersonalizacion='OPT'

	--INSERT INTO BDDM01.DATAMARTANALITICO.dbo.ARP_TotalProductosEstimados
	INSERT INTO BD_ANALITICO.dbo.ARP_TotalProductosEstimados_Pruebas
	SELECT @Codpais,'OF' AS TipoPersonalizacion, @AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,B.TipoTactica,A.CodTactica,
	       B.DesMarca,B.DesCategoria,B.DesTipoSolo,B.CodProducto,DesProducto,B.Unidades,
		   SUM(A.TotalConsultoras) TotalConsultoras
	FROM #TotalConsultorasTactica A INNER JOIN #ProductosCUCTotales B ON A.CodTactica=B.CodTactica
	GROUP BY B.TipoTactica,A.CodTactica,B.DesMarca,B.DesCategoria,B.DesTipoSolo,B.CodProducto,DesProducto,B.Unidades
		
END*/

/** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,'OF',@TipoARP,NULL,@FlagMC,@Perfil,NULL,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

END


