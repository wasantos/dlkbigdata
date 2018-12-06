CREATE PROCEDURE pARP_ReglasNegocio_SR @CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoARP CHAR(1),
@FlagMC INT,@Perfil VARCHAR(1)
AS
BEGIN 

/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoARP						CHAR(1)
--DECLARE @FlagCarga					INT
DECLARE @Perfil							VARCHAR(1)
DECLARE @FlagMC							INT
--DECLARE @EspaciosOF					INT

SET @CodPais				= 'PA'		-- Código de país
SET @AnioCampanaProceso		= '201702'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201707'	-- Campaña de Venta
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
--SET @FlagCarga			= 0			-- 1: Carga/Personalización | 0: Estimación
SET @FlagMC					= 0			-- 1: Con Motor de Canibalización | 0: Sin Motor de Canibalización 
SET @Perfil					= 'X'		-- Número de Perfil | 'X': Sin Perfil
--SET @EspaciosOF			= 12		-- Número de espacios para Oferta Final*/

DECLARE @NumEspaciosFijos INT
DECLARE @NumEspaciosLibres INT
DECLARE @NumEspaciosTop INT
DECLARE @i INT
DECLARE @NumEspacios INT

SET @i=1  

/** Inicio: Variables Log **/
DECLARE @FechaInicio 			DATETIME 
DECLARE @Procedimiento			VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_ReglasNegocio_SR'
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

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,Agrupador 
	INTO ListadoProductos 
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP --AND TipoPalanca=@TipoPalanca AND Perfil=@Perfil

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta 
	INTO ListadoRegalos
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP --AND TipoPalanca=@TipoPalanca AND Perfil=@Perfil

	--Se Guardan el números de espacios forzados
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio 
	INTO EspaciosForzados  
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	--AND TipoPalanca=@TipoPalanca AND Perfil=@Perfil

	--Se guarda el números de espacios total
	SELECT DISTINCT AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios 
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	--AND TipoPalanca=@TipoPalanca AND Perfil=@Perfil

END

/** Estimación **/
ELSE 
BEGIN*/

    --Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,CodVinculoOF
	INTO ListadoProductos 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='SR' AND Perfil=@Perfil

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta 
	INTO ListadoRegalos
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='SR' AND Perfil=@Perfil

	--Se Guardan el números de espacios
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio 
	INTO EspaciosForzados  
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados_Est
	FROM BD_ANALITICO.dbo.ARP_EspaciosForzados_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='SR' AND Perfil=@Perfil

	--Se guarda el numeros de espacios
	SELECT DISTINCT AnioCampanaExpo AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='SR' AND Perfil=@Perfil

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop 
	INTO #ProductosTotales
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo = @AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='SR' AND Perfil=@Perfil

--END

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
	   MAX(B.DESCategoria) DESCategoria,
       MAX(LimUnidades) LimUnidades,
	   MAX(FlagUltMinuto) FlagUltMinuto 
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
INTO #ListadoConsultora_Total2
--FROM BDDM01.[DATAMARTANALITICO].[dbo].[ARP_ListadoProbabilidades] 
FROM BD_ANALITICO.dbo.ARP_ListadoProbabilidades (NOLOCK)
WHERE Probabilidad!=0 
AND CodPais=@CodPais 
AND AnioCampanaProceso=@AnioCampanaProceso 
AND AnioCampanaExpo=@AnioCampanaExpo 
AND TipoARP=@TipoARP 
AND TipoPersonalizacion='SR'
AND FlagMC=@FlagMC
AND Perfil=@Perfil

IF (@TipoARP='E')
BEGIN

	SELECT DISTINCT pkebelista,CodTactica,FlagTop,Probabilidad,
	ROW_NUMBER()OVER(PARTITION BY PkEbelista ORDER BY Probabilidad DESC) AS Posicion
	INTO #Temporal_Ranking
	FROM #ListadoConsultora_Total2 
	ORDER BY PKEbelista, Probabilidad DESC

	--Se separan las tácticas de Oferta Final (Se toma el 3er producto)
	SELECT pkebelista,CodTactica,Probabilidad,Posicion AS Orden,1 OrdenInterfaz 
	INTO #ListadoOF
	FROM #Temporal_Ranking 
	WHERE Posicion = 3

	DELETE A
	FROM #ListadoConsultora_Total2 A 
	INNER JOIN #Temporal_Ranking C ON A.CodTactica=C.CodTactica AND A.PKEbelista=C.pkEbelista
	WHERE Posicion = 3

	--Se separan las tácticas de Oferta Final (Se copia el 1ero y 2do)
	INSERT INTO #ListadoOF
	SELECT PKEbelista,CodTactica,Probabilidad,Posicion AS Orden,2 OrdenInterfaz 
	FROM #Temporal_Ranking 
	WHERE Posicion = 1
	INSERT INTO #ListadoOF
	SELECT PKEbelista,CodTactica,Probabilidad,Posicion AS Orden,3 OrdenInterfaz 
	FROM #Temporal_Ranking 
	WHERE Posicion = 2

END

--Tipo: N (No Forzado) F(Forzado) T(Top)
/** 1.1. TOPS **/

--Se Ordenan las tácticas según la probabilidad: 
SELECT DISTINCT pkebelista,CodTactica,Probabilidad,
	   ROW_NUMBER()OVER(PARTITION BY PkEbelista ORDER BY Probabilidad DESC) as Posicion
INTO #Temporal_Tops
FROM #ListadoConsultora_Total2 
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
FROM #ListadoConsultora_Total2 
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
INTO #ListadoOPT
FROM #ListaRecomendados 

/*SELECT DISTINCT @CodPais CodPais,'OPT' Tipo,@AnioCampanaExpo AnioCampanaVenta,CodEbelista,
CodProducto,CodSAP,CodVenta,'IDP' Portal,0 DiaIni,0 DiaFin, Orden,0 FlagManual,@TipoARP as TipoARP,
0 CodVinculo, 0.0 PPU,B.LimUnidades, B.FlagUltMinuto
INTO #ListadoInterfazOPT 
FROM #ListadoOPT A 
INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1
INNER JOIN DEBELISTA C ON A.PKEbelista=C.PKEbelista*/

/*INSERT INTO #ListadoInterfazFinal
SELECT * 
FROM #ListadoInterfazOPT*/

/** Carga - Personalización **/
/*IF (@FlagCarga = 1)
BEGIN*/
			
	--DELETE BDDM01.DATAMARTANALITICO.dbo.ARP_OfertaPersonalizadaC01
	--WHERE CodPais=@CodPais AND AnioCampanaVenta=@AnioCampanaExpo and TipoARP=@TipoARP

	--Generación de la Interfaz
	--INSERT INTO BDDM01.DATAMARTANALITICO.dbo.ARP_OfertaPersonalizada_ODD
	--SELECT * FROM #ListadoInterfazFinal

/*END

/** Estimación **/
ELSE 
BEGIN*/

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
	--DELETE BD_ANALITICO.dbo.ARP_TotalProductosEstimados
	--WHERE codpais=@Codpais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo
	--AND TipoARP=@TipoARP AND TipoPersonalizacion='SR'

	INSERT INTO BD_ANALITICO.dbo.ARP_TotalProductosEstimados
	--INSERT INTO BD_ANALITICO.dbo.ARP_TotalProductosEstimados_Pruebas
	SELECT @CodPais,'SR' AS TipoPersonalizacion,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@Perfil,B.TipoTactica,A.CodTactica,
	       B.DesMarca,B.DesCategoria,B.DesTipoSolo,B.CodProducto,DesProducto,B.Unidades,
		   SUM(A.TotalConsultoras) TotalConsultoras
	FROM #TotalConsultorasTactica A INNER JOIN #ProductosCUCTotales B ON A.CodTactica=B.CodTactica
	GROUP BY B.TipoTactica,A.CodTactica,B.DesMarca,B.DesCategoria,B.DesTipoSolo,B.CodProducto,DesProducto,B.Unidades
		
--END

/** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,TipoGP,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,'SR',@TipoARP,NULL,@FlagMC,@Perfil,NULL,NULL,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

END

