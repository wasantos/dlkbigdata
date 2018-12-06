CREATE PROCEDURE [dbo].[pARP_RFM] @CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoPersonalizacion CHAR(3),@TipoARP CHAR(1),
@FlagCarga INT,@Perfil VARCHAR(1)
AS
BEGIN 

/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoPersonalizacion			CHAR(3) 
DECLARE @TipoARP						CHAR(1)
DECLARE @FlagCarga						INT
DECLARE @Perfil							VARCHAR(1)

SET @CodPais				= 'PA'		-- Código de país
SET @AnioCampanaProceso		= '201705'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201707'	-- Campaña de Venta
SET @TipoPersonalizacion	= 'ODD'		-- 'OPT','ODD','SR'
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
SET @FlagCarga				=  1		-- 1: Carga/Personalización | 0: Estimación
SET @Perfil					= '1'		-- Número de Perfil | 'X': Sin Perfil*/

DECLARE @AnioCampanaProceso_Menos3 CHAR(6)  
DECLARE @AnioCampanaProceso_Menos4 CHAR(6)  
DECLARE @AnioCampanaProceso_Menos5 CHAR(6)  
DECLARE @AnioCampanaProceso_Menos17 CHAR(6)  
DECLARE @AnioCampanaInicio6UC CHAR(6)  
DECLARE @AnioCampanaInicio24UC CHAR(6)  
DECLARE @AnioCampanaInicio6PUC CHAR(6) 
DECLARE @AnioCampanaFin6PUC CHAR(6) 
DECLARE @AnioCampanaProceso_AA CHAR(6)  
DECLARE @AnioCampanaInicio_AA CHAR(6)  
DECLARE @TotalConsultorasCtes INT
DECLARE @TotalConsultorasInCtes INT

SET @AnioCampanaProceso_Menos3	= dbo.CalculaAnioCampana(@AnioCampanaProceso, -2)  
SET @AnioCampanaProceso_Menos4	= dbo.CalculaAnioCampana(@AnioCampanaProceso, -3) 
SET @AnioCampanaProceso_Menos5	= dbo.CalculaAnioCampana(@AnioCampanaProceso, -4) 
SET @AnioCampanaProceso_Menos17 = dbo.CalculaAnioCampana(@AnioCampanaProceso, -17) 
SET @AnioCampanaInicio6UC		= dbo.CalculaAnioCampana(@AnioCampanaProceso, -5)  
SET @AnioCampanaInicio6PUC		= dbo.CalculaAnioCampana(@AnioCampanaProceso, -11)   
SET @AnioCampanaFin6PUC			= dbo.CalculaAnioCampana(@AnioCampanaProceso, -6)   
SET @AnioCampanaInicio24UC		= dbo.CalculaAnioCampana(@AnioCampanaProceso, -23)   
SET @AnioCampanaProceso_AA		= dbo.CalculaAnioCampana(@AnioCampanaProceso, -18)  
SET @AnioCampanaInicio_AA		= dbo.CalculaAnioCampana(@AnioCampanaProceso, -23)  

/** Inicio: Variables Log **/
DECLARE @FechaInicio 					DATETIME 
DECLARE @Procedimiento					VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_RFM'
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
IF (@FlagCarga = 1) 
BEGIN

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,CodVinculoOF 
	INTO ListadoProductos 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta 
	INTO ListadoRegalos
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

	--Se guardan el números de espacios forzados
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio 
	INTO EspaciosForzados  
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados
	FROM BD_ANALITICO.dbo.ARP_EspaciosForzados (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

	--Se guarda el números de espacios total
	SELECT DISTINCT AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

END

/** Estimación **/
ELSE 
BEGIN

    --Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,CodVinculoOF
	INTO ListadoProductos 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta 
	INTO ListadoRegalos
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

	--Se guardan el números de espacios
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio 
	INTO EspaciosForzados  
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados_Est
	FROM BD_ANALITICO.dbo.ARP_EspaciosForzados_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

	--Se guarda el numeros de espacios
	SELECT DISTINCT AnioCampanaExpo AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop 
	INTO #ProductosTotales
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo = @AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil

END

SELECT * INTO #ListadoProductos FROM ListadoProductos
SELECT * INTO #ListadoRegalos FROM ListadoRegalos
SELECT * INTO #EspaciosForzados FROM EspaciosForzados
SELECT * INTO #CampaniaExpoEspacios FROM CampaniaExpoEspacios

--Leo la Base de Consultoras
SELECT * 
INTO #InfoConsultora 
--FROM BDDM01.[DATAMARTANALITICO].DBO.ARP_BaseConsultoras 
FROM BD_ANALITICO.dbo.ARP_BaseConsultoras (NOLOCK)
WHERE AnioCampanaProceso = @AnioCampanaProceso 
AND CodPais = @CodPais 
AND TipoARP = @TipoARP
AND Perfil = @Perfil

SELECT PKEbelista 
INTO #BaseConsultoras 
FROM #InfoConsultora

/*--CAMBIO
SELECT *--PKEbelista,AnioCampana,FlagPasoPedido,FlagActiva,CodComportamientoRolling,codigofacturaINTernet 
INTO #FSTAEBECAM 
FROM FSTAEBECAMC01_VIEW
WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso 

---Base de Consultoras: aquellas con Pedidos en las 3 últimas campanas   
SELECT DISTINCT pkebelista INTO #BaseConsultoras1 FROM FVTAEBECAMC01_18  
WHERE  AnioCampana BETWEEN @AnioCampanaProceso_Menos3 AND @AnioCampanaProceso   
AND CodTipoProfit = '01' and aniocampana=aniocampanaref  
GROUP BY pkebelista  
HAVING SUM(RealVTAMNNeto)>0  

-- Consultoras Nuevas en su 5to y 6to pedido
SELECT DISTINCT A.PKEbelista  INTO #BaseConsultoras2 FROM FSTAEBECAMC01_VIEW A 
INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista  
WHERE B.AnioCampanaIngreso BETWEEN @AnioCampanaInicio6UC and @AnioCampanaProceso_Menos5      

--Se agregan las consultoras nuevas en su 5to y 6to pedido a las establecidas
SELECT T.pkebelista INTO #BaseConsultoras3 FROM (
SELECT pkebelista FROM #BaseConsultoras1
UNION 
SELECT pkebelista FROM #BaseConsultoras2)T
		
-- Consultoras Nuevas en su 1er, 2do, 3er y 4to pedido
SELECT DISTINCT A.PKEbelista  INTO #BaseConsultoras4 FROM FSTAEBECAMC01_VIEW A 
INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista 
WHERE B.AnioCampanaIngreso  BETWEEN  @AnioCampanaProceso_Menos4 and @AnioCampanaProceso                                                     

SELECT pkebelista INTO #BaseConsultoras FROM #BaseConsultoras3 Where pkebelista 
NOT IN (SELECT PkEbelista FROM #BaseConsultoras4) 

-- Se obtiene la Información Actual de las Consultoras, Región, Segmento 
 SELECT A.PKEbelista,A.CodEbelista, D.CodRegion,B.CodComportamientoRolling, D.DesRegion,D.CodZona,D.DesZona,
 dbo.DiffANIOCampanas(@AnioCampanaProceso, A.AnioCampanaIngreso)+1 Antiguedad INTO #InfoConsultora  
 FROM DEBELISTA A 
 INNER JOIN #FSTAEBECAM B ON A.PKEbelista=B.PKEbelista AND B.AnioCampana=@AnioCampanaProceso  
 INNER JOIN #BaseConsultoras C ON  A.PKEbelista=C.pkebelista
 INNER JOIN DGEOGRAFIACAMPANA D ON B.PKTerritorio=D.PKTerritorio  AND D.AnioCampana=B.AnioCampana
--CAMBIO*/

--Se obtienen los productos a nivel de Item (Pkproducto) 
SELECT TipoTactica,CodTactica, B.CodCUC CodProducto,Unidades,B.DesProductoCUC DesProducto,A.PrecioOferta,CodMarca,PKProducto 
INTO #Productos 
FROM #ListadoProductos A 
--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais

--Se obtienen los productos a nivel CUC
SELECT B.CodCUC CodProducto,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre,FlagTop,CAST('' AS VARCHAR) CodSAP,
       MAX(B.DesProductoCUC) DesProducto,
	   MAX(B.DesMarca) DesMarca,
	   MAX(B.DesCategoria) DesCategoria,
       MAX(LimUnidades) LimUnidades,
	   MAX(FlagUltMinuto) FlagUltMinuto 
INTO #ProductosCUC 
FROM #ListadoProductos A 
--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
WHERE B.DesProductoCUC IS NOT NULL
GROUP BY B.CodCUC,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre,FlagTop 

--Se Actualiza el código SAP desde la Matriz de Facturación
UPDATE B
SET CodSAP = C.CodSAP
--FROM DMATRIZCAMPANA A INNER JOIN #ProductosCUC B ON A.CodVenta=B.CodVenta AND AnioCampana= @AnioCampanaExpo 
FROM DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA A (NOLOCK) INNER JOIN #ProductosCUC B ON A.CodVenta=B.CodVenta AND AnioCampana= @AnioCampanaExpo 
AND A.CodPais=@CodPais
--INNER JOIN DPRODUCTO C ON A.PKProducto=C.PKProducto
INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO C (NOLOCK) ON A.PKProducto=C.PKProducto AND C.CodPais=@CodPais
WHERE AnioCampana= @AnioCampanaExpo

/** 1. RFM para Establecidas **/
IF (@TipoARP = 'E') 
BEGIN 

	SELECT PKEbelista,AnioCampana,FlagPasoPedido,FlagActiva,CodComportamientoRolling,codigofacturaINTernet 
	INTO #FSTAEBECAM 
	--FROM FSTAEBECAMC01_VIEW
	FROM DWH_ANALITICO.dbo.DWH_FSTAEBECAM (NOLOCK)
	WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso
	AND CodPais=@CodPais

	--Obtengo el estado de las consultoras que conforman la base en las últimas 24 campañas
	SELECT A.PKEbelista,B.AnioCampana,B.FlagPasoPedido,B.FlagActiva 
	INTO #BaseEstadoConsultoras
	FROM #BaseConsultoras A INNER JOIN #FSTAEBECAM B ON A.PKEbelista=B.PKEbelista
	--WHERE B.AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso 

	--Se obtienen los productos regalos a nivel de Item (pkproducto) 
	SELECT CodTactica,TipoTactica, B.CodCUC CodProducto,Unidades,B.DesProductoCUC DesProducto,A.PrecioOferta,CodMarca,PKProducto 
	INTO #ProductosRegalo 
	FROM #ListadoRegalos A 
	--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais

	--Se guardan las unidades por Táctica
	SELECT CodTactica, SUM(Unidades) TotalUnidades 
	INTO #UnidadesTactica 
	FROM #ProductosCUC
	GROUP BY CodTactica

	--Se crea la tabla temporal donde se guardan la venta de las últimas 24 campañas
	CREATE TABLE #TEMP_FVTAPROEBECAM(
		AnioCampana CHAR(6) NULL,
		PKEbelista INT NULL,
		PKProducto INT NULL,
		PKTipoOferta SMALLINT NULL,
		PKTerritorio INT NULL,
		PKPedido INT NULL,
		CodVenta CHAR(6) NULL,
		AnioCampanaRef CHAR(6) NULL,
		RealUUVendidas INT NULL,
		RealVtaMNNeto REAL NULL,
		RealVtaMNFactura REAL NULL,
		RealVtaMNCatalogo REAL NULL
	)
	
	TRUNCATE TABLE BD_ANALITICO.dbo.ARP_FVTAPROEBECAM_PaisU24C
	INSERT INTO BD_ANALITICO.dbo.ARP_FVTAPROEBECAM_PaisU24C
	SELECT A.AnioCampana,A.PKEbelista,A.PKProducto,A.PKTipoOferta,A.PKTerritorio,A.PKPedido,
	       A.CodVenta,A.AnioCampanaRef,A.RealUUVendidas,A.RealVtaMNNeto,A.RealVtaMNFactura,A.RealVtaMNCatalogo
	FROM DWH_ANALITICO.dbo.DWH_FVTAPROEBECAM A (NOLOCK) INNER JOIN #BaseConsultoras D ON A.PKEbelista=D.PKEbelista
	WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso
	AND CodPais=@CodPais

	INSERT INTO #TEMP_FVTAPROEBECAM
	SELECT A.AnioCampana,A.PKEbelista,A.PKProducto,A.PKTipoOferta,A.PKTerritorio,A.PKPedido,
	       A.CodVenta,A.AnioCampanaRef,A.RealUUVendidas,A.RealVtaMNNeto,A.RealVtaMNFactura,A.RealVtaMNCatalogo
	--FROM FVTAPROEBECAMC01 A 
	FROM ARP_FVTAPROEBECAM_PaisU24C A (NOLOCK)
	INNER JOIN #Productos B ON A.PKProducto=B.PKProducto  
	--INNER JOIN DTIPOOFERTA C ON C.PKTipoOferta=A.PKTipoOferta 
	INNER JOIN DWH_ANALITICO.dbo.DWH_DTIPOOFERTA C (NOLOCK) ON C.PKTipoOferta=A.PKTipoOferta AND C.CodPais=@CodPais
	--INNER JOIN #BaseConsultoras D ON A.PKEbelista=D.PKEbelista
	--WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso   
	AND AnioCampana=AnioCampanaRef  
	AND C.CodTipoProfit='01'  
	AND C.CodTipoOferta NOT IN ('030','040','051')
	AND A.RealVtaMNNeto>0
	--AND A.CodPais=@CodPais

	CREATE NONCLUSTERED INDEX [INC_FVTAPROEBECAMC01_PKEbelista] ON #TEMP_FVTAPROEBECAM (PKEbelista,AnioCampana,PKTipoOferta,CodVenta,PKProducto)

	/** 1.1. Táctica Individual **/

	/** 1.1.0. Productos Individual **/

	--A nivel de Item (PKProducto) 
	SELECT CodTactica,TipoTactica, B.CodCUC CodProducto, Unidades,B.DesProductoCUC DesProducto,
	       A.PrecioOferta,CodMarca,PKProducto,DesMarca,DesCategoria,FlagTop 
	INTO #Productos_Individual 
	FROM #ListadoProductos A 
	--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC 
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
	AND TipoTactica = 'Individual'

	--A nivel CUC
	SELECT CodTactica,TipoTactica,CodProducto,Unidades,PrecioOferta,FlagTop,
	       MAX(DesProducto)DesProducto
	INTO #ProductosCUC_Individual 
	FROM #Productos_Individual
	WHERE DesProducto IS NOT NULL
	GROUP BY CodTactica,TipoTactica,CodProducto,Unidades,PrecioOferta,FlagTop

	/** 1.1.1. Información de las U24C (Últimas 24 campañas) **/
	SELECT B.CodTactica, B.CodProducto,A.* 
	INTO #FVTAPROEBECAMU24C   
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Individual B ON A.PKProducto=B.PKProducto  
	--WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso    
	
	-- Frecuencia, Recencia, Venta Acumulada y Venta Promedio U24C
	SELECT PKEbelista,CodTactica,CodProducto,
	       COUNT(DISTINCT AnioCampana) Frecuencia,
	  dbo.DiffANIOCampanas(@AnioCampanaProceso, MAX(AnioCampana)) Recencia, 
	       SUM(RealVtaMNNeto) VentaAcumulada, 
	       SUM(RealVtaMNNeto)/ COUNT(DISTINCT Aniocampana) VentaPromedio,
	       CAST(0 AS FLOAT) PrecioOptimo,
	       CAST(0 AS FLOAT) Gatillador
	INTO #VentaU24C 
	FROM #FVTAPROEBECAMU24C 
	GROUP BY PKEbelista,CodTactica,CodProducto   
	HAVING SUM(REALVtaMNNeto)>0 

	/** 1.1.2. Información de las U6C (Últimas 6 campañas) **/
	SELECT B.CodTactica, B.CodProducto,A.* 
	INTO #FVTAPROEBECAMU6C   
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Individual B ON A.PKProducto=B.PKProducto
	WHERE AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso
	
	SELECT A.PKEbelista,A.CodTactica, A.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaU6C 
	FROM #FVTAPROEBECAMU6C A 
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0

	/** 1.1.3. Información de las U6C AA (Últimas 6 campañas del año anterior) **/
	SELECT A.PKEbelista,B.CodTactica, B.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaU6C_AA 
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Individual B ON A.PKProducto=B.PKProducto  
	WHERE AnioCampana BETWEEN @AnioCampanaInicio_AA AND @AnioCampanaProceso_AA  
	GROUP BY A.PKEbelista,B.CodTactica,B.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0  

	/** 1.1.4. Información de las PU6C (Penúltimas 6 campañas) **/
	SELECT A.PKEbelista,B.CodTactica, B.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaPU6C
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Individual B ON A.PKProducto=B.PKProducto  
	WHERE AnioCampana BETWEEN @AnioCampanaInicio6PUC AND @AnioCampanaFin6PUC  
	GROUP BY A.PKEbelista,B.CodTactica,B.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0  
	
	/** 1.1.5. Cálculo del Gatillador = (N° Pedidos CUC)/(N° Campañas aspeadas CUC) (Últimas 24 campañas) **/
	--N° Pedidos CUC
	SELECT A.PKEbelista, A.CodTactica,A.CodProducto,A.PKProducto, A.AnioCampana 
	INTO #VentaU24C_Pedidos2  
	FROM #FVTAPROEBECAMU24C A INNER JOIN #ProductosCUC_Individual B ON A.CodTactica=B.codtactica
	AND A.CodProducto=B.CodProducto
	WHERE A.RealUUVendidas>=B.Unidades 
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto,A.PKProducto, A.AnioCampana   
	HAVING SUM(REALVtaMNNeto)>0 

	SELECT PKEbelista,CodTactica,CodProducto, 
	       COUNT(DISTINCT AnioCampana) NumPedidosCUC
	INTO #VentaU24C_Pedidos 
	FROM #VentaU24C_Pedidos2
	GROUP BY PKEbelista,CodTactica,CodProducto  
	
	--N° Campañas aspeadas CUC
	SELECT DISTINCT A.CodProducto, B.AnioCampana 
	INTO #AspeosU24C 
	FROM #FVTAPROEBECAMU24C A 
	--INNER JOIN DMATRIZCAMPANA B ON A.AnioCampana=B.AnioCampana AND A.PKProducto=B.PKProducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA B (NOLOCK) ON A.AnioCampana=B.AnioCampana AND A.PKProducto=B.PKProducto
	AND B.CodPais=@CodPais
	AND A.PKTipoOferta=B.PKTipoOferta AND A.CodVenta=B.CodVenta

	--Se contabiliza el total de campañas en la que la consultora pasó pedido
	SELECT DISTINCT PKEbelista, AnioCampana 
	INTO #PedidosU24C 
	FROM #BaseEstadoConsultoras
	WHERE FlagPasoPedido=1 

	CREATE NONCLUSTERED INDEX IDX_PEDIDOSU24C ON #PedidosU24C (PKEbelista,AnioCampana)
	CREATE NONCLUSTERED INDEX IDX_VENTAU24C ON #VentaU24C (PKEbelista) INCLUDE (CodTactica, CodProducto)

	SELECT A.PKEbelista,A.CodTactica,A.CodProducto,
	       COUNT(DISTINCT B.AnioCampana) CampaniasAspeadas 
	INTO  #Aspeos
	FROM  #VentaU24C A INNER JOIN #AspeosU24C B ON A.CodProducto=B.CodProducto
	INNER JOIN #PedidosU24C C ON A.PKEbelista=C.PKEbelista AND B.AnioCampana= C.AnioCampana
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto

	--Cálculo del Gatillador
	UPDATE #VentaU24C
	SET Gatillador = B.NumPedidosCUC * 1.0 / CampaniasAspeadas
	FROM #VentaU24C A INNER JOIN #VentaU24C_Pedidos B ON A.PKEbelista=B.PKEbelista 
	AND A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
	INNER JOIN #Aspeos C ON A.PKEbelista=C.PKEbelista 
	AND A.CodTactica=C.CodTactica AND A.CodProducto=C.CodProducto

	/*
	/** 1.1.6. Base Potencial **/
	--Se obtiene la Base Potencial para las últimas 6 campañas anteriores a la campaña de proceso. 
	--Para hallar la Venta Potencial Mínima U6C y la Frecuencia U6C GP
	--Se filtran solo las consultoras que hayan realizado como mínimo un pedido Web o Web Mixto y Activas en la campañana de proceso
	SELECT A.AnioCampana, a.PKEbelista,A.CodTactica, A.CodProducto, D.CodRegion, C.CodComportamientoRolling, 
	       SUM(RealVtaMNNeto) VentaPotencial
	INTO #BasePotencial_aux3
	FROM #FVTAPROEBECAMU6C A 
	--INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista
	INNER JOIN #FSTAEBECAM C ON A.PKEbelista=C.PKEbelista AND C.AnioCampana=@AnioCampanaProceso			-- Segmento de la campaña de proceso
	--INNER JOIN DGEOGRAFIACAMPANA D ON B.PKTerritorio=D.PKTerritorio AND D.AnioCampana=@AnioCampanaProceso	-- Región de la campaña de proceso
	INNER JOIN DWH_ANALITICO.dbo.DWH_DGEOGRAFIACAMPANA D (NOLOCK) ON A.PKTerritorio=D.PKTerritorio AND D.AnioCampana=@AnioCampanaProceso
	AND D.CodPais=@CodPais
	WHERE A.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso 
	AND C.FlagActiva=1 -- Activa en la Campaña de proceso																			
	GROUP BY A.AnioCampana, a.PKEbelista, A.CodTactica,A.CodProducto,D.CodRegion,C.CodComportamientoRolling,A.PKPedido
	HAVING SUM(RealVtaMNNeto)>0

	--Se filtran solo las consultoras que hayan realizado como mínimo un pedido Web o Web Mixto
	SELECT DISTINCT A.* 
	INTO #BasePotencial_auxU6C 
	FROM #BasePotencial_aux3 A INNER JOIN							
	#FSTAEBECAM E ON A.PKEbelista=E.PKEbelista AND E.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso 
	WHERE E.codigofacturaINTerneT IN ('WEB','WMX') 

	--Se halla la venta potencial mínima por consultora y la cantidad de pedidos puros a nivel de consultora
	SELECT CodTactica, CodProducto, CodRegion, CodComportamientoRolling,Pkebelista, 
	       MIN(VentaPotencial) VentaPotencialMin, 
	       COUNT(DISTINCT AnioCampana) PedidosPuros
	INTO #BasePotencial_aux2
	FROM #BasePotencial_auxU6C
	GROUP BY CodTactica, CodProducto, CodRegion, CodComportamientoRolling, Pkebelista

	--Se halla el promedio de la venta potencial mínima por consultora y el promedio de la cantidad de pedidos puros (Frecuencia U6C GP) a nivel de la Base Potencial
	SELECT CodTactica, CodProducto, CodRegion, CodComportamientoRolling, 
	       AVG(VentaPotencialMin) PromVentaPotencialMin, 
	       ROUND(SUM(PedidosPuros)*1.0/COUNT(PedidosPuros),1) Frecuencia 
	INTO #BasePotencialU6C
	FROM #BasePotencial_aux2 
	GROUP BY CodTactica, CodProducto, CodRegion, CodComportamientoRolling*/

	/** 1.1.7. Precio Óptimo **/
	--Obtengo los productos vendidos en las últimas 24 campañas
	SELECT AnioCampana, Aniocampanaref, A.PKEbelista,CodTactica, CodProducto, PKProducto,PKTipoOferta,CodVenta,RealVtaMNNeto, 
	RealVtaMNCatalogo,RealUUVendidas 
	INTO #PO_Venta
	FROM #FVTAPROEBECAMU24C A --INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista

	SELECT DISTINCT PKEbelista, CodTactica,CodProducto, PKProducto 
	INTO #PO_ProdXConsultora 
	FROM #PO_Venta

	--Obtengo la matriz de venta de los productos propuestos
	SELECT B.CodTactica,B.CodProducto,A.* 
	INTO #PO_Catalogo 
	--FROM DMATRIZCAMPANA A INNER JOIN #Productos_Individual B 
	FROM DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA A (NOLOCK) INNER JOIN #Productos_Individual B
	ON A.PKProducto=B.PKProducto 
	--INNER JOIN DTIPOOFERTA C ON A.PKTipoOferta=C.PKTipoOferta
	INNER JOIN DWH_ANALITICO.dbo.DWH_DTIPOOFERTA C (NOLOCK) ON A.PKTipoOferta=C.PKTipoOferta AND C.CodPais=@CodPais
	WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso 
	AND a.PrecioOferta>0 
	AND CodTipoProfit = '01' 
	AND C.CodTipoOferta NOT IN ('030','040','051')
	AND A.VehiculoVenta IN ('REVISTA','CATÁLOGO','CATALOGO')
	AND A.CodPais=@CodPais

	--Se arma la tabla de Precio Óptimo
	--Se cuentan las campañas en las que se muestra el producto a dicho precio y la consultora ha pasado pedido
	SELECT A.Pkebelista,A.CodTactica,A.CodProducto, PrecioOferta,0 PedidosPuros,CAST(0.0 AS FLOAT) Probabilidad, 
	       COUNT(DISTINCT B.AnioCampana) NumAspeos
	INTO #PrecioOptimo 
	FROM #PO_ProdXConsultora A INNER JOIN #PO_Catalogo B ON A.PKProducto=B.PKProducto 
	INNER JOIN #BaseEstadoConsultoras C ON A.PKEbelista=C.PKEbelista AND B.AnioCampana=C.AnioCampana
	WHERE C.FlagPasoPedido=1 
	GROUP BY A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta

	--Pedidos puros
	SELECT A.Pkebelista,A.CodTactica,A.CodProducto, PrecioOferta, 
	       COUNT(DISTINCT B.AnioCampana) PedidosPuros 
	INTO #PO_PedidosPuros 
	FROM #PO_Venta A 
	INNER JOIN #PO_Catalogo B ON A.AnioCampana=B.AnioCampana AND A.PKProducto = B.PKProducto
	AND A.PKTipoOferta=B.PKTipoOferta AND A.CodVenta=B.CodVenta
	GROUP BY A.Pkebelista,A.CodTactica,A.CodProducto, PrecioOferta

	UPDATE A 
	SET A.PedidosPuros = B.PedidosPuros,
	    A.Probabilidad = B.PedidosPuros * 1.00 / A.NumAspeos
	FROM #PrecioOptimo A INNER JOIN #PO_PedidosPuros B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica 
	AND A.CodProducto=B.CodProducto 
	AND A.PrecioOferta=B.PrecioOferta
	
	--Ordena por probabilidad y luego por número de pedido puro
	SELECT PKEbelista,CodTactica,CodProducto,PrecioOferta,NumAspeos,PedidosPuros,Probabilidad,
	ROW_NUMBER() OVER(PARTITION BY PKEbelista,CodTactica,CodProducto ORDER BY Probabilidad DESC,PedidosPuros DESC,PrecioOferta ASC) AS Posicion
	INTO #PrecioOptimoFinal 
	FROM #PrecioOptimo
	ORDER BY PKEbelista,CodTactica,CodProducto,PrecioOferta
	
	--Actualizo el precio óptimo
	UPDATE A
	SET A.PrecioOptimo = B.PrecioOferta
	FROM #VentaU24C A INNER JOIN #PrecioOptimoFinal B ON A.PKEbelista=B.PKEbelista AND Posicion = 1
	AND A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto

	SELECT CodTactica, CodProducto, 
	       MIN(PrecioOferta) PrecioMinimo 
	INTO #PO_PrecioMinimo 
	FROM #PrecioOptimo  
	GROUP BY CodTactica, CodProducto

	/*
	/** 1.1.8. Recencia y Frecuencia de U24C para GP **/

	SELECT A.AnioCampana, A.PKEbelista, A.CodTactica, A.CodProducto, D.CodRegion, C.CodComportamientoRolling 
	INTO #BasePotencial_aux24C  
	FROM #FVTAPROEBECAMU24C A 
	--INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista  
	INNER JOIN #FSTAEBECAM C ON A.PKEbelista=C.PKEbelista  AND C.AnioCampana=@AnioCampanaProceso
	--INNER JOIN DGEOGRAFIACAMPANA D ON B.PKTerritorio=D.PKTerritorio AND D.AnioCampana=@AnioCampanaProceso
	INNER JOIN DWH_ANALITICO.dbo.DWH_DGEOGRAFIACAMPANA D (NOLOCK) ON A.PKTerritorio=D.PKTerritorio AND D.AnioCampana=@AnioCampanaProceso
	AND D.CodPais=@CodPais
	WHERE A.AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso 
	AND C.FlagActiva=1 
	GROUP BY A.AnioCampana, a.PKEbelista,A.CodTactica,  A.CodProducto,D.CodRegion,C.CodComportamientoRolling,A.PKPedido
	HAVING SUM(REALVtaMNNeto)>0   

	SELECT DISTINCT A.* 
	INTO #BasePotencial_aux24C2 
	FROM #BasePotencial_aux24C A INNER JOIN #FSTAEBECAM E ON A.PKEbelista=E.PKEbelista  
	AND E.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso 
	WHERE E.codigofacturaINTerneT IN ('WEB','WMX')  

	--Se halla la cantidad de pedidos puros - frecuencia y el máximo de recencia a nivel de cada consultora
	SELECT CodTactica, CodProducto, CodRegion, CodComportamientoRolling,pkebelista, 
	       dbo.DiffANIOCampanas(@AnioCampanaProceso,MAX(AnioCampana)) Recencia,
	       COUNT(DISTINCT AnioCampana) PedidosPuros
	INTO #BasePotencial_aux24C3
	FROM #BasePotencial_aux24C2
	GROUP BY CodTactica, CodProducto, CodRegion, CodComportamientoRolling, pkebelista

	--Se halla la recencia U24C y frecuencia U24C a nivel de la base potencial
	SELECT CodTactica, CodProducto, CodRegion, CodComportamientoRolling, 0 CicloRecompraPotencial, CAST(0 AS FLOAT) PrecioOptimoGP,
	       AVG(CAST(Recencia AS FLOAT)) RecenciaGP, 
	       ROUND(SUM(PedidosPuros)*1.0/COUNT(PedidosPuros),1) FrecuenciaGP 
	INTO #BasePotencialU24C
	FROM #BasePotencial_aux24C3 
	GROUP BY CodTactica, CodProducto, CodRegion, CodComportamientoRolling

	--Cálculo de Ciclo de Recompra Potencial
	SELECT PKEbelista,CodTactica, CodProducto,CodRegion,CodComportamientoRolling, 
	       MAX(AnioCampana) AnioCampaniaMax,
	       (23-dbo.DiffANIOCampanas(MIN(AnioCampana),@AnioCampanaInicio24UC)-dbo.DiffANIOCampanas(@AnioCampanaProceso,
	       MAX(AnioCampana)))*1.0/(COUNT(AnioCampana)-1) CicloRecompraPotencial
	INTO #BaseCicloRecompra 
	FROM #BasePotencial_aux24C
	GROUP BY PKEbelista,CodTactica,CodProducto,CodRegion,CodComportamientoRolling
	HAVING COUNT(AnioCampana)>1

	--Actualizo Ciclo Recompra Potencial
	UPDATE A
	SET CicloRecompraPotencial = B.CicloRecompraPotencial,
	    FrecuenciaGP = CEILING(FrecuenciaGP)
	FROM #BasePotencialU24C A INNER JOIN (
	SELECT CodTactica,CodProducto,CodRegion,CodComportamientoRolling, CEILING(SUM(CicloRecompraPotencial)/COUNT(PKEbelista)) CicloRecompraPotencial 
	FROM #BaseCicloRecompra GROUP BY CodTactica,CodProducto,codregion,CodComportamientoRolling) B ON  A.CodTactica=B.CodTactica  AND
	A.CodProducto=B.CodProducto AND A.codregion=B.codregion AND A.CodComportamientoRolling=B.CodComportamientoRolling

	-- Calcular la nueva Recencia y Frecuencia de U24C para GP para la consultoras que no han comprado los productos
	SELECT PKEbelista, CodRegion, CodComportamientoRolling, CodTactica, CodProducto,CAST(0 AS FLOAT) PrecioOptimo,
	       COUNT(DISTINCT AnioCampana) PedidosPuros,
	       dbo.DiffANIOCampanas(@AnioCampanaProceso, MAX(AnioCampana)) Recencia 
	INTO #Tabla1 
	FROM #BasePotencial_aux24C2
	GROUP BY PKEbelista,CodRegion,CodComportamientoRolling,CodTactica,CodProducto

	--Se setean en 24 a los registros cuya recencia es 0
	UPDATE #Tabla1 
	SET Recencia = 24 
	WHERE Recencia = 0

	UPDATE A
	SET PrecioOptimo = B.PrecioOferta
	FROM #Tabla1 A INNER JOIN #PrecioOptimoFinal B ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto 
	AND A.CodTactica=B.CodTactica AND Posicion = 1

	SELECT CodRegion, CodComportamientoRolling, 
	       COUNT(DISTINCT PKEbelista) TotalConsultoras 
	INTO #Tabla2 
	FROM #InfoConsultora
	GROUP BY CodRegion, CodComportamientoRolling

	--Se suman los pedidos puros
	SELECT CodRegion, CodComportamientoRolling, CodTactica, CodProducto,
	       SUM(PedidosPuros) TotalPedidosPuros,
		   SUM(Recencia) TotalRecencia,
		   SUM(PrecioOptimo) TotalPrecioOptimo, 
		   0 TotalConsultoras,
	       COUNT(DISTINCT PKEbelista) TotalConsultorasConVenta 
    INTO #Tabla3
	FROM #Tabla1
	GROUP BY CodRegion, CodComportamientoRolling, CodTactica, CodProducto

	UPDATE A
	SET A.TotalConsultoras = B.TotalConsultoras
	FROM #Tabla3 A INNER JOIN #Tabla2 B ON A.CodComportamientoRolling=B.CodComportamientoRolling AND A.CodRegion=B.CodRegion

	--Se actualiza el nuevo valor de la Frecuencia, Recencia y Precio Óptimo U24C GP
	UPDATE A
	SET A.FrecuenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE ROUND((B.TotalPedidosPuros*1.00/TotalConsultoras),2) END,
	    /*A.RecenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE CEILING((B.TotalRecencia*1.00+(24*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras) END,*/
	    A.RecenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE (B.TotalRecencia*1.00+(24*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras END,
		A.PrecioOptimoGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE (B.TotalPrecioOptimo*1.00+(C.PrecioMinimo*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras END
	FROM #BasePotencialU24C A INNER JOIN #Tabla3 B ON A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
	AND A.CodProducto=B.CodProducto INNER JOIN #PO_PrecioMinimo C ON A.CodProducto=C.CodProducto AND A.CodTactica=B.CodTactica

	/** Revisar **/
	--Se halla la venta potencial mínima por Consultora a nivel de consultora
	SELECT PKEbelista,CodTactica, CodProducto, CodRegion, CodComportamientoRolling,
	       MIN(VentaPotencial) VentaPotencialMin 
	INTO #Tabla4
	FROM #BasePotencial_auxU6C
	GROUP BY PKEbelista,CodTactica,CodProducto, CodRegion, CodComportamientoRolling 

	--Se halla el promedio de la venta potencial mínima por Consultora a nivel de la Base Potencial
	SELECT CodRegion, CodComportamientoRolling,CodTactica, CodProducto, 0 TotalConsultoras,
	       SUM(VentaPotencialMin) VentaPotencialMin 
	INTO #Tabla5 
	FROM #Tabla4 
	GROUP by CodRegion, CodComportamientoRolling,CodTactica,CodProducto 

	--Se halla el total de consultoras por Región y Comportamiento
	SELECT CodRegion,CodComportamientoRolling,
	       COUNT(DISTINCT PKEbelista) TotalConsultotas  
	INTO #Tabla6 
	FROM #InfoConsultora
	GROUP BY CodRegion,CodComportamientoRolling

	UPDATE A
	SET A.TotalConsultoras = B.TotalConsultotas
	FROM #Tabla5 A INNER JOIN #Tabla6 B ON A.CodComportamientoRolling=B.CodComportamientoRolling AND A.CodRegion=B.CodRegion

	--Se actualiza el nuevo valor de la VentaMinima U6C
	UPDATE A
	SET A.PromVentaPotencialMin = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE ROUND((B.VentaPotencialMin*1.00/TotalConsultoras),2) END
	FROM #BasePotencialU6C A INNER JOIN #Tabla5 B ON A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
	AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica
	*/
	/** 1.1.9. Tabla final de Individual **/
	
	--Creación de la tabla de variables por producto y Consultora  
	SELECT DISTINCT A.*,B.TipoTactica,B.CodTactica,B.CodProducto,B.DesProducto, CAST(0.0 AS FLOAT) VentaAcumU6C, CAST(0.0 AS FLOAT) VentaAcumPU6C, 
	CAST(0.0 AS FLOAT) VentaAcumU6C_AA,CAST(0.0 AS FLOAT) VentaAcumU24C,CAST(0.0 AS FLOAT) VentaPromU24C,CAST(0.0 AS FLOAT) FrecuenciaU24C,CAST(0.0 AS FLOAT) RecenciaU24C,
	0 FrecuenciaU6C,CAST(0.0 AS FLOAT) CicloRecompraPotencial, CAST(0.0 AS FLOAT) BrechaRecompraPotencial, CAST(0.0 AS FLOAT) VentaPotencialMinU6C, 
	CAST(0.0 AS FLOAT) GAP, CAST(0.0 AS FLOAT) BrechaVenta,CAST(0.0 AS FLOAT) BrechaVenta_MC, 0 FlagCompra, CAST(0.0 AS FLOAT) PrecioOptimo,CAST(0.0 AS FLOAT) GAPPrecioOptimo,0 NumAspeos,
	CAST(0.0 AS FLOAT) Gatillador, CAST(0.0 AS FLOAT) FrecuenciaNor, CAST(0.0 AS FLOAT) RecenciaNor,CAST(0.0 AS FLOAT)BrechaVentaNor,  CAST(0.0 AS FLOAT)BrechaVenta_MCNor,  
	CAST(0.0 AS FLOAT) GAPPrecioOptimoNor, CAST(0.0 AS FLOAT) GatilladorNor, CAST(0.0 AS FLOAT)Oportunidad,CAST(0.0 AS FLOAT)OportunidadNor, 
	CAST(0.0 AS FLOAT) Score,0 UnidadesTactica, CAST(0.0 AS FLOAT) Score_UU,CAST(0.0 AS FLOAT) Score_MC_UU, 'A' PerfilOficial,FlagTop
	INTO #ListadoConsultora 
	FROM #InfoConsultora A, #ProductosCUC_Individual B  

	--Se actualiza la Venta Acumulada de las U6C  
	UPDATE A  
	SET A.VentaAcumU6C = B.VentaAcumulada
	FROM #ListadoConsultora A INNER JOIN #VentaU6C B  
	ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica 

	--Se actualiza la Venta Acumulada de las PU6C  
	UPDATE A  
	SET A.VentaAcumPU6C = B.VentaAcumulada
	FROM #ListadoConsultora A INNER JOIN #VentaPU6C B  
	ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica   

	--Se actualiza la Venta Acumulada de las U6C AA  
	UPDATE A 
	SET A.VentaAcumU6C_AA = B.VentaAcumulada
	FROM #ListadoConsultora A INNER JOIN #VentaU6C_AA B  
	ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica   

	--Se actualiza la información (VtaAcum, VtaProm, Gatillador, Recencia, Frecuencia y Precio Óptimo) de las U24C  
	UPDATE A  
	SET A.VentaAcumU24C = B.VentaAcumulada,
	    A.VentaPromU24C = B.VentaPromedio, 
		A.Gatillador = B.Gatillador,
	    RecenciaU24C = Recencia,
		FrecuenciaU24C = Frecuencia,
		FlagCompra = 1, 
		A.PrecioOptimo = B.PrecioOptimo
	FROM #ListadoConsultora A INNER JOIN #VentaU24C B  
	ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica  
	
	TRUNCATE TABLE ARP_ListadoVariablesIndividual
	INSERT INTO ARP_ListadoVariablesIndividual
	SELECT *
	FROM #ListadoConsultora 
	 
	/*
	--Se actualiza el Ciclo de Recompra Potencial (Se busca en la Base Potencial)
	UPDATE A  
	SET A.CicloRecompraPotencial = B.CicloRecompraPotencial
	FROM #ListadoConsultora A INNER JOIN #BasePotencialU24C B  
	ON  A.CodProducto=B.CodProducto AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
	AND A.CodTactica=B.CodTactica   

	--Se actualiza la Venta Potencial Mínima U6C (Se busca en la Base Potencial)
	UPDATE A  
	SET A.VentaPotencialMinU6C = B.PromVentaPotencialMin
	FROM #ListadoConsultora A INNER JOIN #BasePotencialU6C B  
	ON  A.CodProducto=B.CodProducto AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
	AND A.CodTactica=B.CodTactica   

	--Se actualiza la Frecuencia U24C, Recencia U24C y Precio Óptimo GP para las consultoras que no tuvieron venta (Se busca de la Base Potencial)
	UPDATE A  
	SET A.FrecuenciaU24C = B.FrecuenciaGP, 
	    RecenciaU24C = B.RecenciaGP, 
		A.PrecioOptimo = B.PrecioOptimoGP
	FROM #ListadoConsultora A INNER JOIN #BasePotencialU24C B  
	ON  A.CodProducto=B.CodProducto AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
	AND A.CodTactica=B.CodTactica   
	AND A.FlagCompra = 0 --No tuvieron compra

	--Se calculan las variables del Grupo Potencial: Gatillador
	SELECT CodRegion, CodComportamientoRolling,CodTactica,
	       AVG(Gatillador) GatilladorGP,
		   AVG(FrecuenciaU24C) FrecuenciaU24CGP 
	INTO #GrupoPotencial 
	FROM #ListadoConsultora 
	GROUP BY CodRegion, CodComportamientoRolling,CodTactica

	--Se actualiza el Gatillador para las consultoras que no tuvieron venta 
	UPDATE A  
	SET A.Gatillador = B.GatilladorGP
	FROM #ListadoConsultora A INNER JOIN #GrupoPotencial B  
	ON A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling AND A.CodTactica=B.CodTactica   
	AND A.FlagCompra = 0 --No tuvieron venta
	
	--Se actualiza la Brecha de Recompra Potencial = Recencia U24C – Ciclo Recompra Potencial
	UPDATE A  
	SET A.BrechaRecompraPotencial = A.RecenciaU24C - A.CicloRecompraPotencial
	FROM #ListadoConsultora A

	UPDATE A 
	SET GAPPrecioOptimo = ROUND((B.PrecioOferta)-A.PrecioOptimo,4)
	FROM #ListadoConsultora A INNER JOIN #ProductosCUC_Individual B ON A.CodProducto=B.CodProducto
	AND A.CodTactica=B.CodTactica */

	/** 1.1.10. Cálculo de Brechas - GAP Con Motor de Canibalizacion **/ 

	/*--Si Venta es mayor a cero en las U24C
	-- Condición 1  
	UPDATE #ListadoConsultora  
	SET BrechaVenta_MC = VentaAcumU24C  
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 AND BrechaRecompraPotencial>0

		-- Condición 2 --Comentar por Canibalización
		UPDATE #ListadoConsultora  
		SET BrechaVenta_MC = -1
		WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 AND BrechaRecompraPotencial<=0

		-- Condición 3 - Recompró
		--Actualizo el GAP
		UPDATE #ListadoConsultora  
		SET GAP = VentaAcumU6C - VentaAcumU6C_AA
		WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24

			-- Condición 4  
			UPDATE #ListadoConsultora  
			SET BrechaVenta_MC = VentaPromU24C
			WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24 AND GAP<=0		

			-- Condición 5  --Comentar por Canibalización
			UPDATE #ListadoConsultora  
			SET BrechaVenta_MC = -1
			WHERE  VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24 AND GAP>0

		-- Condición 6 
		-- Actualiza el GAP
		UPDATE #ListadoConsultora  
		SET GAP = VentaAcumU6C - VentaAcumPU6C
		WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24

			-- Condición 7  
			UPDATE #ListadoConsultora  
			SET BrechaVenta_MC = VentaPromU24C
			WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24 AND GAP<=0

			-- Condición 8 --Comentar por Canibalización
			UPDATE #ListadoConsultora  
			SET BrechaVenta_MC = -1
			WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24 AND GAP>0
	
	--Si Venta es menor o igual a cero en las U24C
	-- Condición 9
	UPDATE #ListadoConsultora  
	SET BrechaVenta_MC = VentaPotencialMinU6C
	WHERE VentaAcumU24C<=0 */

	/** 1.1.11. Cálculo de Brechas - GAP Sin Motor Canibalización **/ 
	/*
	--Si Venta es mayor a cero en las U24C
	-- Condición 1  
	UPDATE #ListadoConsultora  
	SET BrechaVenta = VentaAcumU24C  
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 

		-- Condición 2 - Recompró
		UPDATE #ListadoConsultora  
		SET GAP = VentaAcumU6C - VentaAcumU6C_AA, 
			BrechaVenta = VentaPromU24C
		WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24

		-- Condición 3  -- No figura en el documento
		UPDATE #ListadoConsultora  
		SET GAP = VentaAcumU6C - VentaAcumPU6C,
		    BrechaVenta = VentaPromU24C
		WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24

	UPDATE #ListadoConsultora  
	SET GAP = VentaAcumU6C - VentaAcumPU6C,
		BrechaVenta = VentaPromU24C
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1

	-- Condición 4  
	UPDATE #ListadoConsultora  
	SET BrechaVenta = VentaPotencialMinU6C
	WHERE VentaAcumU24C<=0
	*/
	/** 1.2. Táctica Bundle **/

	/** 1.2.0. Productos Bundle **/

	--A nivel de Item (PKProducto) 
	SELECT CodTactica,TipoTactica,B.CodCUC CodProducto,Unidades,B.DesProductoCUC DesProducto,
	A.PrecioOferta,CodMarca,PKProducto,DesMarca,DesCategoria,FlagTop 
	INTO #Productos_Bundle 
	FROM #ListadoProductos A 
	--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC AND TipoTactica='Bundle'
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND TipoTactica='Bundle' AND B.CodPais=@CodPais

	--A nivel CUC
	SELECT CodTactica,TipoTactica,CodProducto,Unidades,PrecioOferta,FlagTop, 
	       MAX(DesProducto) DesProducto
	INTO #ProductosCUC_Bundle
	FROM #Productos_Bundle
	WHERE DesProducto IS NOT NULL
	GROUP BY CodTactica,TipoTactica,CodProducto,Unidades,PrecioOferta,FlagTop

	/** 1.2.1. Información de las U24C (Últimas 24 campañas) **/  
	SELECT B.CodTactica, B.CodProducto,A.* 
	INTO #FVTAPROEBECAMU24C_Bundle   
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto   
	--WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso   

	--Recencia, Venta Acumulada U24C
	SELECT A.PKEbelista,A.CodTactica,A.CodProducto,
	dbo.DiffANIOCampanas(@AnioCampanaProceso,MAX(AnioCampana)) Recencia, 
	SUM(REALVtaMNNeto) VentaAcumulada, 
	CAST(0 AS FLOAT) PrecioOptimo,
	CAST(0 AS FLOAT) CicloRecompraPotencial
	INTO #VentaU24C_ProductoBundle  
	FROM #FVTAPROEBECAMU24C_Bundle A --INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista 
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0 

	/** 1.2.2. Información de las U6C (Últimas 6 campañas) **/ 
	/*SELECT B.CodTactica, B.CodProducto,A.* 
	INTO #FVTAPROEBECAM_Bundle   
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto  
	WHERE AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso   

	SELECT A.PKEbelista,A.CodTactica,A.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	--INTO #VentaU6C_Bundle  
	FROM #FVTAPROEBECAM_Bundle A INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista  
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0*/ 

	SELECT A.PKEbelista,B.CodTactica,B.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaU6C_Bundle  
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto  
	WHERE AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso    
	GROUP BY A.PKEbelista,B.CodTactica,B.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0 

	/** 1.2.3. Información de las U6C AA (Últimas 6 campañas del año anterior) **/
	/*SELECT B.CodTactica,B.CodProducto,A.* 
	INTO #FVTAPROEBECAM_AA_Bundle 
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto  
	WHERE AnioCampana BETWEEN @AnioCampanaInicio_AA AND @AnioCampanaProceso_AA  

	SELECT A.PKEbelista,A.CodTactica,A.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaU6C_AA_Bundle  
	FROM #FVTAPROEBECAM_AA_Bundle A INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista  
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0*/  

	SELECT A.PKEbelista,B.CodTactica,B.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaU6C_AA_Bundle  
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto  
	WHERE AnioCampana BETWEEN @AnioCampanaInicio_AA AND @AnioCampanaProceso_AA  
	GROUP BY A.PKEbelista,B.CodTactica,B.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0  

	/** 1.2.4. Información de las PU6C (Penúltimas 6 campañas) **/
	/*SELECT B.CodTactica,B.CodProducto,A.* 
	INTO #FVTAPROEBECAM_6PUC_Bundle  
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto  
	WHERE AnioCampana BETWEEN @AnioCampanaInicio6PUC AND @AnioCampanaFin6PUC   

	SELECT A.CodTactica,A.PKEbelista, A.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaPU6C_Bundle  
	FROM #FVTAPROEBECAM_6PUC_Bundle A INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista  
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0*/ 

	SELECT A.PKEbelista,B.CodTactica,B.CodProducto, 
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaPU6C_Bundle  
	FROM #TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto   
	WHERE AnioCampana BETWEEN @AnioCampanaInicio6PUC AND @AnioCampanaFin6PUC  
	GROUP BY A.PKEbelista,B.CodTactica,B.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0 

	/** 1.2.5. Ciclo Recompra Potencial **/
	SELECT A.PKEbelista,A.CodTactica,A.CodProducto,
	(23-dbo.DiffANIOCampanas(MIN(AnioCampana),@AnioCampanaInicio24UC)-dbo.DiffANIOCampanas(@AnioCampanaProceso,
	MAX(AnioCampana)))*1.0/(COUNT(AnioCampana)-1) CicloRecompraPotencial
	INTO #CicloRecompraPotencial
	FROM #FVTAPROEBECAMU24C_Bundle A --INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista   
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto   
	HAVING SUM(REALVtaMNNeto)>0 AND COUNT(AnioCampana)>1

	UPDATE A
	SET A.CicloRecompraPotencial = B.CicloRecompraPotencial
	FROM #VentaU24C_ProductoBundle A INNER JOIN #CicloRecompraPotencial B ON A.PKEbelista=B.PKEbelista
	AND A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto

	/** 1.2.6. Gatillador **/
	--Se guardan las campañas en las que se compraron las ofertas (Campañas en que se compraron todos los productos de la táctica)
	--Se cuentan los productos por táctica
	SELECT CodTactica,TipoTactica,
	       COUNT(CodProducto) TotalProductos 
	INTO #TotalProductosBundle 
	FROM #ProductosCUC_Bundle
	GROUP BY CodTactica,TipoTactica

	--Se guardan las campañas donde se compraron todos los productos de la táctica
	SELECT A.PKEbelista, A.CodTactica, A.Aniocampana,C.TotalProductos,  
	       SUM(A.REALVtaMNNeto) VentaAcumulada 
    INTO #FVTAPROEBECAMU24C_TacticaBundle  
	FROM #FVTAPROEBECAMU24C_Bundle A --INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista   
	INNER JOIN #TotalProductosBundle C ON A.CodTactica=C.CodTactica
	GROUP BY A.PKEbelista,A.CodTactica,A.Aniocampana,C.TotalProductos   
	HAVING SUM(REALVtaMNNeto)>0 AND COUNT(DISTINCT A.CodProducto)=C.TotalProductos

	SELECT PKEbelista,CodTactica,CAST(0 AS FLOAT) Gatillador,  
	       SUM(VentaAcumulada) VentaAcumulada, 
		   dbo.DiffANIOCampanas(@AnioCampanaProceso,MAX(AnioCampana)) Recencia, 
	       COUNT(DISTINCT AnioCampana) Frecuencia
	INTO #VentaU24C_TacticaBundle  
	FROM #FVTAPROEBECAMU24C_TacticaBundle   
	GROUP BY PKEbelista,CodTactica 

	SELECT PKEbelista, CodTactica, 
	       MIN(VentaAcumulada) VentaMinima 
	INTO #VentaPotencialMinima 
	FROM #FVTAPROEBECAMU24C_TacticaBundle
	WHERE AnioCampana>=@AnioCampanaInicio6UC
	GROUP BY PKEbelista, CodTactica 

	--Se calculan los pedidos para dos productos
	SELECT A.PKEbelista,A.CodTactica,A.CodProducto,A.PKProducto,A.AnioCampana,
	       SUM(REALVtaMNNeto) VentaAcumulada
	INTO #VentaU24C_Pedidos_Bundle1  
	FROM #FVTAPROEBECAMU24C_Bundle A INNER JOIN #ProductosCUC_Bundle B ON A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
	WHERE A.REALUUVendidas>=B.Unidades
	GROUP BY A.PKEbelista,A.CodTactica,A.CodProducto,A.PKProducto,A.AnioCampana   
	HAVING SUM(REALVtaMNNeto)>0 

	SELECT A.PKEbelista,A.CodTactica,A.Aniocampana,C.TotalProductos,   
	       COUNT(DISTINCT A.CodProducto) NumProducto
	INTO #VentaU24C_Pedidos_Bundle2
	FROM #VentaU24C_Pedidos_Bundle1 A INNER JOIN #TotalProductosBundle C ON A.CodTactica=C.CodTactica
	GROUP BY A.PKEbelista,A.CodTactica,A.Aniocampana,C.TotalProductos   
	HAVING COUNT(DISTINCT A.CodProducto)=C.TotalProductos

	SELECT PKEbelista, CodTactica, 
	       COUNT(Aniocampana) PedidosGatillador 
    INTO #Pedidos_Gatillador
	FROM #VentaU24C_Pedidos_Bundle2
	GROUP BY PKEbelista, CodTactica
	order by 1,2

	--Se calcula el número de campañas en la que se aspearon los productos juntos de cada táctica
	SELECT DISTINCT A.CodTactica,B.AnioCampana 
	INTO #AspeosU24C_Bundle 
	FROM #Productos_Bundle A 
	--INNER JOIN DMATRIZCAMPANA B ON A.PKProducto=B.PKProducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA B (NOLOCK) ON A.PKProducto=B.PKProducto AND B.CodPais=@CodPais
	INNER JOIN #TotalProductosBundle C ON A.CodTactica=C.CodTactica
	GROUP BY A.CodTactica,B.AnioCampana,C.TotalProductos
	HAVING COUNT(DISTINCT A.CodProducto)=C.TotalProductos

	SELECT A.PKEbelista,A.CodTactica,
	       COUNT(DISTINCT B.AnioCampana) CampaniasAspeadas 
	INTO  #Aspeos_Bundle
	FROM  #VentaU24C_TacticaBundle A INNER JOIN #AspeosU24C_Bundle B ON A.CodTactica=B.CodTactica
	INNER JOIN #BaseEstadoConsultoras C ON A.PKEbelista=C.PKEbelista AND B.AnioCampana=C.AnioCampana AND FlagPasoPedido=1
	GROUP BY A.PKEbelista,A.CodTactica

	UPDATE A
	SET Gatillador = B.PedidosGatillador*1.0 / CampaniasAspeadas
	FROM #VentaU24C_TacticaBundle A INNER JOIN #Aspeos_Bundle C ON A.PKEbelista=C.PKEbelista 
	AND A.CodTactica=C.CodTactica INNER JOIN #Pedidos_Gatillador B
	ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica

	/** 1.2.6. Precio Óptimo **/
	--Obtengo los productos vendidos en las últimas 24 campañas
	SELECT AnioCampana, Aniocampanaref, A.PKEbelista,CodTactica, CodProducto, PKProducto,PKTipoOferta,
	CodVenta,REALVtaMNNeto, REALVtaMNCatalogo,REALUUVendidas 
	INTO #PO_Venta_Bundle
	FROM #FVTAPROEBECAMU24C_Bundle A --INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista

	SELECT DISTINCT PKEbelista,CodTactica,CodProducto,PKProducto 
	INTO #PO_ProdXConsultora_Bundle 
	FROM #PO_Venta_Bundle

	--Obtengo la matriz de venta de los productos propuestos
	SELECT B.CodTactica,B.CodProducto,A.* 
	INTO #PO_Catalogo_Bundle 
	--FROM DMATRIZCAMPANA A INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto 
	FROM DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA A (NOLOCK) INNER JOIN #Productos_Bundle B ON A.PKProducto=B.PKProducto 
	--INNER JOIN DTIPOOFERTA C ON A.PKTipoOferta=C.PKTipoOferta
	INNER JOIN DWH_ANALITICO.dbo.DWH_DTIPOOFERTA C (NOLOCK) ON A.PKTipoOferta=C.PKTipoOferta AND C.CodPais=@CodPais
	WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso 
	AND a.PrecioOferta>0 
	AND CodTipoProfit = '01' 
	AND C.CodTipoOferta NOT IN ('030','040','051')
	AND A.CodPais=@CodPais

	--Se arma la tabla de Precio Óptimo
	--Se cuentan las campañas en las que se muestra el producto a dicho precio y la consultora ha pasado pedido

	CREATE NONCLUSTERED INDEX IDX_ProdxCon ON #PO_ProdXConsultora_Bundle (PKProducto)
	
	SELECT A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta,B.AnioCampana
	INTO #PO_ProdXConsultora_Bundle_2
	FROM #PO_ProdXConsultora_Bundle A INNER JOIN #PO_Catalogo_Bundle B ON A.PKProducto=B.PKProducto 

	CREATE NONCLUSTERED INDEX IDX_ProdxCon2 ON #PO_ProdXConsultora_Bundle_2 (PKEbelista,AnioCampana)
	CREATE NONCLUSTERED INDEX IDX_BaseEstCon ON #BaseEstadoConsultoras (PKEbelista,AnioCampana)

	SELECT A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta, 
	       0 PedidosPuros, CAST(0.0 AS FLOAT) Probabilidad,
		   COUNT(DISTINCT A.AnioCampana) NumAspeos
	INTO #PrecioOptimo_Bundle 
	FROM #PO_ProdXConsultora_Bundle_2 A  
	INNER JOIN #BaseEstadoConsultoras C ON A.PKEbelista=C.PKEbelista AND A.AnioCampana=C.AnioCampana
	WHERE C.FlagPasoPedido=1 
	GROUP BY A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta

	/*SELECT A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta, 
	       0 PedidosPuros, CAST(0.0 AS FLOAT) Probabilidad,
		   COUNT(DISTINCT B.AnioCampana) NumAspeos
	INTO #PrecioOptimo_Bundle 
	FROM #PO_ProdXConsultora_Bundle A INNER JOIN #PO_Catalogo_Bundle B ON A.PKProducto=B.PKProducto 
	INNER JOIN #BaseEstadoConsultoras C ON A.PKEbelista=C.PKEbelista AND B.AnioCampana=C.AnioCampana
	WHERE C.FlagPasoPedido=1 
	GROUP BY A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta*/

	SELECT A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta, 
	       COUNT(DISTINCT B.AnioCampana) PedidosPuros 
	INTO #PO_PedidosPuros_Bundle 
	FROM #PO_Venta_Bundle A 
	INNER JOIN #PO_Catalogo_Bundle B ON A.AnioCampana=B.AnioCampana AND A.PKProducto = B.PKProducto
	AND A.PKTipoOferta=B.PKTipoOferta AND A.CodVenta=B.CodVenta
	GROUP BY A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta

	UPDATE A 
	SET A.PedidosPuros = B.PedidosPuros,
	    Probabilidad = B.PedidosPuros*1.00 / A.NumAspeos
	FROM #PrecioOptimo_Bundle A INNER JOIN #PO_PedidosPuros_Bundle B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica 
	AND A.CodProducto=B.CodProducto AND A.PrecioOferta=B.PrecioOferta

	SELECT PKEbelista,CodTactica,CodProducto,PrecioOferta,
	       ROW_NUMBER()OVER(PARTITION BY PKEbelista,CodTactica,CodProducto ORDER BY Probabilidad DESC,PedidosPuros DESC,PrecioOferta ASC) AS Posicion 
	INTO #PrecioOptimoFinal_Bundle 
	FROM #PrecioOptimo_Bundle
	ORDER BY PKEbelista,CodTactica,CodProducto,PrecioOferta

	UPDATE A
	SET A.PrecioOptimo = B.PrecioOferta
	FROM #VentaU24C_ProductoBundle A INNER JOIN #PrecioOptimoFinal_Bundle B ON A.PKEbelista=B.PKEbelista AND Posicion = 1
	AND A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
	
	SELECT CodTactica, CodProducto, 
	       MIN(PrecioOferta) PrecioMinimo 
	INTO #PO_PrecioMinimo_Bundle 
	FROM #PrecioOptimo_Bundle  
	GROUP BY CodTactica, CodProducto

	/** 1.2.7. Tabla final de Bundle **/
	--Creación de la tabla de variables  por producto y Consultora  
	SELECT DISTINCT A.*,B.TipoTactica,B.CodTactica,B.CodProducto,B.DesProducto, CAST(0.0 AS FLOAT) VentaAcumU6C, CAST(0.0 AS FLOAT) VentaAcumPU6C, 
	CAST(0.0 AS FLOAT) VentaAcumU6C_AA,CAST(0.0 AS FLOAT) VentaAcumU24C,CAST(0.0 AS FLOAT) VentaPromU24C,CAST(0.0 AS FLOAT) FrecuenciaU24C, CAST(0.0 AS FLOAT) RecenciaU24C, 
	0 FrecuenciaU6C,CAST(0.0 AS FLOAT) CicloRecompraPotencial, CAST(0.0 AS FLOAT) BrechaRecompraPotencial, CAST(0.0 AS FLOAT) VentaPotencialMinU6C, 
	CAST(0.0 AS FLOAT) GAP,CAST(0.0 AS FLOAT) BrechaVenta,CAST(0.0 AS FLOAT) BrechaVenta_MC,0 FlagCompra, CAST(0.0 AS FLOAT) PrecioOptimo,CAST(0.0 AS FLOAT) GAPPrecioOptimo,0 NumAspeos, 
	CAST(0.0 AS FLOAT) Gatillador,CAST(0.0 AS FLOAT) FrecuenciaNor, CAST(0.0 AS FLOAT) RecenciaNor,CAST(0.0 AS FLOAT)BrechaVentaNor, CAST(0.0 AS FLOAT)BrechaVenta_MCNor, 
	CAST(0.0 AS FLOAT) GAPPrecioOptimoNor,CAST(0.0 AS FLOAT) GatilladorNor, CAST(0.0 AS FLOAT)Oportunidad,CAST(0.0 AS FLOAT)OportunidadNor, 
	CAST(0.0 AS FLOAT) Score,0 UnidadesTactica,  CAST(0.0 AS FLOAT) Score_UU,CAST(0.0 AS FLOAT) Score_MC_UU,'A' PerfilOficial,0 FlagBRP, 0 FlagVentaU6CMenosAA, 0 FlagVentaU6CMenosPP,
	FlagTop
	INTO #ListadoConsultora_Bundle 
	FROM #InfoConsultora A, #ProductosCUC_Bundle B  

	--Se actualiza la Venta Acumulada de las U6C  
	UPDATE A  
	SET A.VentaAcumU6C = B.VentaAcumulada
	FROM #ListadoConsultora_Bundle A INNER JOIN #VentaU6C_Bundle B  
	ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica   

	--Se actualiza la Venta Acumulada de las PU6C  
	UPDATE A  
	SET A.VentaAcumPU6C = B.VentaAcumulada
	FROM #ListadoConsultora_Bundle A INNER JOIN #VentaPU6C_Bundle B  
	ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica   

	--Se actualiza la Venta Acumulada de las U6C AA  
	UPDATE A 
	SET A.VentaAcumU6C_AA = B.VentaAcumulada
	FROM #ListadoConsultora_Bundle A INNER JOIN #VentaU6C_AA_Bundle B  
	ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica   

	--Se actualiza la información de las U24C  
	UPDATE  A  
	SET A.VentaAcumU24C = B.VentaAcumulada,
	    A.CicloRecompraPotencial = B.CicloRecompraPotencial, 
	    A.BrechaRecompraPotencial = Recencia - B.CicloRecompraPotencial, 
	    A.PrecioOptimo = B.PrecioOptimo
	FROM #ListadoConsultora_Bundle A INNER JOIN #VentaU24C_ProductoBundle B  
	ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica   

	--Se actualiza la Venta Potencial Mínima de las U6C
	UPDATE A  
	SET A.VentaPotencialMinU6C = B.VentaMinima
	FROM #ListadoConsultora_Bundle A INNER JOIN #VentaPotencialMinima B  
	ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica   

	/*--Se actualiza El ciclo de Recompra Potencial, se busca en la Base Potencial: BASE POTENCIAL BUNDLE
	SELECT A.CodRegion,A.CodComportamientoRolling,A.CodTactica,A.CodProducto, 
	       CEILING(SUM(CicloRecompraPotencial)/COUNT(PKEbelista)) CicloRecompraPotencial,
	       AVG(B.PrecioMinimo) PrecioMinimo
	INTO #BasePotencial_ProductoBundle
	FROM #ListadoConsultora_Bundle A INNER JOIN #PO_PrecioMinimo_Bundle B ON A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
	GROUP BY A.CodRegion,A.CodComportamientoRolling,A.CodTactica,A.CodProducto

	UPDATE A  
	SET A.CicloRecompraPotencial = B.CicloRecompraPotencial
	FROM #ListadoConsultora_Bundle A INNER JOIN #BasePotencial_ProductoBundle B ON
	A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
	AND A.CodTactica=B.CodTactica AND A.CodProducto=b.CodProducto */

	--Se actualiza la Venta Acumulada de las U24C a nivel de Táctica
	UPDATE A  
	SET A.VentaPromU24C = B.VentaAcumulada/Frecuencia, 
	    A.Gatillador = B.Gatillador,
	    RecenciaU24C = Recencia,
		FrecuenciaU24C = Frecuencia,
		FlagCompra = 1
	FROM #ListadoConsultora_Bundle A INNER JOIN #VentaU24C_TacticaBundle B  
	ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica  
	
	/*UPDATE A  
	SET A.PrecioOptimo = B.PrecioMinimo
	FROM #ListadoConsultora_Bundle A INNER JOIN #BasePotencial_ProductoBundle B ON
	A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
	AND A.CodTactica=B.CodTactica AND A.CodProducto=b.CodProducto 
	WHERE A.PrecioOptimo=0 */

	TRUNCATE TABLE ARP_ListadoVariablesBundle
	INSERT INTO ARP_ListadoVariablesBundle
	SELECT A.*, B.PrecioMinimo 
	FROM #ListadoConsultora_Bundle A INNER JOIN #PO_PrecioMinimo_Bundle B ON A.CodTactica=B.CodTactica AND A.CodProducto=B.CodProducto
	
	/** 1.2.8 Cálculo de Brechas - GAP con Motor de Canibalizacion **/ 
	/*
	UPDATE #ListadoConsultora_Bundle
	SET FlagBRP = 1
	WHERE BrechaRecompraPotencial > 0

	UPDATE #ListadoConsultora_Bundle
	SET FlagVentaU6CMenosAA  = 1
	WHERE (VentaAcumU6C-VentaAcumU6C_AA)<=0

	UPDATE #ListadoConsultora_Bundle
	SET FlagVentaU6CMenosPP = 1
	WHERE (VentaAcumU6C-VentaAcumPU6C)<=0

	SELECT PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica,CAST(0.0 AS FLOAT) GAPRegalo,
	       0 NumAspeos,CAST(0 AS FLOAT) GatilladorRegalo,CAST(0.0 AS FLOAT) FrecuenciaNor, CAST(0.0 AS FLOAT) RecenciaNor,
		   CAST(0.0 AS FLOAT) BrechaVentaNor, CAST(0.0 AS FLOAT) BrechaVenta_MCNor, CAST(0.0 AS FLOAT) GAPPrecioOptimoNor,
		   CAST(0.0 AS FLOAT) GatilladorNor, CAST(0.0 AS FLOAT) Oportunidad,CAST(0.0 AS FLOAT) Oportunidad_MC,
	       CAST(0.0 AS FLOAT) OportunidadNor,CAST(0.0 AS FLOAT) Oportunidad_MCNor, CAST(0.0 AS FLOAT) Score,CAST(0.0 AS FLOAT) Score_MC,
	       0 UnidadesTactica, CAST(0.0 AS FLOAT) Score_UU, CAST(0.0 AS FLOAT) Score_MC_UU,'A' PerfilOficial,0 FlagSeRecomienda,
		   SUM(VentaAcumU6C) VentaAcumU6C,
		   SUM(VentaAcumPU6C) VentaAcumPU6C,
		   SUM(VentaAcumU6C_AA) VentaAcumU6C_AA,
		   SUM(VentaAcumU24C) VentaAcumU24C, 
		   AVG(VentaPromU24C) VentaPromU24C,
		   AVG(FrecuenciaU24C) FrecuenciaU24C, 
		   AVG(RecenciaU24C) RecenciaU24C, 
		   AVG(RecenciaU24C) RecenciaU24C_GP,
	       AVG(CicloRecompraPotencial) CicloRecompraPotencial, 
		   SUM(BrechaRecompraPotencial) BrechaRecompraPotencial, 
	       AVG(VentaPotencialMinU6C) VentaPotencialMinU6C, 
		   AVG(GAP) GAP,
		   AVG(BrechaVenta) BrechaVenta,
		   AVG(BrechaVenta_MC) BrechaVenta_MC, 
		   AVG(FlagCompra) FlagCompra, 
	       SUM(PrecioOptimo) PrecioOptimo,
		   AVG(GAPPrecioOptimo) GAPPrecioOptimo,
		   SUM(FlagBRP) FlagBRP, 
	       SUM(FlagVentaU6CMenosAA) FlagVentaU6CMenosAA, 
		   SUM(FlagVentaU6CMenosPP) FlagVentaU6CMenosPP,
		   AVG(Gatillador) Gatillador,
		   MAX(FlagTop)FlagTop
	INTO #ListadoConsultora_TacticaBundle
	FROM #ListadoConsultora_Bundle 
	GROUP BY PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica

	/*-- Caso Contrario	
	UPDATE #ListadoConsultora_TacticaBundle  
	SET BrechaVenta_MC = -1 
	WHERE FlagCompra>=1*/

	--Si Venta es mayor a cero en las U24C
	--Si Frecuencia U24C=1
	--Si Brecha Recompra Potencial 
	-- Condición 1  
	/*UPDATE #ListadoConsultora_TacticaBundle 
	SET BrechaVenta_MC = VentaPromU24C  
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 AND FlagBRP>0 

	UPDATE #ListadoConsultora_TacticaBundle 
	SET BrechaVenta_MC = -1  
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 AND FlagBRP<=0

	-- Condición 2  
	UPDATE #ListadoConsultora_TacticaBundle  
	SET BrechaVenta_MC = VentaPromU24C
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24 AND FlagVentaU6CMenosAA=1

	UPDATE #ListadoConsultora_TacticaBundle  
	SET BrechaVenta_MC = -1
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24 AND FlagVentaU6CMenosAA=0

	-- Condición 3 
	UPDATE #ListadoConsultora_TacticaBundle  
	SET BrechaVenta_MC = VentaPromU24C
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24 AND FlagVentaU6CMenosPP=1 

	UPDATE #ListadoConsultora_TacticaBundle  
	SET BrechaVenta_MC = -1
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24 AND FlagVentaU6CMenosPP=0*/

	/** 1.2.9 Cálculo de Brechas - GAP Sin Motor de Canibalización **/ 
	
	UPDATE #ListadoConsultora_TacticaBundle  
	SET BrechaVenta = VentaPromU24C
	
	--Base Potencial de Bundle
	--Se actualiza el GAP Precio Óptimo a nivel de Táctica

	SELECT A.PKebelista,A.CodTactica,
	       SUM(B.PrecioOferta)-SUM(DISTINCT A.PrecioOptimo) GAPPrecioOptimo 
	INTO #PO_PrecioOptimo
	FROM #ListadoConsultora_TacticaBundle A INNER JOIN #ProductosCUC_Bundle B ON A.CodTactica=B.CodTactica 
	GROUP BY A.PKebelista,A.CodTactica

	UPDATE A
	SET GAPPrecioOptimo = B.GAPPrecioOptimo
	FROM #ListadoConsultora_TacticaBundle A INNER JOIN #PO_PrecioOptimo B 
	ON A.CodTactica=B.CodTactica AND A.PKEbelista=B.PKEbelista

	UPDATE #ListadoConsultora_TacticaBundle
	SET RecenciaU24C_GP = 24
	WHERE RecenciaU24C_GP = 0

	SELECT CodRegion,CodComportamientoRolling,CodTactica, 
	       SUM(VentaPotencialMinU6C)/COUNT(DISTINCT PKEbelista) VentaPotencialMinU6C_GP, 
	       SUM(FrecuenciaU24C)/COUNT(DISTINCT PKEbelista) FrecuenciaU24C_GP,
		   /*CAST(ROUND(AVG(RecenciaU24C_GP*1.00),0) AS INT) RecenciaU24C_GP,*/
		   AVG(RecenciaU24C_GP*1.00) RecenciaU24C_GP,
	       SUM(Gatillador)/COUNT(DISTINCT PKEbelista) Gatillador_GP
	INTO #BasePotencial_TacticaBundle
	FROM #ListadoConsultora_TacticaBundle
	GROUP BY CodRegion,CodComportamientoRolling,CodTactica

	SELECT CodRegion,CodComportamientoRolling,CodTactica, 
	       SUM(GAPPrecioOptimo)/COUNT(DISTINCT PKEbelista) GAPPrecioOptimo_GP
	INTO #BasePotencial_TacticaBundle_PO
	FROM #ListadoConsultora_TacticaBundle
	WHERE GAPPrecioOptimo!=0
	GROUP BY CodRegion,CodComportamientoRolling,CodTactica

	--Se actualiza la Brecha Venta a nivel de Táctica
	UPDATE A  
	SET A.BrechaVenta = B.VentaPotencialMinU6C_GP
	FROM #ListadoConsultora_TacticaBundle A INNER JOIN #BasePotencial_TacticaBundle B  
	ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
	AND A.BrechaVenta=0

	--Se actualiza la Brecha Venta con Motor de Canibalización a nivel de Táctica
	/*UPDATE A  
	SET A.BrechaVenta_MC = B.VentaPotencialMinU6C_GP
	FROM #ListadoConsultora_TacticaBundle A INNER JOIN #BasePotencial_TacticaBundle B  
	ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
	AND A.BrechaVenta_MC=0*/

	--Se actualiza la FrecuenciaU24C GP Base Potencial a nivel de Táctica
	UPDATE A  
	SET A.FrecuenciaU24C = B.FrecuenciaU24C_GP
	FROM #ListadoConsultora_TacticaBundle A INNER JOIN #BasePotencial_TacticaBundle B  
	ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
	AND A.FrecuenciaU24C=0

	--Se actualiza la RecenciaU24C GP Base Potencial a nivel de Táctica
	UPDATE A  
	SET A.RecenciaU24C_GP = B.RecenciaU24C_GP
	FROM #ListadoConsultora_TacticaBundle A INNER JOIN #BasePotencial_TacticaBundle B 
	ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
	AND A.RecenciaU24C=0

	--Se actualiza el Gatillador GP Base Potencial a nivel de Táctica
	UPDATE A  
	SET A.Gatillador = B.Gatillador_GP
	FROM #ListadoConsultora_TacticaBundle A INNER JOIN #BasePotencial_TacticaBundle B  
	ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
	AND A.Gatillador=0

	--Se actualiza el GAP Precio Óptimo a nivel de Táctica
	UPDATE A  
	SET A.GAPPrecioOptimo = B.GAPPrecioOptimo_GP
	FROM #ListadoConsultora_TacticaBundle A INNER JOIN #BasePotencial_TacticaBundle_PO B  
	ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
	AND A.PrecioOptimo=0

	UPDATE #ListadoConsultora_TacticaBundle
	SET RecenciaU24C = RecenciaU24C_GP

	/** 1.3. Táctica Individual y Bundle **/

	/** 1.3.1. Tabla consolidada **/

	CREATE TABLE #ListadoConsultora_Total
	(PKEbelista INT, CodEbelista VARCHAR(15),
	CodRegion VARCHAR(3),CodComportamientoRolling TINYINT,
	Antiguedad INT,
	TipoTactica VARCHAR(30),CodTactica VARCHAR(4),
	VentaAcumU6C REAL,VentaAcumPU6C REAL,
	VentaAcumU6C_AA REAL,VentaAcumU24C REAL, 
	VentaPromU24C REAL,FrecuenciaU24C REAL, 
	RecenciaU24C REAL, CicloRecompraPotencial REAL,
	BrechaRecompraPotencial REAL, VentaPotencialMinU6C REAL,
	GAP REAL,BrechaVenta REAL,
	BrechaVenta_MC REAL, FlagCompra INT, 
	PrecioOptimo REAL, GAPPrecioOptimo REAL,
	GAPRegalo REAL, NumAspeos INT,
	Gatillador REAL, GatilladorRegalo REAL,
	FrecuenciaNor REAL,  RecenciaNor REAL,
	BrechaVentaNor REAL, BrechaVenta_MCNor REAL, 
	GAPPrecioOptimoNor REAL,GatilladorNor REAL,
	Oportunidad REAL,Oportunidad_MC REAL,
	OportunidadNor REAL,Oportunidad_MCNor REAL,
	Score REAL,Score_MC REAL,
	UnidadesTactica INT, Score_UU REAL, 
	Score_MC_UU REAL,
	PerfilOficial VARCHAR(1),
	FlagSeRecomienda INT,
	FlagTop INT
	)

	--Inserto las tácticas Bundle
	INSERT INTO #ListadoConsultora_Total 
	SELECT PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica,
	VentaAcumU6C,VentaAcumPU6C,VentaAcumU6C_AA,VentaAcumU24C,VentaPromU24C,FrecuenciaU24C,RecenciaU24C,CicloRecompraPotencial,
	BrechaRecompraPotencial,VentaPotencialMinU6C,GAP,BrechaVenta,BrechaVenta_MC,FlagCompra,PrecioOptimo,GAPPrecioOptimo,
	GAPRegalo,NumAspeos,Gatillador,GatilladorRegalo,FrecuenciaNor,RecenciaNor,BrechaVentaNor, BrechaVenta_MCNor, 
	GAPPrecioOptimoNor,GatilladorNor,Oportunidad,Oportunidad_MC,OportunidadNor,Oportunidad_MCNor,
	Score,Score_MC,UnidadesTactica, Score_UU, Score_MC_UU,PerfilOficial,FlagSeRecomienda,FlagTop
	FROM #ListadoConsultora_TacticaBundle

	--Inserto las tácticas Individual
	INSERT INTO #ListadoConsultora_Total
	SELECT PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica,
	       SUM(VentaAcumU6C),
		   SUM(VentaAcumPU6C),
		   SUM(VentaAcumU6C_AA),
		   SUM(VentaAcumU24C),
	       AVG(VentaPromU24C),
		   AVG(FrecuenciaU24C), 
		   AVG(RecenciaU24C), 
		   AVG(CicloRecompraPotencial), 
	       SUM(BrechaRecompraPotencial), 
		   AVG(VentaPotencialMinU6C), 
		   AVG(GAP) GAP,
		   AVG(BrechaVenta),
	       AVG(BrechaVenta_MC), 
		   AVG(FlagCompra), 
		   SUM(PrecioOptimo),
		   AVG(GAPPrecioOptimo),0,0,
		   AVG(Gatillador),
	       0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'A',0,
		   MAX(FlagTop) FlagTop
	FROM #ListadoConsultora
	GROUP BY PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica

	/** 1.3.2. Regalos **/
	--El ARP tiene la funcionalidad de incluir los regalos en las variables GAP Precio:
	--GAP Precio = GAP Precio - (Suma(Precio Normal de los Regalos)/Suma(Unidades de la oferta))

	--Obtengo el mayor precio normal de cada producto SAP
	SELECT B.CodTactica,CodProducto,b.pkProducto,B.Unidades, 
	       MAX(PrecioNormalMN) PrecioNormalMN 
	INTO #TemporalRegalo1
	--FROM DMATRIZCAMPANA A INNER JOIN #ProductosRegalo B ON A.PKProducto=B.PKProducto 
	FROM DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA A (NOLOCK) INNER JOIN #ProductosRegalo B ON A.PKProducto=B.PKProducto 
	--INNER JOIN DTIPOOFERTA C ON A.PKTipoOferta=C.PKTipoOferta 
	INNER JOIN DWH_ANALITICO.dbo.DWH_DTIPOOFERTA C (NOLOCK) ON A.PKTipoOferta=C.PKTipoOferta AND C.CodPais=@CodPais
	WHERE A.PrecioNormalMN>0
	AND A.CodPais=@CodPais
	GROUP BY B.CodTactica,CodProducto,b.pkProducto,B.Unidades

	--Se multiplican las unidades por el precio unitario de cada Producto CUC 
	SELECT B.CodTactica,CodProducto,B.Unidades,
	       ROUND(AVG(PrecioNormalMN),2) AS PrecioOferta,
		   B.Unidades*ROUND(AVG(PrecioNormalMN),2) AS PrecioOfertaTotal 
	INTO #TemporalRegalo2 
	FROM #TemporalRegalo1 B 
	GROUP BY B.CodTactica,CodProducto,B.Unidades

	--Se obtiene el precio unitario por táctica promediando los Precios por producto CUC
	SELECT A.CodTactica, 
	       SUM(PrecioOfertaTotal)/SUM(B.TotalUnidades) GAPRegalo 
	INTO #TemporalRegalo 
	FROM #TemporalRegalo2
	A INNER JOIN #UnidadesTactica B ON A.CodTactica=B.CodTactica
	GROUP BY A.CodTactica

	--Se obtiene el total de pedidos con regalo por cada consultora de la base
	SELECT A.PKEbelista, 
	       COUNT(DISTINCT AnioCampana) AS TotalPedidos 
	INTO #FVTAPROEBECAM_REGALOS   
	--FROM FVTAPROEBECAMC01_VIEW A 
	FROM DWH_ANALITICO.dbo.DWH_FVTAPROEBECAM A (NOLOCK)
	--INNER JOIN DTIPOOFERTA C ON C.PKTipoOferta=A.PKTipoOferta 
	INNER JOIN DWH_ANALITICO.dbo.DWH_DTIPOOFERTA C (NOLOCK) ON C.PKTipoOferta=A.PKTipoOferta AND C.CodPais=@CodPais
	INNER JOIN #BaseConsultoras D ON A.PKEbelista=D.PKEbelista
	WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso   
	AND AnioCampana=AnioCampanaRef 
	AND C.CodTipoProfit='01' 
	AND A.RealVtaMNNeto=0 
	AND A.RealUUVendidas>=1
	AND A.CodPais=@CodPais
	GROUP BY A.PKEbelista

	SELECT PKEbelista, 
	       COUNT(DISTINCT AnioCampana) TotalCampanias 
	INTO #TotalPedidosConultora 
	FROM #BaseEstadoConsultoras
    WHERE FlagPasoPedido=1 
	GROUP BY PKEbelista

	--Se almacena el logaritmo log (#Pedidos con regalos/#Campañas con Pedidos) 
	SELECT A.PKEbelista,  (A.TotalPedidos*1.0/B.TotalCampanias) Gatillador 
	INTO #GatilladorRegalos
	FROM #FVTAPROEBECAM_REGALOS A INNER JOIN #TotalPedidosConultora B ON A.PKEbelista=B.PKEbelista

	--Se actualiza el GAP Regalos y el Gatillador Regalos en la tabla final
	SELECT DISTINCT Codtactica 
	INTO #TacticasConRegalo 
	FROM #ProductosRegalo

	UPDATE A
	SET A.GAPRegalo = B.GAPRegalo
	FROM #ListadoConsultora_Total A INNER JOIN #TemporalRegalo B ON A.CodTactica=B.CodTactica

	UPDATE A
	SET A.GatilladorRegalo = SQRT(B.Gatillador)/10
	FROM #ListadoConsultora_Total A INNER JOIN #GatilladorRegalos B ON A.PKEbelista=B.PKEbelista
	INNER JOIN #TacticasConRegalo C ON A.CodTactica=C.CodTactica

	UPDATE #ListadoConsultora_Total
	SET GAPPrecioOptimo = GAPPrecioOptimo - GAPRegalo, 
	    Gatillador = Gatillador + GatilladorRegalo

	UPDATE A  
	SET A.UnidadesTactica = B.TotalUnidades
	FROM #ListadoConsultora_Total A INNER JOIN #UnidadesTactica B ON A.CodTactica=B.CodTactica
	*/
	/** 1.3.3. Normalización de variables **/
	/*
	--Considerar 4 Decimales
	UPDATE #ListadoConsultora_Total  
	SET BrechaVenta = ROUND(BrechaVenta,4),
	    PrecioOptimo = ROUND(PrecioOptimo,4)
		--BrechaVenta_MC = ROUND(BrechaVenta_MC,4)

	--Se obtienen los promedios y desviaciones
	SELECT 
	ROUND(SUM(FrecuenciaU24C)*1.0/COUNT(PKEbelista),4)	PromFrecuenciaU24C, 
	ROUND(STDEVP(FrecuenciaU24C),4)						DSFrecuenciaU24C,
	ROUND(SUM(RecenciaU24C)*1.0/COUNT(PKEbelista),4)	PromRecenciaU24C, 
	ROUND(STDEVP(RecenciaU24C),4)						DSRecenciaU24C,
	ROUND(SUM(BrechaVenta)*1.0/COUNT(PKEbelista),4)		PromBrechaVenta, 
	ROUND(STDEVP(BrechaVenta),4)						DSBrechaVenta,
	--ROUND(SUM(BrechaVenta_MC)*1.0/COUNT(PKEbelista),4)	PromBrechaVenta_MC, 
	--ROUND(STDEVP(BrechaVenta_MC),4)						DSBrechaVenta_MC,
	ROUND(SUM(Gatillador)*1.0/COUNT(PKEbelista),4)		PromGatillador, 
	ROUND(STDEVP(Gatillador),4)							DSGatillador,
	ROUND(SUM(GAPPrecioOptimo)*1.0/COUNT(PKEbelista),4) PromGAPPrecioOptimo, 
	ROUND(STDEVP(GAPPrecioOptimo),4)					DSGAPPrecioOptimo
	INTO #ListadoPromDSTotal
	FROM #ListadoConsultora_Total
	WHERE BrechaVenta >= 0 --Se consideran únicamente los registros cuya BrechaVenta > 0

	--Se calculan los datos normalizados
	UPDATE A
	SET FrecuenciaNor = CASE (B.DSFrecuenciaU24C) WHEN 0 THEN 0 ELSE ROUND((A.FrecuenciaU24C-B.PromFrecuenciaU24C)/B.DSFrecuenciaU24C,4) END,
		RecenciaNor = CASE (B.DSRecenciaU24C) WHEN 0 THEN 0 ELSE ROUND((A.RecenciaU24C-B.PromRecenciaU24C)/B.DSRecenciaU24C,4) END,
		BrechaVentaNor = CASE (B.DSBrechaVenta) WHEN 0 THEN 0 ELSE ROUND((A.BrechaVenta-B.PromBrechaVenta)/B.DSBrechaVenta,4) END,
		--BrechaVenta_MCNor = CASE (B.DSBrechaVenta_MC) WHEN 0 THEN 0 ELSE ROUND((A.BrechaVenta_MC-B.PromBrechaVenta_MC)/B.DSBrechaVenta_MC,4) END,
		GAPPrecioOptimoNor = CASE (B.DSGAPPrecioOptimo) WHEN 0 THEN 0 ELSE ROUND((A.GAPPrecioOptimo-B.PromGAPPrecioOptimo)/B.DSGAPPrecioOptimo,4) END,
		GatilladorNor = CASE (B.DSGatillador) WHEN 0 THEN 0 ELSE ROUND((A.Gatillador-B.PromGatillador)/B.DSGatillador,4) END
	FROM #ListadoConsultora_Total A, #ListadoPromDSTotal B
	WHERE A.BrechaVenta >= 0 
	--AND A.VentaAcumU24C > 0

	/** 1.3.4. Cálculo del Score **/

	---Se calcula de la Oportunidad
	UPDATE #ListadoConsultora_Total  
	SET Oportunidad = ROUND((FrecuenciaNor - RecenciaNor + BrechaVentaNor - GAPPrecioOptimoNor + GatilladorNor),4)
	    --Oportunidad_MC = ROUND((FrecuenciaNor - RecenciaNor + BrechaVenta_MCNor - GAPPrecioOptimoNor + GatilladorNor),4)

	--Se calcula el Promedio y Desviación Estándar de la Oportunidad
	SELECT ROUND(SUM(Oportunidad)*1.0/COUNT(PKEbelista),4) PromOportunidad, 
	       ROUND(STDEVP(Oportunidad),4) DSOportunidad
	INTO #ListadoPromDSTotalOportunidad
	FROM #ListadoConsultora_Total
	WHERE BrechaVenta>=0

	/*SELECT ROUND(SUM(Oportunidad_MC)*1.0/COUNT(PKEbelista),4) PromOportunidad_MC, 
	       ROUND(STDEVP(Oportunidad_MC),4) DSOportunidad_MC
	INTO #ListadoPromDSTotalOportunidad_MC
	FROM #ListadoConsultora_Total
	WHERE BrechaVenta_MC>=0*/

	--Se normaliza la variable Oportunidad
	UPDATE A
	SET OportunidadNor = CASE(B.DSOportunidad) WHEN 0 THEN 0 ELSE ROUND((A.Oportunidad-B.PromOportunidad)/B.DSOportunidad,4) END
	FROM #ListadoConsultora_Total A, #ListadoPromDSTotalOportunidad B
	WHERE A.BrechaVenta>=0

	/*UPDATE A
	SET Oportunidad_MCNor = CASE(B.DSOportunidad_MC) WHEN 0 THEN 0 ELSE ROUND((A.Oportunidad_MC-B.PromOportunidad_MC)/B.DSOportunidad_MC,4) END
	FROM #ListadoConsultora_Total A, #ListadoPromDSTotalOportunidad_MC B
	WHERE A.BrechaVenta_MC>=0*/

	--Se calcula el Score en base a la Oportunidad
	UPDATE #ListadoConsultora_Total  
	SET Score = (EXP(OportunidadNor))/(1 + EXP(OportunidadNor))
	WHERE BrechaVenta>=0

	/*UPDATE #ListadoConsultora_Total  
	SET Score_MC = (EXP(Oportunidad_MCNor))/(1 + EXP(Oportunidad_MCNor))
	WHERE BrechaVenta_MC>=0*/

	--Se calcula el Score_UU en base a la Oportunidad multiplicada por el número de unidades
	UPDATE #ListadoConsultora_Total  
	SET Score_UU = CASE WHEN (OportunidadNor*UnidadesTactica)>700 THEN ((EXP(700))/(1+ EXP(700)))
	ELSE (EXP(OportunidadNor*UnidadesTactica))/(1+ EXP(OportunidadNor*UnidadesTactica)) END
	WHERE BrechaVenta>=0

	/*UPDATE #ListadoConsultora_Total  
	SET Score_MC_UU = CASE WHEN (Oportunidad_MCNor*UnidadesTactica)>700 THEN ((EXP(700))/(1+ EXP(700)))
	ELSE (EXP(Oportunidad_MCNor*UnidadesTactica))/(1+ EXP(Oportunidad_MCNor*UnidadesTactica)) END
	WHERE BrechaVenta_MC>=0*/
	*/
	/** Carga tabla ListadoVariablesRFM **/
	/*
	DELETE 
	--FROM BDDM01.[DATAMARTANALITICO].DBO.ARP_ListadoVariablesRFM
	FROM BD_ANALITICO.dbo.ARP_ListadoVariablesRFM
	WHERE AnioCampanaProceso = @AnioCampanaProceso 
	AND AnioCampanaExpo = @AnioCampanaExpo
	AND CodPais = @CodPais 
	AND TipoARP = @TipoARP
	AND TipoPersonalizacion = @TipoPersonalizacion
	AND Perfil = @Perfil

	--INSERT INTO BDDM01.[DATAMARTANALITICO].DBO.ARP_ListadoVariablesRFM
	INSERT INTO BD_ANALITICO.dbo.ARP_ListadoVariablesRFM
	(CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoARP,TipoPersonalizacion,PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,
	 Antiguedad,TipoTactica,CodTactica,VentaAcumU6C,VentaAcumPU6C,VentaAcumU6C_AA,VentaAcumU24C, VentaPromU24C,FrecuenciaU24C, 
	 RecenciaU24C, CicloRecompraPotencial,BrechaRecompraPotencial,VentaPotencialMinU6C,GAP,BrechaVenta,BrechaVenta_MC, 
     FlagCompra,PrecioOptimo,GAPPrecioOptimo,GAPRegalo,NumAspeos,Gatillador,GatilladorRegalo,FrecuenciaNor,RecenciaNor,
	 BrechaVentaNor, BrechaVenta_MCNor, GAPPrecioOptimoNor,GatilladorNor,Oportunidad,Oportunidad_MC,OportunidadNor,Oportunidad_MCNor,
	 Score,Score_MC,UnidadesTactica,Score_UU,Score_MC_UU,PerfilOficial,FlagSeRecomienda,FlagTop,Perfil)
	SELECT @CodPais AS CodPais, @AnioCampanaProceso AS AnioCampanaProceso, @AnioCampanaExpo AS AnioCampanaExpo,
	       @TipoARP AS TipoARP, @TipoPersonalizacion AS TipoPersonalizacion,PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,
	       Antiguedad,TipoTactica,CodTactica,VentaAcumU6C,VentaAcumPU6C,VentaAcumU6C_AA,VentaAcumU24C, VentaPromU24C,FrecuenciaU24C, 
	       RecenciaU24C, CicloRecompraPotencial,BrechaRecompraPotencial,VentaPotencialMinU6C,GAP,BrechaVenta,BrechaVenta_MC, 
		   FlagCompra,PrecioOptimo,GAPPrecioOptimo,GAPRegalo,NumAspeos,Gatillador,GatilladorRegalo,FrecuenciaNor,RecenciaNor,
	       BrechaVentaNor, BrechaVenta_MCNor, GAPPrecioOptimoNor,GatilladorNor,Oportunidad,Oportunidad_MC,OportunidadNor,
		   Oportunidad_MCNor,Score,Score_MC,UnidadesTactica,Score_UU,Score_MC_UU,PerfilOficial,FlagSeRecomienda,FlagTop,@Perfil AS Perfil
	FROM #ListadoConsultora_Total
	*/
END
/** 2. RFM para Nuevas **/
ELSE
BEGIN 

	--Se obtienen las consultoras que hayan comprado los productos listados en las últimas 18 campañas 
	--y que estos hayan sido aspeados
	SELECT DISTINCT A.AnioCampana,PKEBELISTA, B.CodTactica, B.CodProducto 
	INTO #FVTAPROEBECAM_NUEVAS
	--FROM FVTAPROEBECAMC01 A INNER JOIN #Productos B ON A.PKProducto=B.PKProducto
	FROM DWH_ANALITICO.dbo.DWH_FVTAPROEBECAM A (NOLOCK) INNER JOIN #Productos B ON A.PKProducto=B.PKProducto
	--INNER JOIN DMATRIZCAMPANA C ON A.AnioCampana=C.AnioCampana AND A.PKProducto=C.PKProducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA C (NOLOCK) ON A.AnioCampana=C.AnioCampana AND A.PKProducto=C.PKProducto AND C.CodPais=@CodPais
	--INNER JOIN DTIPOOFERTA D ON A.PKTipoOferta=C.PKTipoOferta
	INNER JOIN DWH_ANALITICO.dbo.DWH_DTIPOOFERTA D (NOLOCK) ON A.PKTipoOferta=C.PKTipoOferta AND D.CodPais=@CodPais
	WHERE A.AnioCampana BETWEEN @AnioCampanaProceso_Menos17 AND @AnioCampanaProceso 
	AND A.AnioCampana=A.AnioCampanaRef  
	AND D.CodTipoProfit='01'  
	AND D.CodTipoOferta NOT IN ('030','040','051')
	AND A.RealVtaMNNeto>0
	AND A.CodPais=@CodPais

	--Se Actualiza el código SAP desde la Matriz de Facturación
	UPDATE B
	SET CodSAP=C.CodSAP
	--FROM DMATRIZCAMPANA A 
	FROM DWH_ANALITICO.dbo.DWH_DMATRIZCAMPANA A (NOLOCK)
	INNER JOIN #ProductosCUC B ON A.CodVenta=B.CodVenta AND AnioCampana=@AnioCampanaExpo
	--INNER JOIN DPRODUCTO C ON A.PKProducto=C.PKProducto
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO C (NOLOCK) ON A.PKProducto=C.PKProducto AND C.CodPais=@CodPais
	WHERE AnioCampana=@AnioCampanaExpo
	AND A.CodPais=@CodPais

	/** 2.1. Cálculo de variables **/

	/** 2.1.1. Penetración Constantes y Penetración Inconstantes **/

	--Se busca el comportamiento de las consultoras que ingresaron en las últimas 18 campañas
	SELECT A.PKebelista,A.AnioCampana, CodComportamientoRolling,A.DescripcionRolling AS DesNivelComportamiento,AnioCampanaIngreso 
	INTO #TemporalCI
	--FROM FSTAEBECAMC01_VIEW A 
	FROM DWH_ANALITICO.dbo.DWH_FSTAEBECAM A (NOLOCK)
	--INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista
	INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA B (NOLOCK) ON A.PKEbelista=B.PKEbelista AND B.CodPais=@CodPais
	--INNER JOIN DCOMPORTAMIENTOROLLING D ON A.CodComportamientoRolling=D.CodComportamiento
	WHERE AnioCampanaIngreso BETWEEN @AnioCampanaProceso_Menos17 AND @AnioCampanaProceso 
	AND A.CodPais=@CodPais

	--Se halla la última campaña en la que la consultora fue Nueva 
	SELECT PKEbelista,
	       MAX(AnioCampana) AnioCampana,
		   dbo.CalculaAnioCampana(MAX(AnioCampana),1) AnioCampanaMasUno
	INTO #AnioCampanaNuevas 
	FROM #TemporalCI 
	WHERE DesNivelComportamiento = 'Nuevas'
	GROUP BY PKEbelista

	--Se guardan las consultoras que en la campaña consecutiva han sido Constantes 1 o 2
	SELECT DISTINCT A.PKEbelista,B.AnioCampana 
	INTO #ConsultorasNuevasCtes 
	FROM #AnioCampanaNuevas A INNER JOIN #TemporalCI B ON A.PKEbelista=B.PKEbelista
	AND A.AnioCampanaMasUno=B.AnioCampana
	AND DesNivelComportamiento IN ('Constantes 1','Constantes 2')

	--Se guardan las consultoras que en la campaña consecutiva han sido Inconstantes
	SELECT DISTINCT A.PKEbelista,B.AnioCampana 
	INTO #ConsultorasNuevasInCtes 
	FROM #AnioCampanaNuevas A INNER JOIN #TemporalCI B ON A.PKEbelista=B.PKEbelista
	AND A.AnioCampanaMasUno=B.AnioCampana
	AND DesNivelComportamiento IN ('Inconstantes')

	--Se guardan los totales para utilizarlos en la penetración
	SELECT @TotalConsultorasCtes = COUNT(DISTINCT PKEbelista) FROM #ConsultorasNuevasCtes
	SELECT @TotalConsultorasInCtes = COUNT(DISTINCT PKEbelista) FROM #ConsultorasNuevasInCtes

	--Se guarda la Penetración de las Constantes a nivel de cada táctica, producto
	SELECT B.CodTactica,B.CodProducto, 
	       COUNT(DISTINCT A.PKEbelista)*1.00/@TotalConsultorasCtes AS Porcentaje 
	INTO #PenetracionCtes 
	FROM #ConsultorasNuevasCtes A INNER JOIN #FVTAPROEBECAM_NUEVAS B ON A.PKEbelista=B.PKEbelista 
	WHERE B.AnioCampana BETWEEN A.AnioCampana AND dbo.CalculaAnioCampana(A.AnioCampana, 2)
	GROUP BY B.CodTactica,B.CodProducto

	--Se guarda la Penetración de las Inconstantes a nivel de cada táctica, producto
	SELECT B.CodTactica,B.CodProducto, 
	       COUNT(DISTINCT A.PKEbelista)*1.00/@TotalConsultorasInCtes AS Porcentaje 
	INTO #PenetracionInCtes 
	FROM #ConsultorasNuevasInCtes A INNER JOIN #FVTAPROEBECAM_NUEVAS B ON A.PKEbelista=B.PKEbelista 
	WHERE B.AnioCampana BETWEEN A.AnioCampana AND dbo.CalculaAnioCampana(A.AnioCampana, 2)
	GROUP BY B.CodTactica,B.CodProducto

	--Se calcula la frecuencia
	SELECT A.Pkebelista,B.CodTactica,B.CodProducto, 
	       COUNT(DISTINCT B.AnioCampana) Frecuencia 
	INTO #Frecuencia 
	FROM #BaseConsultoras A INNER JOIN #FVTAPROEBECAM_NUEVAS B ON A.Pkebelista=B.Pkebelista 
	WHERE B.AnioCampana BETWEEN @AnioCampanaProceso_Menos3 AND @AnioCampanaProceso 
	GROUP BY A.Pkebelista,B.CodTactica,B.CodProducto

	/** 2.1.2. Actualización de variables **/

	--Creacion de tabla intermedia
	CREATE TABLE #ListadoVariables(
	PKEbelista INT, 
	TipoTactica VARCHAR(30),
	CodTactica VARCHAR(4),
	FlagTop INT,
	Frecuencia REAL, 
	PenetracionCtes REAL,
	PenetracionInCtes REAL,
	PrecioOferta REAL,
	Penetracion REAL,
	FrecuenciaNor REAL,
	PrecioOfertaNor REAL,
	PenetracionNor REAL,
	Score REAL,
	ScoreNor REAL,
	UnidadesOferta INT,
	ProbabilidadCompra REAL)

	INSERT INTO #ListadoVariables
	SELECT A.PKEbelista,B.TipoTactica,B.CodTactica,FlagTop,0,0,0,B.PrecioOferta,0,0,0,0,0,0,B.Unidades,0
	FROM #BaseConsultoras A, #ProductosCUC B

	--Actualización de Frecuencia
	UPDATE #ListadoVariables
	SET Frecuencia = B.Frecuencia
	FROM #ListadoVariables A INNER JOIN #Frecuencia B 
	ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica 

	--Actualización del % Penetración Constantes
	UPDATE #ListadoVariables
	SET PenetracionCtes = B.Porcentaje
	FROM #ListadoVariables A INNER JOIN #PenetracionCtes B ON A.CodTactica=B.CodTactica 

	--Actualización del % Penetración Inconstantes
	UPDATE #ListadoVariables
	SET PenetracionInCtes = B.Porcentaje
	FROM #ListadoVariables A INNER JOIN #PenetracionInCtes B ON A.CodTactica=B.CodTactica 

	/** 2.2. Cálculo del Score **/

	--Se calcula la fórmula de la penetracion
	UPDATE #ListadoVariables  
	SET Penetracion = CASE(PenetracionInCtes) WHEN 0 THEN 0 ELSE PenetracionCtes/PenetracionInCtes END *(PenetracionCtes+PenetracionInCtes)/2

	--Se calcula el promedio y desviacion estándar
	SELECT 
	ROUND(SUM(Frecuencia)*1.0/COUNT(PKEbelista),4) PromFrecuencia, 
	ROUND(STDEVP(Frecuencia),4) DSFrecuencia,
	ROUND(SUM(Penetracion)*1.0/COUNT(PKEbelista),4) PromPenetracion, 
	ROUND(STDEVP(Penetracion),4) DSPenetracion
	INTO #ListadoPromDS
	FROM #ListadoVariables

	--Se calcula los datos normalizados
	UPDATE A 
	SET FrecuenciaNor = CASE (B.DSFrecuencia) WHEN 0 THEN 0 ELSE ROUND((Frecuencia-B.PromFrecuencia)/B.DSFrecuencia,4) END,
	    PenetracionNor = CASE (B.DSPenetracion) WHEN 0 THEN 0 ELSE ROUND((Penetracion-B.PromPenetracion)/B.DSPenetracion,4) END
	FROM #ListadoVariables A, #ListadoPromDS B

	--Se calcula el Score
	UPDATE #ListadoVariables  
	SET Score = PenetracionNor - FrecuenciaNor

	--Se calcula el promedio y desviación estándar del Score
	SELECT ROUND(SUM(Score)*1.0/COUNT(PKEbelista),4) PromScore, 
	       ROUND(STDEVP(Score),4) DSScore
	INTO #ListadoPromDSScore
	FROM #ListadoVariables

	--Se estandariza la variable Score
	UPDATE A
	SET ScoreNor = CASE (B.DSScore) WHEN 0 THEN 0 ELSE ROUND((A.Score-B.PromScore)/B.DSScore,4) END
	FROM #ListadoVariables A, #ListadoPromDSScore B

	--Se calcula el promedio y desviación estándar del Precio de Oferta
	SELECT ROUND(SUM(PrecioOferta)*1.0/COUNT(CodTactica),4) PromPrecioOferta, 
	       ROUND(STDEVP(PrecioOferta),4) DSPrecioOferta
	INTO #ListadoPromDSPrecio
	FROM #ProductosCUC

	--Se normaliza la variable Precio de Oferta
	UPDATE A 
	SET PrecioOfertaNor = CASE (B.DSPrecioOferta) WHEN 0 THEN 0 ELSE ROUND((PrecioOferta-B.PromPrecioOferta)/B.DSPrecioOferta,4) END
	FROM #ListadoVariables A, #ListadoPromDSPrecio B

	--Se calcula el Score en base a la Oportunidad
	UPDATE #ListadoVariables  
	SET ProbabilidadCompra = (EXP(ScoreNor+PrecioOfertaNor))/(1+EXP(ScoreNor+PrecioOfertaNor))*UnidadesOferta

	/** 2.3. Carga a tabla Listado de Probabilidades **/

	--Se borran los registros en caso de reproceso
	--DELETE FROM BDDM01.[DATAMARTANALITICO].dbo.ARP_ListadoProbabilidades 
	DELETE FROM BD_ANALITICO.dbo.ARP_ListadoProbabilidades 
	WHERE CodPais=@CodPais 
	AND AnioCampanaProceso=@AnioCampanaProceso 
	AND AnioCampanaExpo=@AnioCampanaExpo 
	AND TipoARP=@TipoARP 
	AND TipoPersonalizacion=@TipoPersonalizacion
	AND FlagMC=0
	AND Perfil=@Perfil

	--Carga los registros a la tabla ListadoProbabilidades
	--INSERT INTO BDDM01.[DATAMARTANALITICO].dbo.ARP_ListadoProbabilidades 
	INSERT INTO BD_ANALITICO.dbo.ARP_ListadoProbabilidades 
	(CodPais,AnioCampanaProceso,AnioCampanaExpo,PKEbelista,CodTActica,FlagTop,Probabilidad,TipoARP,TipoPersonalizacion,FlagMC,Perfil)
	SELECT @CodPais AS CodPais, @AnioCampanaProceso AS AnioCampanaProceso, @AnioCampanaExpo AS AnioCampanaExpo, PKEbelista,
		   CodTactica, FlagTop, ProbabilidadCompra AS Probabilidad, @TipoARP AS TipoARP, @TipoPersonalizacion AS TipoPersonalizacion,0,@Perfil AS Perfil
	FROM #ListadoVariables

	/** 2.4. Carga tabla de productos para el complemento **/
	
	--Creacion de la tabla
	CREATE TABLE #ListadoVariablesProductos(
		TipoTactica Varchar(30),
		CodTactica varchar(4),
		FlagTop int,
		PenetracionCtes real,
		PenetracionInCtes real,
		PrecioOferta real,
		Penetracion real,
		STD_PrecioOferta real,
		STD_Penetracion real,
		Score real,
		STD_Score real,
		UnidadesOferta int,
		ProbabilidadCompra real)

	INSERT INTO #ListadoVariablesProductos
	SELECT TipoTactica,CodTactica,FlagTop,0,0,PrecioOferta,0,0,0,0,0,Unidades,0
	FROM #ProductosCUC

	--Penetración Constantes
	UPDATE #ListadoVariablesProductos
	SET PenetracionCtes = B.Porcentaje
	FROM #ListadoVariablesProductos A INNER JOIN #PenetracionCtes B ON A.CodTactica=B.CodTactica 

	--Penetración Inconstantes
	UPDATE #ListadoVariablesProductos
	SET PenetracionInCtes = B.Porcentaje
	FROM #ListadoVariablesProductos A INNER JOIN #PenetracionInCtes B ON A.CodTactica=B.CodTactica 

	--Se calcula la fórmula de la penetracion
	UPDATE #ListadoVariablesProductos  
	SET Penetracion = CASE(PenetracionInCtes) WHEN 0 THEN 0 ELSE 
	    PenetracionCtes/PenetracionInCtes END *(PenetracionCtes+PenetracionInCtes)/2

	SELECT ROUND(SUM(Penetracion)*1.0/COUNT( CodTactica),4) PromPenetracion, 
		   ROUND(STDEVP(Penetracion),4) DSPenetracion,
		   ROUND(SUM(PrecioOferta)*1.0/COUNT(CodTactica),4) PromPrecioOferta, 
		   ROUND(STDEVP(PrecioOferta),4) DSPrecioOferta
	INTO #ListadoPromDSProd
	FROM #ListadoVariablesProductos

	--Se calcula los datos normalizados
	UPDATE  A 
	SET STD_Penetracion = CASE (B.DSPenetracion) WHEN 0 THEN 0 ELSE ROUND((Penetracion-B.PromPenetracion)/B.DSPenetracion,4) END,
		STD_PrecioOferta = CASE (B.DSPrecioOferta) WHEN 0 THEN 0 ELSE ROUND((PrecioOferta-B.PromPrecioOferta)/B.DSPrecioOferta,4) END
	FROM #ListadoVariablesProductos A, #ListadoPromDSProd B

	--Se calcula el Score
	UPDATE #ListadoVariablesProductos  
	SET Score = STD_Penetracion + STD_PrecioOferta

	-- Se calcula el Promedio y Desviación Estándar del Score
	SELECT ROUND(SUM(Score)*1.0/COUNT(CodTactica),4) PromScore, 
	       ROUND(STDEVP(Score),4) DSScore
	INTO #ListadoPromDSScoreProd
	FROM #ListadoVariablesProductos

	--Se Estandariza la variable Score
	UPDATE A
	SET STD_Score = CASE (B.DSScore) WHEN 0 THEN 0 ELSE ROUND((A.Score-B.PromScore)/B.DSScore,4) END
	FROM #ListadoVariablesProductos A, #ListadoPromDSScoreProd B

	--Se calcula el Score en base a la Oportunidad
	UPDATE #ListadoVariablesProductos  
		SET ProbabilidadCompra=(EXP(STD_Score))/(1+EXP(STD_Score))*UnidadesOferta

	--DELETE FROM BDDM01.[DATAMARTANALITICO].dbo.ARP_ListadoVariablesProductos
	DELETE FROM BD_ANALITICO.dbo.ARP_ListadoVariablesProductos
	WHERE CodPais=@CodPais 
	AND AnioCampanaProceso=@AnioCampanaProceso 
	AND AnioCampanaExpo=@AnioCampanaExpo 
	AND TipoARP=@TipoARP 
	AND TipoPersonalizacion=@TipoPersonalizacion

	--INSERT INTO BDDM01.[DATAMARTANALITICO].dbo.ARP_ListadoVariablesProductos
	INSERT INTO BD_ANALITICO.dbo.ARP_ListadoVariablesProductos
	(CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoARP,TipoPersonalizacion,TipoTactica,CodTactica,FlagTop,PenetracionCtes,PenetracionInCtes,
	PrecioOferta,Penetracion,STD_PrecioOferta,STD_Penetracion,Score,STD_Score,UnidadesOferta,ProbabilidadCompra)
	SELECT @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@TipoPersonalizacion AS TipoPersonalizacion,TipoTactica,CodTactica,FlagTop,PenetracionCtes,
	PenetracionInCtes,PrecioOferta,Penetracion,STD_PrecioOferta,STD_Penetracion,Score,STD_Score,UnidadesOferta,ProbabilidadCompra
	FROM #ListadoVariablesProductos

END

/** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,TipoGP,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,NULL,@Perfil,NULL,NULL,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

END


