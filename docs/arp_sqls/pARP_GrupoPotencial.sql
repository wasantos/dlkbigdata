CREATE PROCEDURE [dbo].[pARP_GrupoPotencial] @CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoPersonalizacion CHAR(3),@TipoARP CHAR(1),
@FlagCarga INT,@Perfil VARCHAR(1),@TipoGP INT
AS
BEGIN 

/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoPersonalizacion			CHAR(3) 
DECLARE @TipoARP						CHAR(1)
DECLARE @FlagCarga						INT
DECLARE @Perfil							VARCHAR(1)
DECLARE @TipoGP							INT

SET @CodPais				= 'PA'		-- Código de país
SET @AnioCampanaProceso		= '201705'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201707'	-- Campaña de Venta
SET @TipoPersonalizacion	= 'ODD'		-- 'OPT','ODD','SR'
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
SET @FlagCarga				=  1		-- 1: Carga/Personalización | 0: Estimación
SET @Perfil					= '1'		-- Número de Perfil | 'X': Sin Perfil
SET @TipoGP					=  2		-- 1: Segmento y Región | 2: Perfil*/

print '==========================================GrupoPotencial=========================================='
DECLARE @AnioCampanaInicio6UC CHAR(6)  
DECLARE @AnioCampanaInicio24UC CHAR(6)  

SET @AnioCampanaInicio6UC		= dbo.CalculaAnioCampana(@AnioCampanaProceso, -5)  
SET @AnioCampanaInicio24UC		= dbo.CalculaAnioCampana(@AnioCampanaProceso, -23)

/** Inicio: Variables Log **/
DECLARE @FechaInicio 					DATETIME 
DECLARE @Procedimiento					VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_GrupoPotencial'
/** Fin: Variables Log **/

IF OBJECT_ID('ListadoProductos') IS NOT NULL
DROP TABLE ListadoProductos

IF OBJECT_ID('ListadoRegalos') IS NOT NULL
DROP TABLE ListadoRegalos

IF OBJECT_ID('EspaciosForzados') IS NOT NULL
DROP TABLE EspaciosForzados

IF OBJECT_ID('CampaniaExpoEspacios') IS NOT NULL
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
	AND TipoPersonalizacion=@TipoPersonalizacion AND Perfil=@Perfil

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

/** 1. Establecidas **/
IF (@TipoARP='E')
BEGIN

	/** Eliminar tablas temporales **/
	IF OBJECT_ID('ListadoConsultora') IS NOT NULL DROP TABLE ListadoConsultora
	IF OBJECT_ID('FSTAEBECAM') IS NOT NULL DROP TABLE FSTAEBECAM
	IF OBJECT_ID('BaseEstadoConsultoras') IS NOT NULL DROP TABLE BaseEstadoConsultoras
	IF OBJECT_ID('TEMP_FVTAPROEBECAM') IS NOT NULL DROP TABLE TEMP_FVTAPROEBECAM
	IF OBJECT_ID('FVTAPROEBECAMU24C') IS NOT NULL DROP TABLE FVTAPROEBECAMU24C
	IF OBJECT_ID('FVTAPROEBECAMU6C') IS NOT NULL DROP TABLE FVTAPROEBECAMU6C
	IF OBJECT_ID('BasePotencial_aux3') IS NOT NULL DROP TABLE BasePotencial_aux3
	IF OBJECT_ID('BasePotencial_auxU6C') IS NOT NULL DROP TABLE BasePotencial_auxU6C
	IF OBJECT_ID('BasePotencial_aux2') IS NOT NULL DROP TABLE BasePotencial_aux2
	IF OBJECT_ID('BasePotencialU6C') IS NOT NULL DROP TABLE BasePotencialU6C
	IF OBJECT_ID('BasePotencial_aux3_p') IS NOT NULL DROP TABLE BasePotencial_aux3_p
	IF OBJECT_ID('BasePotencial_auxU6C_p') IS NOT NULL DROP TABLE BasePotencial_auxU6C_p
	IF OBJECT_ID('BasePotencial_aux2_p') IS NOT NULL DROP TABLE BasePotencial_aux2_p
	IF OBJECT_ID('BasePotencialU6C_p') IS NOT NULL DROP TABLE BasePotencialU6C_p
	IF OBJECT_ID('PO_Venta') IS NOT NULL DROP TABLE PO_Venta
	IF OBJECT_ID('PO_ProdXConsultora') IS NOT NULL DROP TABLE PO_ProdXConsultora
	IF OBJECT_ID('PO_Catalogo') IS NOT NULL DROP TABLE PO_Catalogo
	IF OBJECT_ID('PrecioOptimo') IS NOT NULL DROP TABLE PrecioOptimo
	IF OBJECT_ID('PO_PrecioOptimo') IS NOT NULL DROP TABLE PO_PrecioOptimo
	IF OBJECT_ID('PO_PedidosPuros') IS NOT NULL DROP TABLE PO_PedidosPuros
	IF OBJECT_ID('PrecioOptimoFinal') IS NOT NULL DROP TABLE PrecioOptimoFinal
	IF OBJECT_ID('PO_PrecioMinimo') IS NOT NULL DROP TABLE PO_PrecioMinimo
	IF OBJECT_ID('BasePotencial_aux24C') IS NOT NULL DROP TABLE BasePotencial_aux24C
	IF OBJECT_ID('BasePotencial_aux24C2') IS NOT NULL DROP TABLE BasePotencial_aux24C2
	IF OBJECT_ID('BasePotencial_aux24C3') IS NOT NULL DROP TABLE BasePotencial_aux24C3
	IF OBJECT_ID('BasePotencialU24C') IS NOT NULL DROP TABLE BasePotencialU24C
	IF OBJECT_ID('BaseCicloRecompra') IS NOT NULL DROP TABLE BaseCicloRecompra
	IF OBJECT_ID('Tabla1') IS NOT NULL DROP TABLE Tabla1
	IF OBJECT_ID('Tabla2') IS NOT NULL DROP TABLE Tabla2
	IF OBJECT_ID('Tabla3') IS NOT NULL DROP TABLE Tabla3
	IF OBJECT_ID('Tabla4') IS NOT NULL DROP TABLE Tabla4
	IF OBJECT_ID('Tabla5') IS NOT NULL DROP TABLE Tabla5
	IF OBJECT_ID('Tabla6') IS NOT NULL DROP TABLE Tabla6
	IF OBJECT_ID('BasePotencial_aux24C_p') IS NOT NULL DROP TABLE BasePotencial_aux24C_p
	IF OBJECT_ID('BasePotencial_aux24C2_p') IS NOT NULL DROP TABLE BasePotencial_aux24C2_p
	IF OBJECT_ID('BasePotencial_aux24C3_p') IS NOT NULL DROP TABLE BasePotencial_aux24C3_p
	IF OBJECT_ID('BasePotencialU24C_p') IS NOT NULL DROP TABLE BasePotencialU24C_p
	IF OBJECT_ID('BaseCicloRecompra_p') IS NOT NULL DROP TABLE BaseCicloRecompra_p
	IF OBJECT_ID('Tabla1_p') IS NOT NULL DROP TABLE Tabla1_p
	IF OBJECT_ID('Tabla2_p') IS NOT NULL DROP TABLE Tabla2_p
	IF OBJECT_ID('Tabla3_p') IS NOT NULL DROP TABLE Tabla3_p
	IF OBJECT_ID('Tabla4_p') IS NOT NULL DROP TABLE Tabla4_p
	IF OBJECT_ID('Tabla5_p') IS NOT NULL DROP TABLE Tabla5_p
	IF OBJECT_ID('Tabla6_p') IS NOT NULL DROP TABLE Tabla6_p
	IF OBJECT_ID('GrupoPotencial') IS NOT NULL DROP TABLE GrupoPotencial
	IF OBJECT_ID('GrupoPotencial_p') IS NOT NULL DROP TABLE GrupoPotencial_p
	IF OBJECT_ID('ListadoConsultora_Bundle') IS NOT NULL DROP TABLE ListadoConsultora_Bundle
	IF OBJECT_ID('BasePotencial_ProductoBundle') IS NOT NULL DROP TABLE BasePotencial_ProductoBundle
	IF OBJECT_ID('BasePotencial_ProductoBundle_p') IS NOT NULL DROP TABLE BasePotencial_ProductoBundle_p
	IF OBJECT_ID('ListadoConsultora_TacticaBundle') IS NOT NULL DROP TABLE ListadoConsultora_TacticaBundle
	IF OBJECT_ID('BasePotencial_TacticaBundle') IS NOT NULL DROP TABLE BasePotencial_TacticaBundle
	IF OBJECT_ID('BasePotencial_TacticaBundle_PO') IS NOT NULL DROP TABLE BasePotencial_TacticaBundle_PO
	IF OBJECT_ID('BasePotencial_TacticaBundle_p') IS NOT NULL DROP TABLE BasePotencial_TacticaBundle_p
	IF OBJECT_ID('BasePotencial_TacticaBundle_PO_p') IS NOT NULL DROP TABLE BasePotencial_TacticaBundle_PO_p
	IF OBJECT_ID('ListadoConsultora_Total ') IS NOT NULL DROP TABLE ListadoConsultora_Total 

	--Leo la Base de Consultoras
	--Extraigo el Perfil para Grupo Potencial
	SELECT A.*, B.Perfil AS PerfilGP
	INTO #InfoConsultora 
	--FROM BDDM01.[DATAMARTANALITICO].DBO.ARP_BaseConsultoras 
	FROM BD_ANALITICO.dbo.ARP_BaseConsultoras A (NOLOCK) LEFT JOIN BD_ANALITICO.dbo.MDL_PerfilOutput B (NOLOCK) ON 
	A.PKEbelista=B.PKEbelista AND B.CodPais=@CodPais AND B.AnioCampanaProceso=@AnioCampanaProceso
	WHERE A.AnioCampanaProceso = @AnioCampanaProceso 
	AND A.CodPais = @CodPais 
	AND A.TipoARP = @TipoARP
	AND A.Perfil = @Perfil

	SELECT PKEbelista 
	INTO #BaseConsultoras 
	FROM #InfoConsultora

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

	/** 1.1. Individual **/

	SELECT A.*, B.PerfilGP
	INTO ListadoConsultora 
	FROM BD_ANALITICO.dbo.ARP_ListadoVariablesIndividual A (NOLOCK) INNER JOIN #InfoConsultora B ON A.PKEbelista=B.PKEbelista

	--Se obtienen los productos a nivel de Item (Pkproducto) 
	SELECT TipoTactica,CodTactica, B.CodCUC CodProducto,Unidades,B.DesProductoCUC DesProducto,A.PrecioOferta,CodMarca,PKProducto 
	INTO #Productos 
	FROM #ListadoProductos A 
	--INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais

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

	--Se crea la tabla temporal donde se guardan la venta de las últimas 24 campañas
	CREATE TABLE TEMP_FVTAPROEBECAM(
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
	PRINT 'INSERT INTO BD_ANALITICO.dbo.ARP_FVTAPROEBECAM_PaisU24C'
	INSERT INTO BD_ANALITICO.dbo.ARP_FVTAPROEBECAM_PaisU24C
	SELECT A.AnioCampana,A.PKEbelista,A.PKProducto,A.PKTipoOferta,A.PKTerritorio,A.PKPedido,
	       A.CodVenta,A.AnioCampanaRef,A.RealUUVendidas,A.RealVtaMNNeto,A.RealVtaMNFactura,A.RealVtaMNCatalogo
	FROM DWH_ANALITICO.dbo.DWH_FVTAPROEBECAM A (NOLOCK) INNER JOIN #BaseConsultoras D ON A.PKEbelista=D.PKEbelista
	WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso
	AND CodPais=@CodPais

	PRINT 'INTO FSTAEBECAM'
	SELECT PKEbelista,AnioCampana,FlagPasoPedido,FlagActiva,CodComportamientoRolling,codigofacturaINTernet 
	INTO FSTAEBECAM 
	--FROM FSTAEBECAMC01_VIEW
	FROM DWH_ANALITICO.dbo.DWH_FSTAEBECAM (NOLOCK)
	WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso
	AND CodPais=@CodPais

	--Obtengo el estado de las consultoras que conforman la base en las últimas 24 campañas
	SELECT A.PKEbelista,B.AnioCampana,B.FlagPasoPedido,B.FlagActiva 
	INTO BaseEstadoConsultoras
	FROM #BaseConsultoras A INNER JOIN FSTAEBECAM B ON A.PKEbelista=B.PKEbelista
	--WHERE B.AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso

	PRINT 'INSERT INTO TEMP_FVTAPROEBECAM'
	INSERT INTO TEMP_FVTAPROEBECAM
	SELECT A.AnioCampana,A.PKEbelista,A.PKProducto,A.PKTipoOferta,A.PKTerritorio,A.PKPedido,
	       A.CodVenta,A.AnioCampanaRef,A.RealUUVendidas,A.RealVtaMNNeto,A.RealVtaMNFactura,A.RealVtaMNCatalogo
	--FROM FVTAPROEBECAMC01 A 
	FROM ARP_FVTAPROEBECAM_PaisU24C A 
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

	PRINT 'FVTAPROEBECAMU24C'
	SELECT B.CodTactica, B.CodProducto,A.* 
	INTO FVTAPROEBECAMU24C   
	FROM TEMP_FVTAPROEBECAM A INNER JOIN #Productos_Individual B ON A.PKProducto=B.PKProducto  
	--WHERE AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso    

	SELECT *
	INTO FVTAPROEBECAMU6C   
	FROM FVTAPROEBECAMU24C
	WHERE AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso

	/** Base Potencial U6C para GP por Región y Segmento **/
	IF (@TipoGP=1)
	BEGIN
		PRINT 'INTO BasePotencial_aux3'
		--Se obtiene la Base Potencial para las últimas 6 campañas anteriores a la campaña de proceso. 
		--Para hallar la Venta Potencial Mínima U6C y la Frecuencia U6C GP
		--Se filtran solo las consultoras que hayan realizado como mínimo un pedido Web o Web Mixto y Activas en la campañana de proceso
		SELECT A.AnioCampana, a.PKEbelista,A.CodTactica, A.CodProducto, D.CodRegion, C.CodComportamientoRolling, 
			   SUM(RealVtaMNNeto) VentaPotencial
		INTO BasePotencial_aux3
		FROM FVTAPROEBECAMU6C A 
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
		INTO BasePotencial_auxU6C 
		FROM BasePotencial_aux3 A INNER JOIN							
		FSTAEBECAM E ON A.PKEbelista=E.PKEbelista AND E.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso 
		WHERE E.codigofacturaInternet IN ('WEB','WMX') 

		--Se halla la venta potencial mínima por consultora y la cantidad de pedidos puros a nivel de consultora
		SELECT CodTactica, CodProducto, CodRegion, CodComportamientoRolling,Pkebelista, 
			   MIN(VentaPotencial) VentaPotencialMin, 
			   COUNT(DISTINCT AnioCampana) PedidosPuros
		INTO BasePotencial_aux2
		FROM BasePotencial_auxU6C
		GROUP BY CodTactica, CodProducto, CodRegion, CodComportamientoRolling, Pkebelista

		--Se halla el promedio de la venta potencial mínima por consultora y el promedio de la cantidad de pedidos puros (Frecuencia U6C GP) a nivel de la Base Potencial
		SELECT CodTactica, CodProducto, CodRegion, CodComportamientoRolling, 
			   AVG(VentaPotencialMin) PromVentaPotencialMin, 
			   ROUND(SUM(PedidosPuros)*1.0/COUNT(PedidosPuros),1) Frecuencia 
		INTO BasePotencialU6C
		FROM BasePotencial_aux2 
		GROUP BY CodTactica, CodProducto, CodRegion, CodComportamientoRolling

	END

	/** Base Potencial U6C para GP por Perfil **/
	IF (@TipoGP=2)
	BEGIN
		PRINT 'INTO BasePotencial_aux3_p'
		--Se obtiene la Base Potencial para las últimas 6 campañas anteriores a la campaña de proceso. 
		--Para hallar la Venta Potencial Mínima U6C y la Frecuencia U6C GP
		--Se filtran solo las consultoras que hayan realizado como mínimo un pedido Web o Web Mixto y Activas en la campañana de proceso
		SELECT A.AnioCampana, a.PKEbelista,A.CodTactica, A.CodProducto, PerfilGP, 
			   SUM(RealVtaMNNeto) VentaPotencial
		INTO BasePotencial_aux3_p
		FROM FVTAPROEBECAMU6C A 
		--INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista
		INNER JOIN FSTAEBECAM C ON A.PKEbelista=C.PKEbelista AND C.AnioCampana=@AnioCampanaProceso			-- Segmento de la campaña de proceso
		--INNER JOIN DGEOGRAFIACAMPANA D ON B.PKTerritorio=D.PKTerritorio AND D.AnioCampana=@AnioCampanaProceso	-- Región de la campaña de proceso
		INNER JOIN #InfoConsultora D ON A.PKEbelista=D.PKEbelista
		WHERE A.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso 
		AND C.FlagActiva=1 -- Activa en la Campaña de proceso																			
		GROUP BY A.AnioCampana, a.PKEbelista, A.CodTactica,A.CodProducto,D.PerfilGP,A.PKPedido
		HAVING SUM(RealVtaMNNeto)>0

		--Se filtran solo las consultoras que hayan realizado como mínimo un pedido Web o Web Mixto
		SELECT DISTINCT A.* 
		INTO BasePotencial_auxU6C_p 
		FROM BasePotencial_aux3_p A INNER JOIN							
		FSTAEBECAM E ON A.PKEbelista=E.PKEbelista AND E.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso 
		WHERE E.codigofacturaInternet IN ('WEB','WMX') 

		--Se halla la venta potencial mínima por consultora y la cantidad de pedidos puros a nivel de consultora
		SELECT CodTactica, CodProducto, PerfilGP, Pkebelista, 
			   MIN(VentaPotencial) VentaPotencialMin, 
			   COUNT(DISTINCT AnioCampana) PedidosPuros
		INTO BasePotencial_aux2_p
		FROM BasePotencial_auxU6C_p
		GROUP BY CodTactica, CodProducto, PerfilGP, Pkebelista

		--Se halla el promedio de la venta potencial mínima por consultora y el promedio de la cantidad de pedidos puros (Frecuencia U6C GP) a nivel de la Base Potencial
		SELECT CodTactica, CodProducto, PerfilGP, 
			   AVG(VentaPotencialMin) PromVentaPotencialMin, 
			   ROUND(SUM(PedidosPuros)*1.0/COUNT(PedidosPuros),1) Frecuencia 
		INTO BasePotencialU6C_p
		FROM BasePotencial_aux2_p 
		GROUP BY CodTactica, CodProducto, PerfilGP

	END

	/** Precio Óptimo **/
	--Obtengo los productos vendidos en las últimas 24 campañas
	SELECT AnioCampana, Aniocampanaref, A.PKEbelista,CodTactica, CodProducto, PKProducto,PKTipoOferta,CodVenta,RealVtaMNNeto, 
	RealVtaMNCatalogo,RealUUVendidas 
	INTO PO_Venta
	FROM FVTAPROEBECAMU24C A --INNER JOIN #BaseConsultoras B ON A.PKEbelista=B.PKEbelista

	SELECT DISTINCT PKEbelista, CodTactica,CodProducto, PKProducto 
	INTO PO_ProdXConsultora 
	FROM PO_Venta

	PRINT 'INTO PO_Catalogo'
	--Obtengo la matriz de venta de los productos propuestos
	SELECT B.CodTactica,B.CodProducto,A.* 
	INTO PO_Catalogo 
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
	INTO PrecioOptimo 
	FROM PO_ProdXConsultora A INNER JOIN PO_Catalogo B ON A.PKProducto=B.PKProducto 
	INNER JOIN BaseEstadoConsultoras C ON A.PKEbelista=C.PKEbelista AND B.AnioCampana=C.AnioCampana
	WHERE C.FlagPasoPedido=1 
	GROUP BY A.Pkebelista,A.CodTactica,A.CodProducto,PrecioOferta

	--Pedidos puros
	SELECT A.Pkebelista,A.CodTactica,A.CodProducto, PrecioOferta, 
	       COUNT(DISTINCT B.AnioCampana) PedidosPuros 
	INTO PO_PedidosPuros 
	FROM PO_Venta A 
	INNER JOIN PO_Catalogo B ON A.AnioCampana=B.AnioCampana AND A.PKProducto = B.PKProducto
	AND A.PKTipoOferta=B.PKTipoOferta AND A.CodVenta=B.CodVenta
	GROUP BY A.Pkebelista,A.CodTactica,A.CodProducto, PrecioOferta

	UPDATE A 
	SET A.PedidosPuros = B.PedidosPuros,
	    A.Probabilidad = B.PedidosPuros * 1.00 / A.NumAspeos
	FROM PrecioOptimo A INNER JOIN PO_PedidosPuros B ON A.PKEbelista=B.PKEbelista AND A.CodTactica=B.CodTactica 
	AND A.CodProducto=B.CodProducto 
	AND A.PrecioOferta=B.PrecioOferta

	--Ordena por probabilidad y luego por número de pedido puro
	SELECT PKEbelista,CodTactica,CodProducto,PrecioOferta,NumAspeos,PedidosPuros,Probabilidad,
	ROW_NUMBER() OVER(PARTITION BY PKEbelista,CodTactica,CodProducto ORDER BY Probabilidad DESC,PedidosPuros DESC,PrecioOferta ASC) AS Posicion
	INTO PrecioOptimoFinal 
	FROM PrecioOptimo
	ORDER BY PKEbelista,CodTactica,CodProducto,PrecioOferta

	SELECT CodTactica, CodProducto, 
	       MIN(PrecioOferta) PrecioMinimo 
	INTO PO_PrecioMinimo 
	FROM PrecioOptimo  
	GROUP BY CodTactica, CodProducto

	/** Recencia y Frecuencia de U24C para GP **/

	/** Base Potencial U24C para GP por Región y Segmento **/
	IF (@TipoGP=1)
	BEGIN
		PRINT 'INTO BasePotencial_aux24C'
		SELECT A.AnioCampana, A.PKEbelista, A.CodTactica, A.CodProducto, D.CodRegion, C.CodComportamientoRolling 
		INTO BasePotencial_aux24C  
		FROM FVTAPROEBECAMU24C A 
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
		INTO BasePotencial_aux24C2 
		FROM BasePotencial_aux24C A INNER JOIN #FSTAEBECAM E ON A.PKEbelista=E.PKEbelista  
		AND E.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso 
		WHERE E.codigofacturaINTerneT IN ('WEB','WMX')  

		--Se halla la cantidad de pedidos puros - frecuencia y el máximo de recencia a nivel de cada consultora
		SELECT CodTactica, CodProducto, CodRegion, CodComportamientoRolling,pkebelista, 
			   dbo.DiffANIOCampanas(@AnioCampanaProceso,MAX(AnioCampana)) Recencia,
			   COUNT(DISTINCT AnioCampana) PedidosPuros
		INTO BasePotencial_aux24C3
		FROM BasePotencial_aux24C2
		GROUP BY CodTactica, CodProducto, CodRegion, CodComportamientoRolling, pkebelista

		--Se halla la recencia U24C y frecuencia U24C a nivel de la base potencial
		SELECT CodTactica, CodProducto, CodRegion, CodComportamientoRolling, 0 CicloRecompraPotencial, CAST(0 AS FLOAT) PrecioOptimoGP,
			   AVG(CAST(Recencia AS FLOAT)) RecenciaGP, 
			   ROUND(SUM(PedidosPuros)*1.0/COUNT(PedidosPuros),1) FrecuenciaGP 
		INTO BasePotencialU24C
		FROM BasePotencial_aux24C3 
		GROUP BY CodTactica, CodProducto, CodRegion, CodComportamientoRolling

		--Cálculo de Ciclo de Recompra Potencial
		SELECT PKEbelista,CodTactica, CodProducto,Codregion,CodComportamientoRolling, 
			   MAX(AnioCampana) AnioCampaniaMax,
			   (23-dbo.DiffANIOCampanas(MIN(AnioCampana),@AnioCampanaInicio24UC)-dbo.DiffANIOCampanas(@AnioCampanaProceso,
			   MAX(AnioCampana)))*1.0/(COUNT(AnioCampana)-1) CicloRecompraPotencial
		INTO BaseCicloRecompra 
		FROM BasePotencial_aux24C
		GROUP BY PKEbelista,CodTactica,CodProducto,Codregion,CodComportamientoRolling
		HAVING COUNT(AnioCampana)>1

		--Actualizo Ciclo Recompra Potencial
		UPDATE A
		SET CicloRecompraPotencial = B.CicloRecompraPotencial,
			FrecuenciaGP = CEILING(FrecuenciaGP)
		FROM BasePotencialU24C A INNER JOIN (
		SELECT CodTactica,CodProducto,codregion,CodComportamientoRolling, CEILING(SUM(CicloRecompraPotencial)/COUNT(PKEbelista)) CicloRecompraPotencial 
		FROM BaseCicloRecompra GROUP BY CodTactica,CodProducto,codregion,CodComportamientoRolling) B ON A.CodTactica=B.CodTactica  AND
		A.CodProducto=B.CodProducto AND A.codregion=B.codregion AND A.CodComportamientoRolling=B.CodComportamientoRolling

		-- Calcular la nueva Recencia y Frecuencia de U24C para GP para la consultoras que no han comprado los productos
		SELECT PKEbelista, CodRegion, CodComportamientoRolling, CodTactica, CodProducto,CAST(0 AS FLOAT) PrecioOptimo,
			   COUNT(DISTINCT AnioCampana) PedidosPuros,
			   dbo.DiffANIOCampanas(@AnioCampanaProceso, MAX(AnioCampana)) Recencia 
		INTO Tabla1 
		FROM BasePotencial_aux24C2
		GROUP BY PKEbelista,CodRegion,CodComportamientoRolling,CodTactica,CodProducto

		--Se setean en 24 a los registros cuya recencia es 0
		UPDATE Tabla1 
		SET Recencia = 24 
		WHERE Recencia = 0

		UPDATE A
		SET PrecioOptimo = B.PrecioOferta
		FROM Tabla1 A INNER JOIN PrecioOptimoFinal B ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto 
		AND A.CodTactica=B.CodTactica AND Posicion = 1

		SELECT CodRegion, CodComportamientoRolling, 
			   COUNT(DISTINCT PKEbelista) TotalConsultoras 
		INTO Tabla2 
		FROM #InfoConsultora
		GROUP BY CodRegion, CodComportamientoRolling

		--Se suman los pedidos puros
		SELECT CodRegion, CodComportamientoRolling, CodTactica, CodProducto,
			   SUM(PedidosPuros) TotalPedidosPuros,
			   SUM(Recencia) TotalRecencia,
			   SUM(PrecioOptimo) TotalPrecioOptimo, 
			   0 TotalConsultoras,
			   COUNT(DISTINCT PKEbelista) TotalConsultorasConVenta 
		INTO Tabla3
		FROM Tabla1
		GROUP BY CodRegion, CodComportamientoRolling, CodTactica, CodProducto

		UPDATE A
		SET A.TotalConsultoras = B.TotalConsultoras
		FROM Tabla3 A INNER JOIN Tabla2 B ON A.CodComportamientoRolling=B.CodComportamientoRolling AND A.CodRegion=B.CodRegion

		--Se actualiza el nuevo valor de la Frecuencia, Recencia y Precio Óptimo U24C GP
		UPDATE A
		SET A.FrecuenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE ROUND((B.TotalPedidosPuros*1.00/TotalConsultoras),2) END,
			/*A.RecenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE CEILING((B.TotalRecencia*1.00+(24*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras) END,*/
			A.RecenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE (B.TotalRecencia*1.00+(24*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras END,
			A.PrecioOptimoGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE (B.TotalPrecioOptimo*1.00+(C.PrecioMinimo*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras END
		FROM BasePotencialU24C A INNER JOIN Tabla3 B ON A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodProducto=B.CodProducto INNER JOIN PO_PrecioMinimo C ON A.CodProducto=C.CodProducto AND A.CodTactica=B.CodTactica

		/** Revisar **/
		--Se halla la venta potencial mínima por Consultora a nivel de consultora
		SELECT PKEbelista,CodTactica, CodProducto, CodRegion, CodComportamientoRolling,
			   MIN(VentaPotencial) VentaPotencialMin 
		INTO Tabla4
		FROM BasePotencial_auxU6C
		GROUP BY PKEbelista,CodTactica,CodProducto, CodRegion, CodComportamientoRolling 

		--Se halla el promedio de la venta potencial mínima por Consultora a nivel de la Base Potencial
		SELECT CodRegion, CodComportamientoRolling,CodTactica, CodProducto, 0 TotalConsultoras,
			   SUM(VentaPotencialMin) VentaPotencialMin 
		INTO Tabla5 
		FROM Tabla4 
		GROUP by CodRegion, CodComportamientoRolling,CodTactica,CodProducto 

		--Se halla el total de consultoras por Región y Comportamiento
		SELECT CodRegion,CodComportamientoRolling,
			   COUNT(DISTINCT PKEbelista) TotalConsultotas  
		INTO Tabla6 
		FROM #InfoConsultora
		GROUP BY CodRegion,CodComportamientoRolling

		UPDATE A
		SET A.TotalConsultoras = B.TotalConsultotas
		FROM #Tabla5 A INNER JOIN #Tabla6 B ON A.CodComportamientoRolling=B.CodComportamientoRolling AND A.CodRegion=B.CodRegion

		--Se actualiza el nuevo valor de la VentaMinima U6C
		UPDATE A
		SET A.PromVentaPotencialMin = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE ROUND((B.VentaPotencialMin*1.00/TotalConsultoras),2) END
		FROM BasePotencialU6C A INNER JOIN Tabla5 B ON A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica

	END

	/** Base Potencial U24C para GP por Perfil **/
	IF (@TipoGP=2)
	BEGIN
		PRINT 'INTO BasePotencial_aux24C_p'
		SELECT A.AnioCampana, A.PKEbelista, A.CodTactica, A.CodProducto, D.PerfilGP
		INTO BasePotencial_aux24C_p  
		FROM FVTAPROEBECAMU24C A 
		--INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista  
		INNER JOIN FSTAEBECAM C ON A.PKEbelista=C.PKEbelista  AND C.AnioCampana=@AnioCampanaProceso
		--INNER JOIN DGEOGRAFIACAMPANA D ON B.PKTerritorio=D.PKTerritorio AND D.AnioCampana=@AnioCampanaProceso
		INNER JOIN #InfoConsultora D ON A.PKEbelista=D.PKEbelista
		WHERE A.AnioCampana BETWEEN @AnioCampanaInicio24UC AND @AnioCampanaProceso 
		AND C.FlagActiva=1 
		GROUP BY A.AnioCampana, a.PKEbelista,A.CodTactica,  A.CodProducto,D.PerfilGP,A.PKPedido
		HAVING SUM(RealVtaMNNeto)>0   

		SELECT DISTINCT A.* 
		INTO BasePotencial_aux24C2_p 
		FROM BasePotencial_aux24C_p A INNER JOIN FSTAEBECAM E ON A.PKEbelista=E.PKEbelista  
		AND E.AnioCampana BETWEEN @AnioCampanaInicio6UC AND @AnioCampanaProceso 
		WHERE E.codigofacturaINTerneT IN ('WEB','WMX')  

		--Se halla la cantidad de pedidos puros - frecuencia y el máximo de recencia a nivel de cada consultora
		SELECT CodTactica, CodProducto, PerfilGP,pkebelista, 
			   dbo.DiffANIOCampanas(@AnioCampanaProceso,MAX(AnioCampana)) Recencia,
			   COUNT(DISTINCT AnioCampana) PedidosPuros
		INTO BasePotencial_aux24C3_p
		FROM BasePotencial_aux24C2_p
		GROUP BY CodTactica, CodProducto,PerfilGP, pkebelista

		--Se halla la recencia U24C y frecuencia U24C a nivel de la base potencial
		SELECT CodTactica, CodProducto, PerfilGP, 0 CicloRecompraPotencial, CAST(0 AS FLOAT) PrecioOptimoGP,
			   AVG(CAST(Recencia AS FLOAT)) RecenciaGP, 
			   ROUND(SUM(PedidosPuros)*1.0/COUNT(PedidosPuros),1) FrecuenciaGP 
		INTO BasePotencialU24C_p
		FROM BasePotencial_aux24C3_p 
		GROUP BY CodTactica, CodProducto, PerfilGP

		--Cálculo de Ciclo de Recompra Potencial
		SELECT PKEbelista,CodTactica, CodProducto,PerfilGP, 
			   MAX(AnioCampana) AnioCampaniaMax,
			   (23-dbo.DiffANIOCampanas(MIN(AnioCampana),@AnioCampanaInicio24UC)-dbo.DiffANIOCampanas(@AnioCampanaProceso,
			   MAX(AnioCampana)))*1.0/(COUNT(AnioCampana)-1) CicloRecompraPotencial
		INTO BaseCicloRecompra_p 
		FROM BasePotencial_aux24C_p
		GROUP BY PKEbelista,CodTactica,CodProducto,PerfilGP
		HAVING COUNT(AnioCampana)>1

		--Actualizo Ciclo Recompra Potencial
		UPDATE A
		SET CicloRecompraPotencial = B.CicloRecompraPotencial,
			FrecuenciaGP = CEILING(FrecuenciaGP)
		FROM BasePotencialU24C_p A INNER JOIN (
		SELECT CodTactica,CodProducto,PerfilGP, CEILING(SUM(CicloRecompraPotencial)/COUNT(PKEbelista)) CicloRecompraPotencial 
		FROM BaseCicloRecompra_p GROUP BY CodTactica,CodProducto,PerfilGP) B ON A.CodTactica=B.CodTactica AND
		A.CodProducto=B.CodProducto AND A.PerfilGP=B.PerfilGP

		-- Calcular la nueva Recencia y Frecuencia de U24C para GP para la consultoras que no han comprado los productos
		SELECT PKEbelista,PerfilGP, CodTactica, CodProducto,CAST(0 AS FLOAT) PrecioOptimo,
			   COUNT(DISTINCT AnioCampana) PedidosPuros,
			   dbo.DiffANIOCampanas(@AnioCampanaProceso, MAX(AnioCampana)) Recencia 
		INTO Tabla1_p 
		FROM BasePotencial_aux24C2_p
		GROUP BY PKEbelista,PerfilGP,CodTactica,CodProducto

		--Se setean en 24 a los registros cuya recencia es 0
		UPDATE Tabla1_p 
		SET Recencia = 24 
		WHERE Recencia = 0

		UPDATE A
		SET PrecioOptimo = B.PrecioOferta
		FROM Tabla1_p A INNER JOIN PrecioOptimoFinal B ON A.PKEbelista=B.PKEbelista AND A.CodProducto=B.CodProducto 
		AND A.CodTactica=B.CodTactica AND Posicion = 1

		SELECT PerfilGP, 
			   COUNT(DISTINCT PKEbelista) TotalConsultoras 
		INTO Tabla2_p 
		FROM #InfoConsultora
		GROUP BY PerfilGP

		--Se suman los pedidos puros
		SELECT PerfilGP, CodTactica, CodProducto,
			   SUM(PedidosPuros) TotalPedidosPuros,
			   SUM(Recencia) TotalRecencia,
			   SUM(PrecioOptimo) TotalPrecioOptimo, 
			   0 TotalConsultoras,
			   COUNT(DISTINCT PKEbelista) TotalConsultorasConVenta 
		INTO Tabla3_p
		FROM Tabla1_p
		GROUP BY PerfilGP, CodTactica, CodProducto

		UPDATE A
		SET A.TotalConsultoras = B.TotalConsultoras
		FROM Tabla3_p A INNER JOIN Tabla2_p B ON A.PerfilGP=B.PerfilGP

		--Se actualiza el nuevo valor de la Frecuencia, Recencia y Precio Óptimo U24C GP
		UPDATE A
		SET A.FrecuenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE ROUND((B.TotalPedidosPuros*1.00/TotalConsultoras),2) END,
			/*A.RecenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE CEILING((B.TotalRecencia*1.00+(24*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras) END,*/
			A.RecenciaGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE (B.TotalRecencia*1.00+(24*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras END,
			A.PrecioOptimoGP = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE (B.TotalPrecioOptimo*1.00+(C.PrecioMinimo*(TotalConsultoras-TotalConsultorasConVenta)))/TotalConsultoras END
		FROM BasePotencialU24C_p A INNER JOIN Tabla3_p B ON A.PerfilGP=B.PerfilGP
		AND A.CodProducto=B.CodProducto INNER JOIN PO_PrecioMinimo C ON A.CodProducto=C.CodProducto AND A.CodTactica=B.CodTactica

		/** Revisar **/
		--Se halla la venta potencial mínima por Consultora a nivel de consultora
		SELECT PKEbelista,CodTactica, CodProducto,PerfilGP,
			   MIN(VentaPotencial) VentaPotencialMin 
		INTO Tabla4_p
		FROM BasePotencial_auxU6C_p
		GROUP BY PKEbelista,CodTactica,CodProducto,PerfilGP

		--Se halla el promedio de la venta potencial mínima por Consultora a nivel de la Base Potencial
		SELECT PerfilGP,CodTactica, CodProducto, 0 TotalConsultoras,
			   SUM(VentaPotencialMin) VentaPotencialMin 
		INTO Tabla5_p 
		FROM Tabla4_p 
		GROUP by PerfilGP,CodTactica,CodProducto 

		--Se halla el total de consultoras por Región y Comportamiento
		SELECT PerfilGP,
			   COUNT(DISTINCT PKEbelista) TotalConsultotas  
		INTO Tabla6_p 
		FROM #InfoConsultora
		GROUP BY PerfilGP

		UPDATE A
		SET A.TotalConsultoras = B.TotalConsultotas
		FROM Tabla5_p A INNER JOIN Tabla6_p B ON A.PerfilGP=B.PerfilGP

		--Se actualiza el nuevo valor de la VentaMinima U6C
		UPDATE A
		SET A.PromVentaPotencialMin = CASE(TotalConsultoras) WHEN 0 THEN 0 ELSE ROUND((B.VentaPotencialMin*1.00/TotalConsultoras),2) END
		FROM BasePotencialU6C_p A INNER JOIN Tabla5_p B ON A.PerfilGP=B.PerfilGP
		AND A.CodProducto=B.CodProducto AND A.CodTactica=B.CodTactica

	END

	/** Actualizar las variables por GP **/

	/** GP por Región y Segmento **/
	IF (@TipoGP=1)
	BEGIN

		--Se actualiza el Ciclo de Recompra Potencial (Se busca en la Base Potencial)
		UPDATE A  
		SET A.CicloRecompraPotencial = B.CicloRecompraPotencial
		FROM ListadoConsultora A INNER JOIN BasePotencialU24C B  
		ON  A.CodProducto=B.CodProducto AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodTactica=B.CodTactica   

		--Se actualiza la Venta Potencial Mínima U6C (Se busca en la Base Potencial)
		UPDATE A  
		SET A.VentaPotencialMinU6C = B.PromVentaPotencialMin
		FROM ListadoConsultora A INNER JOIN BasePotencialU6C B  
		ON  A.CodProducto=B.CodProducto AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodTactica=B.CodTactica   

		--Se actualiza la Frecuencia U24C, Recencia U24C y Precio Óptimo GP para las consultoras que no tuvieron venta (Se busca de la Base Potencial)
		UPDATE A  
		SET A.FrecuenciaU24C = B.FrecuenciaGP, 
			RecenciaU24C = B.RecenciaGP, 
			A.PrecioOptimo = B.PrecioOptimoGP
		FROM ListadoConsultora A INNER JOIN BasePotencialU24C B  
		ON  A.CodProducto=B.CodProducto AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodTactica=B.CodTactica   
		AND A.FlagCompra = 0 --No tuvieron compra

		--Se calculan las variables del Grupo Potencial: Gatillador
		SELECT CodRegion, CodComportamientoRolling,CodTactica,
			   AVG(Gatillador) GatilladorGP,
			   AVG(FrecuenciaU24C) FrecuenciaU24CGP 
		INTO GrupoPotencial 
		FROM ListadoConsultora 
		GROUP BY CodRegion, CodComportamientoRolling,CodTactica

		--Se actualiza el Gatillador para las consultoras que no tuvieron venta 
		UPDATE A  
		SET A.Gatillador = B.GatilladorGP
		FROM ListadoConsultora A INNER JOIN GrupoPotencial B  
		ON A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling AND A.CodTactica=B.CodTactica   
		AND A.FlagCompra = 0 --No tuvieron venta

		/** Cargar a tabla de Grupo Potencial**/
		SELECT @CodPais AS CodPais, @AnioCampanaProceso AS AnioCampanaProceso, @AnioCampanaExpo AS AnioCampanaExpo,
	           @TipoARP AS TipoARP, @TipoPersonalizacion AS TipoPersonalizacion,@Perfil AS Perfil,
			   'Individual' AS TipoTactica,CodTactica,CodProducto,@TipoGP AS TipoGP,CodRegion,CodComportamientoRolling,SPACE(10) AS PerfilGP,
			   FrecuenciaGP AS FrecuenciaU24C,RecenciaGP AS RecenciaU24C,PrecioOptimoGP AS PrecioOptimo,CicloRecompraPotencial,
			   CAST(0.0 AS FLOAT) AS PromVentaPotencialMinU6C,CAST(0.0 AS FLOAT) AS Gatillador
		INTO #BasePotencial_TipoGP1
		FROM BasePotencialU24C

		UPDATE A
		SET PromVentaPotencialMinU6C = B.PromVentaPotencialMin
		FROM #BasePotencial_TipoGP1 A INNER JOIN BasePotencialU6C B ON A.CodProducto=B.CodProducto AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodTactica=B.CodTactica

		UPDATE A
		SET Gatillador = B.GatilladorGP
		FROM #BasePotencial_TipoGP1 A INNER JOIN GrupoPotencial B ON A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodTactica=B.CodTactica

		DELETE FROM BD_ANALITICO.dbo.ARP_BaseGrupoPotencial
		WHERE AnioCampanaProceso = @AnioCampanaProceso 
		AND AnioCampanaExpo = @AnioCampanaExpo
		AND CodPais = @CodPais 
		AND TipoARP = @TipoARP
		AND TipoPersonalizacion = @TipoPersonalizacion
		AND Perfil = @Perfil
		AND TipoGP = @TipoGP
		AND TipoTactica = 'Individual'

		INSERT INTO BD_ANALITICO.dbo.ARP_BaseGrupoPotencial
		(CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoARP,TipoPersonalizacion,Perfil,TipoTactica,CodTactica,CodProducto,TipoGP,CodRegion,CodComportamientoRolling,PerfilGP,
		 FrecuenciaU24C,RecenciaU24C,PrecioOptimo,CicloRecompraPotencial,VentaPotencialMinU6C,Gatillador)
        SELECT * FROM #BasePotencial_TipoGP1
	
	END

	/** GP por Perfil **/
	IF (@TipoGP=2)
	BEGIN

		--Se actualiza el Ciclo de Recompra Potencial (Se busca en la Base Potencial)
		UPDATE A  
		SET A.CicloRecompraPotencial = B.CicloRecompraPotencial
		FROM ListadoConsultora A INNER JOIN BasePotencialU24C_p B  
		ON  A.CodProducto=B.CodProducto AND A.PerfilGP=B.PerfilGP
		AND A.CodTactica=B.CodTactica   

		--Se actualiza la Venta Potencial Mínima U6C (Se busca en la Base Potencial)
		UPDATE A  
		SET A.VentaPotencialMinU6C = B.PromVentaPotencialMin
		FROM ListadoConsultora A INNER JOIN BasePotencialU6C_p B  
		ON  A.CodProducto=B.CodProducto AND A.PerfilGP=B.PerfilGP
		AND A.CodTactica=B.CodTactica   

		--Se actualiza la Frecuencia U24C, Recencia U24C y Precio Óptimo GP para las consultoras que no tuvieron venta (Se busca de la Base Potencial)
		UPDATE A  
		SET A.FrecuenciaU24C = B.FrecuenciaGP, 
			RecenciaU24C = B.RecenciaGP, 
			A.PrecioOptimo = B.PrecioOptimoGP
		FROM ListadoConsultora A INNER JOIN BasePotencialU24C_p B  
		ON  A.CodProducto=B.CodProducto AND A.PerfilGP=B.PerfilGP
		AND A.CodTactica=B.CodTactica   
		AND A.FlagCompra = 0 --No tuvieron compra

		--Se calculan las variables del Grupo Potencial: Gatillador
		SELECT PerfilGP,CodTactica,
			   AVG(Gatillador) GatilladorGP,
			   AVG(FrecuenciaU24C) FrecuenciaU24CGP 
		INTO GrupoPotencial_p 
		FROM ListadoConsultora 
		GROUP BY PerfilGP,CodTactica

		--Se actualiza el Gatillador para las consultoras que no tuvieron venta 
		UPDATE A  
		SET A.Gatillador = B.GatilladorGP
		FROM ListadoConsultora A INNER JOIN GrupoPotencial_p B  
		ON A.PerfilGP=B.PerfilGP AND A.CodTactica=B.CodTactica   
		AND A.FlagCompra = 0 --No tuvieron venta

		/** Cargar a tabla de Grupo Potencial**/
		SELECT @CodPais AS CodPais, @AnioCampanaProceso AS AnioCampanaProceso, @AnioCampanaExpo AS AnioCampanaExpo,
	           @TipoARP AS TipoARP, @TipoPersonalizacion AS TipoPersonalizacion,@Perfil AS Perfil,
			   'Individual' AS TipoTactica,CodTactica,CodProducto,@TipoGP AS TipoGP,SPACE(3) AS CodRegion,CAST(0 AS INT) AS CodComportamientoRolling,PerfilGP,
			   FrecuenciaGP AS FrecuenciaU24C,RecenciaGP AS RecenciaU24C,PrecioOptimoGP AS PrecioOptimo,CicloRecompraPotencial,
			   CAST(0.0 AS FLOAT) AS PromVentaPotencialMinU6C,CAST(0.0 AS FLOAT) AS Gatillador
		INTO #BasePotencial_TipoGP2
		FROM BasePotencialU24C_p

		UPDATE A
		SET PromVentaPotencialMinU6C = B.PromVentaPotencialMin
		FROM #BasePotencial_TipoGP2 A INNER JOIN BasePotencialU6C_p B ON A.CodProducto=B.CodProducto AND A.PerfilGP=B.PerfilGP
		AND A.CodTactica=B.CodTactica  

		UPDATE A
		SET Gatillador = B.GatilladorGP
		FROM #BasePotencial_TipoGP2 A INNER JOIN GrupoPotencial_p B ON A.PerfilGP=B.PerfilGP
		AND A.CodTactica=B.CodTactica

		DELETE FROM BD_ANALITICO.dbo.ARP_BaseGrupoPotencial
		WHERE AnioCampanaProceso = @AnioCampanaProceso 
		AND AnioCampanaExpo = @AnioCampanaExpo
		AND CodPais = @CodPais 
		AND TipoARP = @TipoARP
		AND TipoPersonalizacion = @TipoPersonalizacion
		AND Perfil = @Perfil
		AND TipoGP = @TipoGP
		AND TipoTactica = 'Individual'

		INSERT INTO BD_ANALITICO.dbo.ARP_BaseGrupoPotencial
		(CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoARP,TipoPersonalizacion,Perfil,TipoTactica,CodTactica,CodProducto,TipoGP,CodRegion,CodComportamientoRolling,PerfilGP,
		 FrecuenciaU24C,RecenciaU24C,PrecioOptimo,CicloRecompraPotencial,VentaPotencialMinU6C,Gatillador)
        SELECT * FROM #BasePotencial_TipoGP2
	
	END

	--Se actualiza la Brecha de Recompra Potencial = Recencia U24C – Ciclo Recompra Potencial
	UPDATE A  
	SET A.BrechaRecompraPotencial = A.RecenciaU24C - A.CicloRecompraPotencial
	FROM ListadoConsultora A

	UPDATE A 
	SET GAPPrecioOptimo = ROUND((B.PrecioOferta)-A.PrecioOptimo,4)
	FROM ListadoConsultora A INNER JOIN #ProductosCUC_Individual B ON A.CodProducto=B.CodProducto
	AND A.CodTactica=B.CodTactica

	/** Cálculo de Brechas - GAP Sin Motor Canibalización **/ 
	
	--Si Venta es mayor a cero en las U24C
	-- Condición 1  
	UPDATE ListadoConsultora  
	SET BrechaVenta = VentaAcumU24C  
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 
	
	-- Condición 2 - Recompró
	UPDATE ListadoConsultora  
	SET GAP = VentaAcumU6C - VentaAcumU6C_AA, 
		BrechaVenta = VentaPromU24C
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24

	-- Condición 3  -- No figura en el documento
	UPDATE ListadoConsultora  
	SET GAP = VentaAcumU6C - VentaAcumPU6C,
		BrechaVenta = VentaPromU24C
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24

	/*UPDATE #ListadoConsultora  
	SET GAP = VentaAcumU6C - VentaAcumPU6C,
		BrechaVenta = VentaPromU24C
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1*/

	-- Condición 4  
	UPDATE ListadoConsultora  
	SET BrechaVenta = VentaPotencialMinU6C
	WHERE VentaAcumU24C<=0

	/** 1.2. Bundle **/

	SELECT A.*, B.PerfilGP 
	INTO ListadoConsultora_Bundle
	FROM BD_ANALITICO.dbo.ARP_ListadoVariablesBundle A (NOLOCK) INNER JOIN #InfoConsultora B ON A.PKEbelista=B.PKEbelista

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

	/** GP por Región y Segmento **/
	IF (@TipoGP=1)
	BEGIN

		--Se actualiza El ciclo de Recompra Potencial, se busca en la Base Potencial: BASE POTENCIAL BUNDLE
		SELECT A.CodRegion,A.CodComportamientoRolling,A.CodTactica,A.CodProducto, 
			   CEILING(SUM(CicloRecompraPotencial)/COUNT(PKEbelista)) CicloRecompraPotencial,
			   AVG(PrecioMinimo) PrecioMinimo
		INTO BasePotencial_ProductoBundle
		FROM ListadoConsultora_Bundle A
		GROUP BY A.CodRegion,A.CodComportamientoRolling,A.CodTactica,A.CodProducto

		UPDATE A  
		SET A.CicloRecompraPotencial = B.CicloRecompraPotencial
		FROM ListadoConsultora_Bundle A INNER JOIN BasePotencial_ProductoBundle B ON
		A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodTactica=B.CodTactica AND A.CodProducto=b.CodProducto 

		UPDATE A  
		SET A.PrecioOptimo = B.PrecioMinimo
		FROM ListadoConsultora_Bundle A INNER JOIN BasePotencial_ProductoBundle B ON
		A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling
		AND A.CodTactica=B.CodTactica AND A.CodProducto=b.CodProducto 
		WHERE A.PrecioOptimo=0 

		/** Cargar a tabla de Grupo Potencial**/
		SELECT @CodPais AS CodPais, @AnioCampanaProceso AS AnioCampanaProceso, @AnioCampanaExpo AS AnioCampanaExpo,
	           @TipoARP AS TipoARP, @TipoPersonalizacion AS TipoPersonalizacion,@Perfil AS Perfil,
			   'Bundle' AS TipoTactica,CodTactica,CodProducto AS CodProducto,@TipoGP AS TipoGP,CodRegion,CodComportamientoRolling,SPACE(10) AS PerfilGP,
			   CAST(0.0 AS FLOAT) AS FrecuenciaU24C,CAST(0.0 AS FLOAT) AS RecenciaU24C,PrecioMinimo AS PrecioOptimo,CicloRecompraPotencial AS CicloRecompraPotencial,
			   CAST(0.0 AS FLOAT) AS PromVentaPotencialMinU6C,CAST(0.0 AS FLOAT) AS Gatillador
		INTO #BasePotencialB_TipoGP1
		FROM BasePotencial_ProductoBundle
	END

	/** GP por Perfil **/
	IF (@TipoGP=2)
	BEGIN

		--Se actualiza El ciclo de Recompra Potencial, se busca en la Base Potencial: BASE POTENCIAL BUNDLE
		SELECT A.PerfilGP,A.CodTactica,A.CodProducto, 
			   CEILING(SUM(CicloRecompraPotencial)/COUNT(PKEbelista)) CicloRecompraPotencial,
			   AVG(PrecioMinimo) PrecioMinimo
		INTO BasePotencial_ProductoBundle_p
		FROM ListadoConsultora_Bundle A
		GROUP BY A.PerfilGP,A.CodTactica,A.CodProducto

		UPDATE A  
		SET A.CicloRecompraPotencial = B.CicloRecompraPotencial
		FROM ListadoConsultora_Bundle A INNER JOIN BasePotencial_ProductoBundle_p B ON
		A.PerfilGP=B.PerfilGP
		AND A.CodTactica=B.CodTactica AND A.CodProducto=b.CodProducto 

		UPDATE A  
		SET A.PrecioOptimo = B.PrecioMinimo
		FROM ListadoConsultora_Bundle A INNER JOIN BasePotencial_ProductoBundle_p B ON
		A.PerfilGP=B.PerfilGP
		AND A.CodTactica=B.CodTactica AND A.CodProducto=b.CodProducto 
		WHERE A.PrecioOptimo=0 

		/** Cargar a tabla de Grupo Potencial**/
		SELECT @CodPais AS CodPais, @AnioCampanaProceso AS AnioCampanaProceso, @AnioCampanaExpo AS AnioCampanaExpo,
	           @TipoARP AS TipoARP, @TipoPersonalizacion AS TipoPersonalizacion,@Perfil AS Perfil,
			   'Bundle' AS TipoTactica,CodTactica,CodProducto AS CodProducto,@TipoGP AS TipoGP,SPACE(3) AS CodRegion,CAST(0 AS INT) AS CodComportamientoRolling,PerfilGP,
			   CAST(0.0 AS FLOAT) AS FrecuenciaU24C,CAST(0.0 AS FLOAT) AS RecenciaU24C,PrecioMinimo AS PrecioOptimo,CicloRecompraPotencial AS CicloRecompraPotencial,
			   CAST(0.0 AS FLOAT) AS PromVentaPotencialMinU6C,CAST(0.0 AS FLOAT) AS Gatillador
		INTO #BasePotencialB_TipoGP2
		FROM BasePotencial_ProductoBundle_p
	END

	UPDATE ListadoConsultora_Bundle
	SET FlagBRP = 1
	WHERE BrechaRecompraPotencial > 0

	UPDATE ListadoConsultora_Bundle
	SET FlagVentaU6CMenosAA  = 1
	WHERE (VentaAcumU6C-VentaAcumU6C_AA)<=0

	UPDATE ListadoConsultora_Bundle
	SET FlagVentaU6CMenosPP = 1
	WHERE (VentaAcumU6C-VentaAcumPU6C)<=0
	
	SELECT PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica,PerfilGP,CAST(0.0 AS FLOAT) GAPRegalo,
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
	INTO ListadoConsultora_TacticaBundle
	FROM ListadoConsultora_Bundle 
	GROUP BY PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica,PerfilGP

	/** Cálculo de Brechas - GAP Sin Motor de Canibalización **/ 
	
	UPDATE ListadoConsultora_TacticaBundle  
	SET BrechaVenta = VentaPromU24C
	
	--Base Potencial de Bundle
	--Se actualiza el GAP Precio Óptimo a nivel de Táctica
	
	SELECT A.PKebelista,A.CodTactica,
	       SUM(B.PrecioOferta)-SUM(DISTINCT A.PrecioOptimo) GAPPrecioOptimo 
	INTO PO_PrecioOptimo
	FROM ListadoConsultora_TacticaBundle A INNER JOIN #ProductosCUC_Bundle B ON A.CodTactica=B.CodTactica 
	GROUP BY A.PKebelista,A.CodTactica

	UPDATE A
	SET GAPPrecioOptimo = B.GAPPrecioOptimo
	FROM ListadoConsultora_TacticaBundle A INNER JOIN PO_PrecioOptimo B 
	ON A.CodTactica=B.CodTactica AND A.PKEbelista=B.PKEbelista

	UPDATE ListadoConsultora_TacticaBundle
	SET RecenciaU24C_GP = 24
	WHERE RecenciaU24C_GP = 0

	/** GP por Región y Segmento **/
	IF (@TipoGP=1)
	BEGIN

		SELECT CodRegion,CodComportamientoRolling,CodTactica, 
			   SUM(VentaPotencialMinU6C)/COUNT(DISTINCT PKEbelista) VentaPotencialMinU6C_GP, 
			   SUM(FrecuenciaU24C)/COUNT(DISTINCT PKEbelista) FrecuenciaU24C_GP,
			   /*CAST(ROUND(AVG(RecenciaU24C_GP*1.00),0) AS INT) RecenciaU24C_GP,*/
			   AVG(RecenciaU24C_GP*1.00) RecenciaU24C_GP,
			   SUM(Gatillador)/COUNT(DISTINCT PKEbelista) Gatillador_GP
		INTO BasePotencial_TacticaBundle
		FROM ListadoConsultora_TacticaBundle
		GROUP BY CodRegion,CodComportamientoRolling,CodTactica
	
		SELECT CodRegion,CodComportamientoRolling,CodTactica, 
			   SUM(GAPPrecioOptimo)/COUNT(DISTINCT PKEbelista) GAPPrecioOptimo_GP
		INTO BasePotencial_TacticaBundle_PO
		FROM ListadoConsultora_TacticaBundle
		WHERE GAPPrecioOptimo!=0
		GROUP BY CodRegion,CodComportamientoRolling,CodTactica

		--Se actualiza la Brecha Venta a nivel de Táctica
		UPDATE A  
		SET A.BrechaVenta = B.VentaPotencialMinU6C_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle B  
		ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
		AND A.BrechaVenta=0

		--Se actualiza la FrecuenciaU24C GP Base Potencial a nivel de Táctica
		UPDATE A  
		SET A.FrecuenciaU24C = B.FrecuenciaU24C_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle B  
		ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
		AND A.FrecuenciaU24C=0

		--Se actualiza la RecenciaU24C GP Base Potencial a nivel de Táctica
		UPDATE A  
		SET A.RecenciaU24C_GP = B.RecenciaU24C_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle B 
		ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
		AND A.RecenciaU24C=0

		--Se actualiza el Gatillador GP Base Potencial a nivel de Táctica
		UPDATE A  
		SET A.Gatillador = B.Gatillador_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle B  
		ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
		AND A.Gatillador=0

		--Se actualiza el GAP Precio Óptimo a nivel de Táctica
		UPDATE A  
		SET A.GAPPrecioOptimo = B.GAPPrecioOptimo_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle_PO B  
		ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
		AND A.PrecioOptimo=0

		/** Actualizo la tabla de Grupo Potencial **/
		UPDATE A
		SET FrecuenciaU24C = B.FrecuenciaU24C_GP,
		    RecenciaU24C = B.RecenciaU24C_GP,
		    PromVentaPotencialMinU6C = B.VentaPotencialMinU6C_GP,
			Gatillador = B.Gatillador_GP
		FROM #BasePotencialB_TipoGP1 A INNER JOIN BasePotencial_TacticaBundle B ON A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling 
		AND A.CodTactica=B.CodTactica

		DELETE FROM BD_ANALITICO.dbo.ARP_BaseGrupoPotencial
		WHERE AnioCampanaProceso = @AnioCampanaProceso 
		AND AnioCampanaExpo = @AnioCampanaExpo
		AND CodPais = @CodPais 
		AND TipoARP = @TipoARP
		AND TipoPersonalizacion = @TipoPersonalizacion
		AND Perfil = @Perfil
		AND TipoGP = @TipoGP
		AND TipoTactica = 'Bundle'

		INSERT INTO BD_ANALITICO.dbo.ARP_BaseGrupoPotencial
		(CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoARP,TipoPersonalizacion,Perfil,TipoTactica,CodTactica,CodProducto,TipoGP,CodRegion,CodComportamientoRolling,PerfilGP,
		 FrecuenciaU24C,RecenciaU24C,PrecioOptimo,CicloRecompraPotencial,VentaPotencialMinU6C,Gatillador)
        SELECT * FROM #BasePotencialB_TipoGP1

	END

	/** GP por Perfil **/
	IF (@TipoGP=2)
	BEGIN

		SELECT PerfilGP,CodTactica, 
			   SUM(VentaPotencialMinU6C)/COUNT(DISTINCT PKEbelista) VentaPotencialMinU6C_GP, 
			   SUM(FrecuenciaU24C)/COUNT(DISTINCT PKEbelista) FrecuenciaU24C_GP,
			   /*CAST(ROUND(AVG(RecenciaU24C_GP*1.00),0) AS INT) RecenciaU24C_GP,*/
			   AVG(RecenciaU24C_GP*1.00) RecenciaU24C_GP,
			   SUM(Gatillador)/COUNT(DISTINCT PKEbelista) Gatillador_GP
		INTO BasePotencial_TacticaBundle_p
		FROM ListadoConsultora_TacticaBundle
		GROUP BY PerfilGP,CodTactica
	
		SELECT PerfilGP,CodTactica, 
			   SUM(GAPPrecioOptimo)/COUNT(DISTINCT PKEbelista) GAPPrecioOptimo_GP
		INTO BasePotencial_TacticaBundle_PO_p
		FROM ListadoConsultora_TacticaBundle
		WHERE GAPPrecioOptimo!=0
		GROUP BY PerfilGP,CodTactica

		--Se actualiza la Brecha Venta a nivel de Táctica
		UPDATE A  
		SET A.BrechaVenta = B.VentaPotencialMinU6C_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle_p B  
		ON A.CodTactica=B.CodTactica AND A.PerfilGP=B.PerfilGP
		AND A.BrechaVenta=0

		--Se actualiza la FrecuenciaU24C GP Base Potencial a nivel de Táctica
		UPDATE A  
		SET A.FrecuenciaU24C = B.FrecuenciaU24C_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle_p B  
		ON A.CodTactica=B.CodTactica AND A.PerfilGP=B.PerfilGP 
		AND A.FrecuenciaU24C=0

		--Se actualiza la RecenciaU24C GP Base Potencial a nivel de Táctica
		UPDATE A  
		SET A.RecenciaU24C_GP = B.RecenciaU24C_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle_p B 
		ON A.CodTactica=B.CodTactica AND A.PerfilGP=B.PerfilGP  
		AND A.RecenciaU24C=0

		--Se actualiza el Gatillador GP Base Potencial a nivel de Táctica
		UPDATE A  
		SET A.Gatillador = B.Gatillador_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle_p B  
		ON A.CodTactica=B.CodTactica AND A.PerfilGP=B.PerfilGP  
		AND A.Gatillador=0

		--Se actualiza el GAP Precio Óptimo a nivel de Táctica
		UPDATE A  
		SET A.GAPPrecioOptimo = B.GAPPrecioOptimo_GP
		FROM ListadoConsultora_TacticaBundle A INNER JOIN BasePotencial_TacticaBundle_PO_p B  
		ON A.CodTactica=B.CodTactica AND A.PerfilGP=B.PerfilGP 
		AND A.PrecioOptimo=0

		/** Actualizo la tabla de Grupo Potencial **/
		UPDATE A
		SET FrecuenciaU24C = B.FrecuenciaU24C_GP,
		    RecenciaU24C = B.RecenciaU24C_GP,
		    PromVentaPotencialMinU6C = B.VentaPotencialMinU6C_GP,
			Gatillador = B.Gatillador_GP
		FROM #BasePotencialB_TipoGP2 A INNER JOIN BasePotencial_TacticaBundle_p B ON A.PerfilGP=B.PerfilGP
		AND A.CodTactica=B.CodTactica

		DELETE FROM BD_ANALITICO.dbo.ARP_BaseGrupoPotencial
		WHERE AnioCampanaProceso = @AnioCampanaProceso 
		AND AnioCampanaExpo = @AnioCampanaExpo
		AND CodPais = @CodPais 
		AND TipoARP = @TipoARP
		AND TipoPersonalizacion = @TipoPersonalizacion
		AND Perfil = @Perfil
		AND TipoGP = @TipoGP
		AND TipoTactica = 'Bundle'

		INSERT INTO BD_ANALITICO.dbo.ARP_BaseGrupoPotencial
		(CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoARP,TipoPersonalizacion,Perfil,TipoTactica,CodTactica,CodProducto,TipoGP,CodRegion,CodComportamientoRolling,PerfilGP,
		 FrecuenciaU24C,RecenciaU24C,PrecioOptimo,CicloRecompraPotencial,VentaPotencialMinU6C,Gatillador)
        SELECT * FROM #BasePotencialB_TipoGP2

	END

	UPDATE ListadoConsultora_TacticaBundle
	SET RecenciaU24C = RecenciaU24C_GP

	/** 1.3. Individual y Bundle **/

	/** Tabla consolidada **/

	CREATE TABLE ListadoConsultora_Total
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
	INSERT INTO ListadoConsultora_Total 
	SELECT PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica,
	VentaAcumU6C,VentaAcumPU6C,VentaAcumU6C_AA,VentaAcumU24C,VentaPromU24C,FrecuenciaU24C,RecenciaU24C,CicloRecompraPotencial,
	BrechaRecompraPotencial,VentaPotencialMinU6C,GAP,BrechaVenta,BrechaVenta_MC,FlagCompra,PrecioOptimo,GAPPrecioOptimo,
	GAPRegalo,NumAspeos,Gatillador,GatilladorRegalo,FrecuenciaNor,RecenciaNor,BrechaVentaNor, BrechaVenta_MCNor, 
	GAPPrecioOptimoNor,GatilladorNor,Oportunidad,Oportunidad_MC,OportunidadNor,Oportunidad_MCNor,
	Score,Score_MC,UnidadesTactica, Score_UU, Score_MC_UU,PerfilOficial,FlagSeRecomienda,FlagTop
	FROM ListadoConsultora_TacticaBundle

	--Inserto las tácticas Individual
	INSERT INTO ListadoConsultora_Total
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
	FROM ListadoConsultora
	GROUP BY PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,Antiguedad,TipoTactica,CodTactica

	/** Regalos **/
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
	FROM BaseEstadoConsultoras
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
	FROM ListadoConsultora_Total A INNER JOIN #TemporalRegalo B ON A.CodTactica=B.CodTactica

	UPDATE A
	SET A.GatilladorRegalo = SQRT(B.Gatillador)/10
	FROM ListadoConsultora_Total A INNER JOIN #GatilladorRegalos B ON A.PKEbelista=B.PKEbelista
	INNER JOIN #TacticasConRegalo C ON A.CodTactica=C.CodTactica

	UPDATE ListadoConsultora_Total
	SET GAPPrecioOptimo = GAPPrecioOptimo - GAPRegalo, 
	    Gatillador = Gatillador + GatilladorRegalo

	UPDATE A  
	SET A.UnidadesTactica = B.TotalUnidades
	FROM ListadoConsultora_Total A INNER JOIN #UnidadesTactica B ON A.CodTactica=B.CodTactica

	/** Carga tabla ListadoVariablesRFM **/
	
	DELETE 
	--FROM BDDM01.[DATAMARTANALITICO].DBO.ARP_ListadoVariablesRFM
	FROM BD_ANALITICO.dbo.ARP_ListadoVariablesRFM
	WHERE AnioCampanaProceso = @AnioCampanaProceso 
	AND AnioCampanaExpo = @AnioCampanaExpo
	AND CodPais = @CodPais 
	AND TipoARP = @TipoARP
	AND TipoPersonalizacion = @TipoPersonalizacion
	AND Perfil = @Perfil

	PRINT 'INSERT INTO BD_ANALITICO.dbo.ARP_ListadoVariablesRFM'
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
	FROM ListadoConsultora_Total




	/** Eliminar tablas temporales **/
	IF OBJECT_ID('ListadoConsultora') IS NOT NULL DROP TABLE ListadoConsultora
	IF OBJECT_ID('FSTAEBECAM') IS NOT NULL DROP TABLE FSTAEBECAM
	IF OBJECT_ID('BaseEstadoConsultoras') IS NOT NULL DROP TABLE BaseEstadoConsultoras
	IF OBJECT_ID('TEMP_FVTAPROEBECAM') IS NOT NULL DROP TABLE TEMP_FVTAPROEBECAM
	IF OBJECT_ID('FVTAPROEBECAMU24C') IS NOT NULL DROP TABLE FVTAPROEBECAMU24C
	IF OBJECT_ID('FVTAPROEBECAMU6C') IS NOT NULL DROP TABLE FVTAPROEBECAMU6C
	IF OBJECT_ID('BasePotencial_aux3') IS NOT NULL DROP TABLE BasePotencial_aux3
	IF OBJECT_ID('BasePotencial_auxU6C') IS NOT NULL DROP TABLE BasePotencial_auxU6C
	IF OBJECT_ID('BasePotencial_aux2') IS NOT NULL DROP TABLE BasePotencial_aux2
	IF OBJECT_ID('BasePotencialU6C') IS NOT NULL DROP TABLE BasePotencialU6C
	IF OBJECT_ID('BasePotencial_aux3_p') IS NOT NULL DROP TABLE BasePotencial_aux3_p
	IF OBJECT_ID('BasePotencial_auxU6C_p') IS NOT NULL DROP TABLE BasePotencial_auxU6C_p
	IF OBJECT_ID('BasePotencial_aux2_p') IS NOT NULL DROP TABLE BasePotencial_aux2_p
	IF OBJECT_ID('BasePotencialU6C_p') IS NOT NULL DROP TABLE BasePotencialU6C_p
	IF OBJECT_ID('PO_Venta') IS NOT NULL DROP TABLE PO_Venta
	IF OBJECT_ID('PO_ProdXConsultora') IS NOT NULL DROP TABLE PO_ProdXConsultora
	IF OBJECT_ID('PO_Catalogo') IS NOT NULL DROP TABLE PO_Catalogo
	IF OBJECT_ID('PrecioOptimo') IS NOT NULL DROP TABLE PrecioOptimo
	IF OBJECT_ID('PO_PrecioOptimo') IS NOT NULL DROP TABLE PO_PrecioOptimo
	IF OBJECT_ID('PO_PedidosPuros') IS NOT NULL DROP TABLE PO_PedidosPuros
	IF OBJECT_ID('PrecioOptimoFinal') IS NOT NULL DROP TABLE PrecioOptimoFinal
	IF OBJECT_ID('PO_PrecioMinimo') IS NOT NULL DROP TABLE PO_PrecioMinimo
	IF OBJECT_ID('BasePotencial_aux24C') IS NOT NULL DROP TABLE BasePotencial_aux24C
	IF OBJECT_ID('BasePotencial_aux24C2') IS NOT NULL DROP TABLE BasePotencial_aux24C2
	IF OBJECT_ID('BasePotencial_aux24C3') IS NOT NULL DROP TABLE BasePotencial_aux24C3
	IF OBJECT_ID('BasePotencialU24C') IS NOT NULL DROP TABLE BasePotencialU24C
	IF OBJECT_ID('BaseCicloRecompra') IS NOT NULL DROP TABLE BaseCicloRecompra
	IF OBJECT_ID('Tabla1') IS NOT NULL DROP TABLE Tabla1
	IF OBJECT_ID('Tabla2') IS NOT NULL DROP TABLE Tabla2
	IF OBJECT_ID('Tabla3') IS NOT NULL DROP TABLE Tabla3
	IF OBJECT_ID('Tabla4') IS NOT NULL DROP TABLE Tabla4
	IF OBJECT_ID('Tabla5') IS NOT NULL DROP TABLE Tabla5
	IF OBJECT_ID('Tabla6') IS NOT NULL DROP TABLE Tabla6
	IF OBJECT_ID('BasePotencial_aux24C_p') IS NOT NULL DROP TABLE BasePotencial_aux24C_p
	IF OBJECT_ID('BasePotencial_aux24C2_p') IS NOT NULL DROP TABLE BasePotencial_aux24C2_p
	IF OBJECT_ID('BasePotencial_aux24C3_p') IS NOT NULL DROP TABLE BasePotencial_aux24C3_p
	IF OBJECT_ID('BasePotencialU24C_p') IS NOT NULL DROP TABLE BasePotencialU24C_p
	IF OBJECT_ID('BaseCicloRecompra_p') IS NOT NULL DROP TABLE BaseCicloRecompra_p
	IF OBJECT_ID('Tabla1_p') IS NOT NULL DROP TABLE Tabla1_p
	IF OBJECT_ID('Tabla2_p') IS NOT NULL DROP TABLE Tabla2_p
	IF OBJECT_ID('Tabla3_p') IS NOT NULL DROP TABLE Tabla3_p
	IF OBJECT_ID('Tabla4_p') IS NOT NULL DROP TABLE Tabla4_p
	IF OBJECT_ID('Tabla5_p') IS NOT NULL DROP TABLE Tabla5_p
	IF OBJECT_ID('Tabla6_p') IS NOT NULL DROP TABLE Tabla6_p
	IF OBJECT_ID('GrupoPotencial') IS NOT NULL DROP TABLE GrupoPotencial
	IF OBJECT_ID('GrupoPotencial_p') IS NOT NULL DROP TABLE GrupoPotencial_p
	IF OBJECT_ID('ListadoConsultora_Bundle') IS NOT NULL DROP TABLE ListadoConsultora_Bundle
	IF OBJECT_ID('BasePotencial_ProductoBundle') IS NOT NULL DROP TABLE BasePotencial_ProductoBundle
	IF OBJECT_ID('BasePotencial_ProductoBundle_p') IS NOT NULL DROP TABLE BasePotencial_ProductoBundle_p
	IF OBJECT_ID('ListadoConsultora_TacticaBundle') IS NOT NULL DROP TABLE ListadoConsultora_TacticaBundle
	IF OBJECT_ID('BasePotencial_TacticaBundle') IS NOT NULL DROP TABLE BasePotencial_TacticaBundle
	IF OBJECT_ID('BasePotencial_TacticaBundle_PO') IS NOT NULL DROP TABLE BasePotencial_TacticaBundle_PO
	IF OBJECT_ID('BasePotencial_TacticaBundle_p') IS NOT NULL DROP TABLE BasePotencial_TacticaBundle_p
	IF OBJECT_ID('BasePotencial_TacticaBundle_PO_p') IS NOT NULL DROP TABLE BasePotencial_TacticaBundle_PO_p
	IF OBJECT_ID('ListadoConsultora_Total ') IS NOT NULL DROP TABLE ListadoConsultora_Total 

END

/** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,TipoGP,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,NULL,@Perfil,NULL,@TipoGP,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

END

