

CREATE PROCEDURE [dbo].[pARP_ReglasNegocio_ODD] @CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoARP CHAR(1)
,@FlagMC INT, @Perfil VARCHAR(1)
AS
BEGIN 

/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoARP						CHAR(1)
--DECLARE @FlagCarga					INT
DECLARE @Perfil							VARCHAR(1)
DECLARE @FlagMC							INT

SET @CodPais				= 'EC'		-- Codigo de país
SET @AnioCampanaProceso		= '201704'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201706'	-- Campaña de Venta
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
--SET @FlagCarga		    = 1			-- 1: Carga/Personalización | 0: Estimación
SET @FlagMC					= 1			-- 1: Con Motor de Canibalización | 0: Sin Motor de Canibalización
SET @Perfil					='6'		-- Número de Perfil | 'X': Sin Perfil*/

DECLARE @NumEspaciosFijos				INT
DECLARE @NumEspaciosLibres				INT
DECLARE @NumEspaciosTop					INT
DECLARE @NumEspacios					INT

/** Inicio: Variables Log **/
DECLARE @FechaInicio 	DATETIME 
DECLARE @Procedimiento	VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_ReglasNegocio_ODD'
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
--IF (@FlagCarga = 1) 
--BEGIN

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,CodVinculoOF
	INTO ListadoProductos 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='ODD'
	AND Perfil=@Perfil

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta 
	INTO ListadoRegalos
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='ODD'
	AND Perfil=@Perfil

	--Se guardan el números de espacios forzados
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio 
	INTO EspaciosForzados  
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados
	FROM BD_ANALITICO.dbo.ARP_EspaciosForzados (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='ODD' AND Perfil=@Perfil

	--Se guarda el números de espacios total
	SELECT DISTINCT AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios 
	--FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='ODD' AND Perfil=@Perfil

--END

/** Estimación **/
/*ELSE 
BEGIN

    --Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop,LimUnidades,FlagUltMinuto,Agrupador
	INTO ListadoProductos 
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta>0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='ODD' AND Perfil=@Perfil

	--Se eligen los regalos para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta 
	INTO ListadoRegalos
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND PrecioOferta=0
	AND TipoARP=@TipoARP AND TipoPersonalizacion='ODD' AND Perfil=@Perfil

	--Se Guardan el números de espacios
	SELECT Marca,Categoria,TipoForzado,VinculoEspacio 
	INTO EspaciosForzados  
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_EspaciosForzados_Est
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='ODD' AND Perfil=@Perfil

	--Se guarda el numeros de espacios
	SELECT DISTINCT AnioCampanaExpo AnioCampanaExpo,Espacios,EspaciosTop 
	INTO CampaniaExpoEspacios 
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='ODD' AND Perfil=@Perfil

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop 
	INTO #ProductosTotales
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	WHERE CODPAIS=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo = @AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='ODD' AND Perfil=@Perfil

END*/

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
INTO #ListadoConsultora
--FROM BDDM01.[DATAMARTANALITICO].[dbo].[ARP_ListadoProbabilidades] 
FROM BD_ANALITICO.dbo.ARP_ListadoProbabilidades (NOLOCK)
WHERE CodPais=@CodPais 
AND AnioCampanaProceso=@AnioCampanaProceso 
AND AnioCampanaExpo=@AnioCampanaExpo 
AND TipoARP=@TipoARP 
AND TipoPersonalizacion='ODD'
AND FlagMC=@FlagMC
AND Perfil=@Perfil

/** Establecidas **/
IF (@TipoARP='E')
BEGIN

	SELECT DISTINCT PKEbelista,CodTactica,Probabilidad,
		   ROW_NUMBER()OVER(PARTITION BY PkEbelista ORDER BY Probabilidad DESC) AS Posicion 
	INTO #Temporal_ODD
	FROM #ListadoConsultora
	GROUP BY PKEbelista,CodTactica,Probabilidad
	ORDER BY PKEbelista,Probabilidad DESC

	SELECT DISTINCT PKEbelista,CodTactica,Probabilidad, 0 Orden 
	INTO #ListaRecomendadosODD
	FROM #Temporal_ODD
	WHERE Posicion<=@NumEspacios

	SELECT PKEbelista,CodTactica,Probabilidad,
		   ROW_NUMBER()OVER(PARTITION BY PkEbelista ORDER BY Probabilidad DESC) AS Orden 
	INTO #ListadoODD
	FROM #ListaRecomendadosODD

	IF EXISTS(SELECT * FROM BD_ANALITICO.dbo.ARP_Parametros_DiaInicio WHERE CodPais=@CodPais AND AnioCampanaVenta=@AnioCampanaExpo
	AND TipoPersonalizacion='ODD' AND Estado=1)
	BEGIN

		INSERT INTO #ListadoInterfazFinal 
		SELECT DISTINCT @CodPais CodPais,'ODD' Tipo,@AnioCampanaExpo AnioCampanaVenta,CodEbelista,
	--	CodProducto,CodSAP,CodVenta,'IDP'Portal,D.DiaInicio,0 DiaFin, A.Orden,0 FlagManual,'E' TipoARP,
		CodProducto,CodSAP,CodVenta,'IDP'Portal,D.DiaInicio,0 DiaFin, D.OrdenxDia ,0 FlagManual,'E' TipoARP,
		0 CodVinculo, 0.0 PPU,B.LimUnidades, B.FlagUltMinuto, @Perfil Perfil
		FROM #ListadoODD A 
		INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1
		--INNER JOIN DEBELISTA C ON A.PKEbelista=C.PKEbelista
		INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA C (NOLOCK) ON A.PKEbelista=C.PKEbelista AND C.CodPais=@CodPais
		INNER JOIN BD_ANALITICO.dbo.ARP_Parametros_DiaInicio D (NOLOCK) ON A.Orden = D.Orden AND
	    D.CodPais=@CodPais AND D.AnioCampanaVenta=@AnioCampanaExpo AND D.TipoPersonalizacion='ODD' AND D.Estado=1 and TipoARP = @TipoARP

		END
	ELSE
	BEGIN

		INSERT INTO #ListadoInterfazFinal 
		SELECT DISTINCT @CodPais CodPais,'ODD' Tipo,@AnioCampanaExpo AnioCampanaVenta,CodEbelista,
		CodProducto,CodSAP,CodVenta,'IDP'Portal,CASE 
		WHEN Orden=1 then 0 
		WHEN Orden=2 then -1 
		WHEN Orden=3 then 1 
		WHEN Orden=4 then 2 
		WHEN Orden=5 then 3 
		WHEN Orden=6 then 4 
		END as DiaIni,0 DiaFin, Orden,0 FlagManual,'E' TipoARP,
		0 CodVinculo, 0.0 PPU,B.LimUnidades, B.FlagUltMinuto,@Perfil Perfil
		FROM #ListadoODD A 
		INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1
		--INNER JOIN DEBELISTA C ON A.PKEbelista=C.PKEbelista
		INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA C (NOLOCK) ON A.PKEbelista=C.PKEbelista AND C.CodPais=@CodPais

	END

END

/** Nuevas **/
IF (@TipoARP='N')
BEGIN

/*Revisar porque una oferta tiene dos probabilidades*/
	--SELECT PKEbelista,CodTactica,Probabilidad,
	--       ROW_NUMBER()OVER(partition by PkEbelista ORDER BY Probabilidad DESC) AS Posicion
	--INTO #Temporal_ODD_N
	--FROM #ListadoConsultora
	----WHERE Probabilidad!=0
	--GROUP BY PKEbelista,CodTactica,Probabilidad
	--ORDER BY PKEbelista,Probabilidad DESC

	--DROP TABLE #Temporal_ODD_N
	SELECT  PKEbelista,CodTactica,  Probabilidad,
	       ROW_NUMBER()OVER(partition by PkEbelista ORDER BY Probabilidad DESC) AS Posicion 
	INTO #Temporal_ODD_N
	FROM (
	SELECT PKEbelista,CodTactica,MAX(Probabilidad) AS Probabilidad
	FROM #ListadoConsultora
	--WHERE Probabilidad!=0
	GROUP BY PKEbelista,CodTactica ) AS T
	ORDER BY PKEbelista,Probabilidad DESC

	SELECT PKEbelista,CodTactica,Probabilidad,0 Prioridad, 0 Orden 
	INTO #ListaRecomendadosODD_N
	FROM #Temporal_ODD_N
	WHERE Posicion<=@NumEspacios

	SELECT PKEbelista,CodTactica,Probabilidad,
		   ROW_NUMBER()OVER(partition by PkEbelista ORDER BY Probabilidad DESC) AS Orden 
	INTO #ListadoODD_N
	FROM #ListaRecomendadosODD_N
	ORDER BY pkEbelista,CodTactica

	Print '#ListadoInterfaz'
	SELECT DISTINCT C.CodEbelista,B.CodProducto,B.CodSAP,B.CodVenta,Orden,0 CodVinculo,0 PPU,B.LimUnidades,B.FlagUltMinuto 
	INTO #ListadoInterfaz
	FROM #ListadoODD_N A 
	INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1
	--INNER JOIN DEBELISTA C ON A.PKEbelista=C.PKEbelista
	INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA C (NOLOCK) ON A.PKEbelista=C.PKEbelista AND C.CodPais=@CodPais
	
	Print 'Inicio======================Agregacion de Consultora(XXXXXXXX) Dummy=============================================='
	--Inicio======================Agregacion de Consultora(XXXXXXXX) Dummy==============================================
	-- Fecha:20170511
	Insert into #ListadoInterfaz
		SELECT DISTINCT A.CodEbelista,B.CodProducto,B.CodSAP,B.CodVenta,Orden,0 CodVinculo,0 PPU,B.LimUnidades,B.FlagUltMinuto 
	FROM (
					SELECT CodEbelista ,  CodTactica, ProbabilidadCompra,
					ROW_NUMBER()OVER( ORDER BY ProbabilidadCompra DESC) AS Orden  
					FROM (
								select  'XXXXXXXXX' AS CodEbelista ,  CodTactica, Max(ProbabilidadCompra) as ProbabilidadCompra
								from  ARP_ListadoVariablesProductos  
								where codpais = @CodPais
								AND ANIOCAMPANAPROCESO  = @AnioCampanaProceso
								AND AnioCampanaExpo = @AnioCampanaExpo
								AND TIPOPERSONALIZACION = 'ODD'
								GROUP BY  CodTactica 
						) AS T 
			) A INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1 


	--SELECT DISTINCT A.CodEbelista,B.CodProducto,B.CodSAP,B.CodVenta,Orden,0 CodVinculo,0 PPU,B.LimUnidades,B.FlagUltMinuto 
	--FROM (
	--	select  'XXXXXXXX' AS CodEbelista ,  CodTactica,ProbabilidadCompra,
	--	ROW_NUMBER()OVER( ORDER BY ProbabilidadCompra DESC) AS Orden  
	--	from  ARP_ListadoVariablesProductos  
	--	where codpais = @CodPais
	--	AND ANIOCAMPANAPROCESO  = @AnioCampanaProceso
	--	AND AnioCampanaExpo = @AnioCampanaExpo
	--	AND TIPOPERSONALIZACION = 'ODD'
	--	GROUP BY  CodTactica,ProbabilidadCompra ) A INNER JOIN #ProductosCUC B ON A.CodTactica=B.CodTactica AND B.IndicadorPadre=1 
	--Fin=========================Agregacion de Consultora(XXXXXXXX) Dummy==============================================

	IF EXISTS(SELECT * FROM BD_ANALITICO.dbo.ARP_Parametros_DiaInicio (NOLOCK) WHERE CodPais=@CodPais AND AnioCampanaVenta=@AnioCampanaExpo
	AND TipoPersonalizacion='ODD' AND Estado=1)
	BEGIN

		INSERT INTO #ListadoInterfazFinal    
		SELECT DISTINCT @CodPais CodPais,'ODD' Tipo,@AnioCampanaExpo AnioCampanaVenta,
		--CodEbelista,CodProducto,CodSAP,CodVenta,'IDP'Portal,D.DiaInicio,0 DiaFin, A.Orden,0 FlagManual,'N' TipoARP,CodVinculo, PPU,LimUnidades,
		CodEbelista,CodProducto,CodSAP,CodVenta,'IDP'Portal,D.DiaInicio,0 DiaFin, D.OrdenxDia, 0 FlagManual,'N' TipoARP,CodVinculo, PPU,LimUnidades,
		FlagUltMinuto, @Perfil Perfil 
		FROM #ListadoInterfaz​ A INNER JOIN BD_ANALITICO.dbo.ARP_Parametros_DiaInicio D ON A.Orden = D.Orden AND
		 D.CodPais=@CodPais AND D.AnioCampanaVenta=@AnioCampanaExpo AND D.TipoPersonalizacion='ODD' AND D.Estado=1 and TipoARP = @TipoARP
	    -- D.CodPais=@CodPais AND D.AnioCampanaVenta=@AnioCampanaExpo AND D.TipoPersonalizacion='ODD' AND D.Estado=1

		--SELECT * FROM BD_ANALITICO.dbo.ARP_Parametros_DiaInicio
		--where CODPAIS = 'DO' AND AnioCampanaVenta = '201707'

	END
	ELSE
	BEGIN

		INSERT INTO #ListadoInterfazFinal
		SELECT DISTINCT @CodPais CodPais,'ODD' Tipo,@AnioCampanaExpo AnioCampanaVenta,
		CodEbelista,CodProducto,CodSAP,CodVenta,'IDP'Portal,CASE
		WHEN Orden=1 then 0 
		WHEN Orden=2 then -1 
		WHEN Orden=3 then 1 
		WHEN Orden=4 then 2 
		WHEN Orden=5 then 3 
		WHEN Orden=6 then 4 
		END AS DiaIni,0 DiaFin, Orden,0 FlagManual,'N' TipoARP,CodVinculo, PPU,LimUnidades,FlagUltMinuto,
		@Perfil Perfil
		FROM #ListadoInterfaz​
	
	END
END

/** Carga - Personalización **/
--IF (@FlagCarga = 1)
--BEGIN
			
	--DELETE BDDM01.DATAMARTANALITICO.dbo.ARP_OfertaPersonalizadaC01
	--DELETE BD_ANALITICO.dbo.ARP_OfertaPersonalizadaC01
	--WHERE CodPais=@CodPais AND AnioCampanaVenta=@AnioCampanaExpo and TipoARP=@TipoARP AND TipoPersonalizacion='ODD'

	--INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizada_Pruebas
	--INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizadaC01
	INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizadaC01 
	(CodPais,TipoPersonalizacion,AnioCampanaVenta,CodEbelista,CodCUC,CodSAP,CodVenta,ZonaPortal,DiaInicio,DiaFin,Orden,FlagManual,
	TipoARP,CodVinculo,PPU,LimUnidades,FlagUltMinuto,Perfil)
	SELECT * FROM #ListadoInterfazFinal

	--Generación de la Interfaz

	--INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizada_Pruebas_interfaz
	--INSERT INTO BD_ANALITICO.dbo.ARP_OfertaPersonalizada_Temporal
	--(CodPais,TipoPersonalizacion,AnioCampanaVenta,CodEbelista,CodCUC,CodSAP,CodVenta,ZonaPortal,DiaInicio,DiaFin,Orden,
	--CodVinculo,PPU,LimUnidades,FlagUltMinuto)
	--SELECT CodPais,Tipo,AnioCampanaVenta,rtrim(CodEbelista) CodEbelista,rtrim(CodProducto) CodCUC,
	--       rtrim(CodSAP) CodSAP,rtrim(CodVenta) CodVenta,Portal,DiaInicio,DiaFin,Orden,CodVinculo,PPU,LimUnidades,FlagUltMinuto
	--FROM #ListadoInterfazFinal

--END

/** Estimación **/
/*ELSE 
BEGIN

	--Se eligen productos sin regalo para los cálculos
	SELECT DISTINCT TipoTactica,CodTactica,CodCUC,Unidades,PrecioOferta,CodVenta,IndicadorPadre,FlagTop 
	INTO #ProductosTotales
	FROM BDDM01.DATAMARTANALITICO.dbo.ARP_Parametros_Est
	WHERE CodPais=@CodPais and AnioCampanaProceso=@AnioCampanaProceso and AnioCampanaExpo=@AnioCampanaExpo AND TipoARP=@TipoARP
	AND TipoPersonalizacion='ODD'

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
	FROM #ProductosTotales A INNER JOIN DPRODUCTO B ON A.CodCUC=B.CodCUC
	WHERE B.DesProductoCUC IS NOT NULL
	GROUP BY B.CodCUC,TipoTactica,CodTactica,CodVenta,Unidades,A.PrecioOferta,IndicadorPadre 

	--DELETE BDDM01.DATAMARTANALITICO.dbo.ARP_TotalProductosEstimados
	--WHERE codpais=@Codpais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo
	--AND TipoARP=@TipoARP AND AND TipoPersonalizacion='ODD'

	--INSERT INTO BDDM01.DATAMARTANALITICO.dbo.ARP_TotalProductosEstimados
	SELECT @Codpais,'ODD' AS TipoPersonalizacion,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,B.TipoTactica,A.CodTactica,
	       B.DesMarca,B.DesCategoria,B.DesTipoSolo,B.CodProducto,DesProducto,B.Unidades,
		   SUM(A.TotalConsultoras) TotalConsultoras
	FROM #TotalConsultorasTactica A INNER JOIN #ProductosCUCTotales B ON A.CodTactica=B.CodTactica
	GROUP BY B.TipoTactica,A.CodTactica,B.DesMarca,B.DesCategoria,B.DesTipoSolo,B.CodProducto,DesProducto,B.Unidades
		
END*/

/** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,TipoGP,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,'ODD',@TipoARP,NULL,@FlagMC,@Perfil,NULL,NULL,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

END


