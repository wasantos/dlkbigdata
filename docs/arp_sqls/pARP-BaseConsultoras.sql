CREATE PROCEDURE [dbo].[pARP_BaseConsultoras] 
@CodPais VARCHAR(2), @AnioCampanaProceso CHAR(6),@Perfil VARCHAR(1), @TipoPersonalizacion CHAR(3)
AS
BEGIN
Set nocount on;
/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @Perfil							VARCHAR(1)

SET @CodPais							= 'MX'		-- Código de país
SET @AnioCampanaProceso					= '201705'	-- Última campaña cerrada
SET @Perfil								= 'X'		-- Número de Perfil | 'X': Sin Perfil*/


DECLARE @AnioCampanaProceso_Menos2		CHAR(6)
DECLARE @AnioCampanaProceso_Menos3		CHAR(6)
DECLARE @AnioCampanaProceso_Menos4		CHAR(6)
DECLARE @AnioCampanaProceso_Menos5		CHAR(6)

SET @AnioCampanaProceso_Menos2 = dbo.CalculaAnioCampana(@AnioCampanaProceso, -2) 
SET @AnioCampanaProceso_Menos3 = dbo.CalculaAnioCampana(@AnioCampanaProceso, -3) 
SET @AnioCampanaProceso_Menos4 = dbo.CalculaAnioCampana(@AnioCampanaProceso, -4) 
SET @AnioCampanaProceso_Menos5 = dbo.CalculaAnioCampana(@AnioCampanaProceso, -5) 

/** Inicio: Variables Log **/
DECLARE @FechaInicio 					DATETIME 
DECLARE @Procedimiento					VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_BaseConsultoras'
/** Fin: Variables Log **/

Print '=============================Base Consultoras ================================'
Print 'Perfil: ' + @Perfil

/** 1. Sin Perfil **/
IF (@Perfil='X')
BEGIN

	/** 1.1. Base de Consultoras Nuevas **/

	--Consultoras Nuevas en su 1er, 2do, 3er y 4to pedido

	CREATE TABLE #BaseConsultorasNuevas (PKEbelista INT )

	IF @TipoPersonalizacion in ('ODD','SR')
	BEGIN
	
	INSERT INTO #BaseConsultorasNuevas
	SELECT DISTINCT A.PKEbelista 
	--FROM FSTAEBECAMC01_VIEW A 
	FROM DWH_ANALITICO.dbo.DWH_FSTAEBECAM A (NOLOCK)
	--INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista
	INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA B (NOLOCK) ON A.PKEbelista=B.PKEbelista AND A.CodPais = B.CodPais
	WHERE B.AnioCampanaIngreso BETWEEN @AnioCampanaProceso_Menos3 AND @AnioCampanaProceso
	AND A.CodPais=@CodPais AND A.PKEBELISTA NOT IN (SELECT PKEBELISTA FROM BD_ANALITICO.dbo.MDL_PerfilOutput (NOLOCK)
	WHERE CodPais= @CodPais AND AnioCampanaProceso=@AnioCampanaProceso)
	
	END

	ELSE 
	
	BEGIN

	INSERT INTO #BaseConsultorasNuevas
	SELECT DISTINCT A.PKEbelista 
	--FROM FSTAEBECAMC01_VIEW A 
	FROM DWH_ANALITICO.dbo.DWH_FSTAEBECAM A (NOLOCK)
	--INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista
	INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA B (NOLOCK) ON A.PKEbelista=B.PKEbelista AND B.CodPais=@CodPais
	WHERE B.AnioCampanaIngreso BETWEEN @AnioCampanaProceso_Menos3 AND @AnioCampanaProceso
	AND A.CodPais=@CodPais
	END 

	/** 1.2. Base de Consultoras Establecidas **/

	---Base de Consultoras: Aquellas con pedidos en las 3 últimas campanas   
	SELECT DISTINCT PKEbelista 
	INTO #BaseConsultoras1 
	--FROM FVTAEBECAMC01_18  
	FROM DWH_ANALITICO.dbo.DWH_FVTAPROEBECAM A (NOLOCK)
	INNER JOIN DWH_ANALITICO.dbo.DWH_DTIPOOFERTA B (NOLOCK) ON A.PKTipoOferta=B.PKTipoOferta AND B.CodPais=@CodPais
	WHERE AnioCampana BETWEEN @AnioCampanaProceso_Menos2 AND @AnioCampanaProceso 
	AND A.CodPais=@CodPais
	AND B.CodTipoProfit = '01' 
	AND AnioCampana=AnioCampanaRef  
	GROUP BY PKEbelista  
	HAVING SUM(RealVTAMNNeto)>0  

	--Consultoras Nuevas en su 5to y 6to pedido
	SELECT DISTINCT A.PKEbelista 
	INTO #BaseConsultoras2 
	--FROM FSTAEBECAMC01_VIEW A 
	FROM DWH_ANALITICO.dbo.DWH_FSTAEBECAM A (NOLOCK)
	--INNER JOIN DEBELISTA B ON A.PKEbelista=B.PKEbelista 
	INNER JOIN DWH_ANALITICO.dbo.DWH_DEBELISTA B (NOLOCK) ON A.PKEbelista=B.PKEbelista AND B.CodPais=@CodPais
	WHERE B.AnioCampanaIngreso BETWEEN @AnioCampanaProceso_Menos5 AND @AnioCampanaProceso_Menos4  
	AND A.CodPais=@CodPais

	--Se agregan las consultoras nuevas en su 5to y 6to pedido a las establecidas
	SELECT T.pkebelista 
	INTO #BaseConsultoras3 
	FROM (
	SELECT PKEbelista FROM #BaseConsultoras1
	UNION 
	SELECT PKEbelista FROM #BaseConsultoras2)T

	SELECT PKEbelista 
	INTO #BaseConsultorasEstablecidas 
	FROM #BaseConsultoras3 
	WHERE PKEbelista 
	NOT IN (SELECT PkEbelista FROM #BaseConsultorasNuevas) 

	SELECT PKEbelista,TipoBase 
	INTO #BaseConsultorasFinal 
	FROM (
	SELECT A.PKEbelista,'N' TipoBase FROM #BaseConsultorasNuevas A
	UNION ALL
	SELECT B.PKEbelista,'E' TipoBase FROM #BaseConsultorasEstablecidas B) x

	-- Se obtiene la Información Actual de las Consultoras, Región, Segmento 
	SELECT @CodPais AS CodPais,A.PKEbelista,A.CodEbelista, D.CodRegion,B.CodComportamientoRolling, D.DesRegion,D.CodZona,D.DesZona,
	 dbo.DiffANIOCampanas(@AnioCampanaProceso, A.AnioCampanaIngreso)+1 Antiguedad, C.TipoBase 
	INTO #InfoConsultora  
	--FROM DEBELISTA A 
	FROM DWH_ANALITICO.dbo.DWH_DEBELISTA A (NOLOCK)
	--INNER JOIN FSTAEBECAMC01_VIEW B ON A.PKEbelista=B.PKEbelista AND B.AnioCampana=@AnioCampanaProceso  
	INNER JOIN DWH_ANALITICO.dbo.DWH_FSTAEBECAM B (NOLOCK) ON A.PKEbelista=B.PKEbelista AND B.AnioCampana=@AnioCampanaProceso AND B.CodPais=@CodPais
	INNER JOIN #BaseConsultorasFinal C ON A.PKEbelista=C.PKEbelista
	--INNER JOIN DGEOGRAFIACAMPANA D ON B.PKTerritorio=D.PKTerritorio AND D.AnioCampana=B.AnioCampana
	INNER JOIN DWH_ANALITICO.dbo.DWH_DGEOGRAFIACAMPANA D (NOLOCK) ON B.PKTerritorio=D.PKTerritorio AND D.AnioCampana=B.AnioCampana AND D.CodPais=@CodPais
	--INNER JOIN DGEOGRAFIA E ON B.PKTerritorio=E.PKTerritorio
	WHERE A.CodPais=@CodPais

END
/** 2. Con Perfil **/
ELSE
BEGIN

	SELECT PKEbelista,'E' AS TipoBase 
	  INTO #BaseConsultorasFinal_p
	  FROM BD_ANALITICO.dbo.MDL_PerfilOutput (NOLOCK)
	 WHERE CodPais=@CodPais
	   AND AnioCampanaProceso=@AnioCampanaProceso
	   AND Perfil=CAST(@Perfil AS INT)


	   

	-- Se obtiene la Información Actual de las Consultoras, Región, Segmento 
	SELECT @CodPais AS CodPais,A.PKEbelista,A.CodEbelista, D.CodRegion,B.CodComportamientoRolling, D.DesRegion,D.CodZona,D.DesZona,
	 dbo.DiffANIOCampanas(@AnioCampanaProceso, A.AnioCampanaIngreso)+1 Antiguedad,C.TipoBase 
	INTO #InfoConsultora_p  
	--FROM DEBELISTA A 
	FROM DWH_ANALITICO.dbo.DWH_DEBELISTA A (NOLOCK)
	--INNER JOIN FSTAEBECAMC01_VIEW B ON A.PKEbelista=B.PKEbelista AND B.AnioCampana=@AnioCampanaProceso  
	INNER JOIN DWH_ANALITICO.dbo.DWH_FSTAEBECAM B (NOLOCK) ON A.PKEbelista=B.PKEbelista AND B.AnioCampana=@AnioCampanaProceso AND B.CodPais=@CodPais
	INNER JOIN #BaseConsultorasFinal_p C ON A.PKEbelista=C.PKEbelista
	--INNER JOIN DGEOGRAFIACAMPANA D ON B.PKTerritorio=D.PKTerritorio AND D.AnioCampana=B.AnioCampana
	INNER JOIN DWH_ANALITICO.dbo.DWH_DGEOGRAFIACAMPANA D (NOLOCK) ON B.PKTerritorio=D.PKTerritorio AND D.AnioCampana=B.AnioCampana AND D.CodPais=@CodPais
	--INNER JOIN DGEOGRAFIA E ON B.PKTerritorio=E.PKTerritorio
	WHERE A.CodPais=@CodPais



END

DELETE BD_ANALITICO.dbo.ARP_BaseConsultoras 
WHERE CodPais=@CodPais 
AND AnioCampanaProceso=@AnioCampanaProceso
AND Perfil=@Perfil

	

IF (@Perfil='X')
BEGIN
	
	INSERT INTO BD_ANALITICO.dbo.ARP_BaseConsultoras 
	(CodPais,AnioCampanaProceso,PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,TipoARP,Antiguedad,Perfil)
	SELECT CodPais,@AnioCampanaProceso AS AnioCampanaProceso,PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,
	TipoBase AS TipoARP,Antiguedad,@Perfil AS Perfil
	FROM #InfoConsultora

END
ELSE
BEGIN

	INSERT INTO BD_ANALITICO.dbo.ARP_BaseConsultoras 
	(CodPais,AnioCampanaProceso,PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,TipoARP,Antiguedad,Perfil)
	SELECT CodPais,@AnioCampanaProceso AS AnioCampanaProceso,PKEbelista,CodEbelista,CodRegion,CodComportamientoRolling,
	TipoBase AS TipoARP,Antiguedad,@Perfil AS Perfil
	FROM #InfoConsultora_p

END

/** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,TipoGP,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,NULL,NULL,NULL,NULL,NULL,@Perfil,NULL,NULL,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

set nocount off;
END


