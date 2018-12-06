CREATE PROCEDURE [dbo].[pARP_MotorCanibalizacion] @CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoPersonalizacion CHAR(3),@TipoARP CHAR(1),
@FlagMC INT, @Perfil VARCHAR(1), @TipoGP INT
AS
BEGIN 

/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoPersonalizacion			CHAR(3) 
DECLARE @TipoARP						CHAR(1)
--DECLARE @FlagCarga					INT
DECLARE @FlagMC							INT
DECLARE @Perfil							VARCHAR(1)
DECLARE @TipoGP							INT

SET @CodPais				= 'CO'		-- Código de país
SET @AnioCampanaProceso		= '201701'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201704'	-- Campaña de Venta
SET @TipoPersonalizacion	= 'OPT'		-- 'OPT','ODD','SR'
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
--SET @FlagCarga			= 1			-- 1: Carga/Personalización | 0: Estimación
SET @FlagMC					= 1			-- 1: Con Motor de Canibalizacion | 0: Sin Motor de Canibalizacion
SET @Perfil					= 'X'		-- Número de Perfil | 'X': Sin Perfil
SET @TipoGP					= 2			-- 1: Segmento y Región | 2: Perfil*/

/** Inicio: Variables Log **/
DECLARE @FechaInicio 					DATETIME 
DECLARE @Procedimiento					VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_MotorCanibalizacion'
/** Fin: Variables Log **/

/** Establecidas **/
IF (@TipoARP='E')
BEGIN
	PRINT '#ListadoConsultoraTotal'
	--Leo la tabla de ListadoVariablesRFM
	SELECT A.* , B.Perfil AS PerfilGP
	INTO #ListadoConsultoraTotal
	--FROM BDDM01.[DATAMARTANALITICO].DBO.ARP_ListadoVariablesRFM
	FROM BD_ANALITICO.dbo.ARP_ListadoVariablesRFM A (NOLOCK) LEFT JOIN BD_ANALITICO.dbo.MDL_PerfilOutput B (NOLOCK) ON 
	A.PKEbelista=B.PKEbelista AND B.CodPais=@CodPais AND B.AnioCampanaProceso=@AnioCampanaProceso
	WHERE A.AnioCampanaProceso = @AnioCampanaProceso 
	AND AnioCampanaExpo = @AnioCampanaExpo
	AND A.CodPais = @CodPais 
	AND A.TipoARP = @TipoARP
	AND A.TipoPersonalizacion = @TipoPersonalizacion
	AND A.Perfil = @Perfil

	PRINT '#ListadoConsultoraIndividual'
	SELECT * 
	INTO #ListadoConsultoraIndividual
	FROM #ListadoConsultoraTotal
	WHERE TipoTactica = 'Individual'

	PRINT '#ListadoConsultoraBundle'
	SELECT * 
	INTO #ListadoConsultoraBundle
	FROM #ListadoConsultoraTotal
	WHERE TipoTactica = 'Bundle'

	/** 1. Táctica Individual **/

	/** 1.1. Cálculo de Brechas - GAP Normal **/

	/** 1.2. Cálculo de Brechas - GAP Motor Canibalización **/

	--Si Venta es mayor a cero en las U24C
	-- Condición 1  
	UPDATE #ListadoConsultoraIndividual 
	SET BrechaVenta_MC = VentaAcumU24C  
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 AND BrechaRecompraPotencial>0

		-- Condición 2 --Comentar por Canibalización
		UPDATE #ListadoConsultoraIndividual
		SET BrechaVenta_MC = -1
		WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 AND BrechaRecompraPotencial<=0

		-- Condición 3 - Recompró
		--Actualizo el GAP
		UPDATE #ListadoConsultoraIndividual  
		SET GAP = VentaAcumU6C - VentaAcumU6C_AA
		WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24

			-- Condición 4  
			UPDATE #ListadoConsultoraIndividual
			SET BrechaVenta_MC = VentaPromU24C
			WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24 AND GAP<=0		

			-- Condición 5  --Comentar por Canibalización
			UPDATE #ListadoConsultoraIndividual 
			SET BrechaVenta_MC = -1
			WHERE  VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24 AND GAP>0

		-- Condición 6 
		-- Actualiza el GAP
		UPDATE #ListadoConsultoraIndividual 
		SET GAP = VentaAcumU6C - VentaAcumPU6C
		WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24

			-- Condición 7  
			UPDATE #ListadoConsultoraIndividual 
			SET BrechaVenta_MC = VentaPromU24C
			WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24 AND GAP<=0

			-- Condición 8 --Comentar por Canibalización
			UPDATE #ListadoConsultoraIndividual  
			SET BrechaVenta_MC = -1
			WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24 AND GAP>0
	
	--Si Venta es menor o igual a cero en las U24C
	-- Condición 9
	UPDATE #ListadoConsultoraIndividual  
	SET BrechaVenta_MC = VentaPotencialMinU6C
	WHERE VentaAcumU24C<=0 

	/** 2. Táctica Bundle **/

	/** 2.1. Cálculo de Brechas - GAP Normal **/

	/** 2.2. Cálculo de Brechas - GAP Motor Canibalización **/

	UPDATE #ListadoConsultoraBundle
	SET BrechaVenta_MC = VentaPromU24C  
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 AND BrechaRecompraPotencial > 0

	UPDATE #ListadoConsultoraBundle 
	SET BrechaVenta_MC = -1  
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C=1 AND BrechaRecompraPotencial <=0

	-- Condición 2  
	UPDATE #ListadoConsultoraBundle  
	SET BrechaVenta_MC = VentaPromU24C
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24 AND (VentaAcumU6C-VentaAcumU6C_AA)<=0

	UPDATE #ListadoConsultoraBundle  
	SET BrechaVenta_MC = -1
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad>24 AND (VentaAcumU6C-VentaAcumU6C_AA)>0

	-- Condición 3 
	UPDATE #ListadoConsultoraBundle  
	SET BrechaVenta_MC = VentaPromU24C
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24 AND (VentaAcumU6C-VentaAcumPU6C)<=0 

	UPDATE #ListadoConsultoraBundle
	SET BrechaVenta_MC = -1
	WHERE VentaAcumU24C>0 AND FrecuenciaU24C>1 AND Antiguedad<=24 AND (VentaAcumU6C-VentaAcumPU6C)>0

	IF (@TipoGP = 1)
	BEGIN

		SELECT CodRegion,CodComportamientoRolling,CodTactica, 
			   SUM(VentaPotencialMinU6C)/COUNT(DISTINCT PKEbelista) VentaPotencialMinU6C_GP
			   /*SUM(FrecuenciaU24C)/COUNT(DISTINCT PKEbelista) FrecuenciaU24C_GP,
			   /*CAST(ROUND(AVG(RecenciaU24C_GP*1.00),0) AS INT) RecenciaU24C_GP,*/
			   AVG(RecenciaU24C_GP*1.00) RecenciaU24C_GP,
			   SUM(Gatillador)/COUNT(DISTINCT PKEbelista) Gatillador_GP*/
		INTO #BasePotencial_TacticaBundle
		FROM #ListadoConsultoraBundle
		GROUP BY CodRegion,CodComportamientoRolling,CodTactica

		--Se actualiza la Brecha Venta con Motor de Canibalización a nivel de Táctica
		UPDATE A  
		SET A.BrechaVenta_MC = B.VentaPotencialMinU6C_GP
		FROM #ListadoConsultoraBundle A INNER JOIN #BasePotencial_TacticaBundle B  
		ON A.CodTactica=B.CodTactica AND A.CodRegion=B.CodRegion AND A.CodComportamientoRolling=B.CodComportamientoRolling  
		AND A.BrechaVenta_MC=0

	END
	ELSE
	BEGIN

		SELECT PerfilGP,CodTactica, 
			   SUM(VentaPotencialMinU6C)/COUNT(DISTINCT PKEbelista) VentaPotencialMinU6C_GP
			   /*SUM(FrecuenciaU24C)/COUNT(DISTINCT PKEbelista) FrecuenciaU24C_GP,
			   /*CAST(ROUND(AVG(RecenciaU24C_GP*1.00),0) AS INT) RecenciaU24C_GP,*/
			   AVG(RecenciaU24C_GP*1.00) RecenciaU24C_GP,
			   SUM(Gatillador)/COUNT(DISTINCT PKEbelista) Gatillador_GP*/
		INTO #BasePotencial_TacticaBundle_p
		FROM #ListadoConsultoraBundle
		GROUP BY PerfilGP,CodTactica

		--Se actualiza la Brecha Venta con Motor de Canibalización a nivel de Táctica
		UPDATE A  
		SET A.BrechaVenta_MC = B.VentaPotencialMinU6C_GP
		FROM #ListadoConsultoraBundle A INNER JOIN #BasePotencial_TacticaBundle_p B  
		ON A.CodTactica=B.CodTactica AND A.PerfilGP=B.PerfilGP  
		AND A.BrechaVenta_MC=0

	END

	/** 3. Cálculo del Score **/

	--Unión de tácticas
	SELECT x.* 
	INTO #ListadoConsultora_Total
	FROM
	(SELECT * 
	FROM #ListadoConsultoraIndividual
	UNION ALL
	SELECT * 
	FROM #ListadoConsultoraBundle) x

	-- Considerar 4 Decimales
	UPDATE #ListadoConsultora_Total  
	SET BrechaVenta = ROUND(BrechaVenta,4),
	    PrecioOptimo = ROUND(PrecioOptimo,4),
		BrechaVenta_MC = ROUND(BrechaVenta_MC,4)

	-- Se obtienen los Promedios y Desviaciones
	SELECT 
	ROUND(SUM(FrecuenciaU24C)*1.0/COUNT(PKEbelista),4)	PromFrecuenciaU24C, 
	ROUND(STDEVP(FrecuenciaU24C),4)						DSFrecuenciaU24C,
	ROUND(SUM(RecenciaU24C)*1.0/COUNT(PKEbelista),4)	PromRecenciaU24C, 
	ROUND(STDEVP(RecenciaU24C),4)						DSRecenciaU24C,
	ROUND(SUM(BrechaVenta)*1.0/COUNT(PKEbelista),4)		PromBrechaVenta, 
	ROUND(STDEVP(BrechaVenta),4)						DSBrechaVenta,
	ROUND(SUM(BrechaVenta_MC)*1.0/COUNT(PKEbelista),4)	PromBrechaVenta_MC, 
	ROUND(STDEVP(BrechaVenta_MC),4)						DSBrechaVenta_MC,
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
		BrechaVenta_MCNor = CASE (B.DSBrechaVenta_MC) WHEN 0 THEN 0 ELSE ROUND((A.BrechaVenta_MC-B.PromBrechaVenta_MC)/B.DSBrechaVenta_MC,4) END,
		GAPPrecioOptimoNor = CASE (B.DSGAPPrecioOptimo) WHEN 0 THEN 0 ELSE ROUND((A.GAPPrecioOptimo-B.PromGAPPrecioOptimo)/B.DSGAPPrecioOptimo,4) END,
		GatilladorNor = CASE (B.DSGatillador) WHEN 0 THEN 0 ELSE ROUND((A.Gatillador-B.PromGatillador)/B.DSGatillador,4) END
	FROM #ListadoConsultora_Total A, #ListadoPromDSTotal B
	WHERE A.BrechaVenta >= 0 

	---Se calcula de la Oportunidad
	UPDATE #ListadoConsultora_Total  
	SET Oportunidad = ROUND((FrecuenciaNor - RecenciaNor + BrechaVentaNor - GAPPrecioOptimoNor + GatilladorNor),4),
	    Oportunidad_MC = ROUND((FrecuenciaNor - RecenciaNor + BrechaVenta_MCNor - GAPPrecioOptimoNor + GatilladorNor),4)

	--Se calcula el Promedio y Desviación Estándar de la Oportunidad
	SELECT ROUND(SUM(Oportunidad)*1.0/COUNT(PKEbelista),4) PromOportunidad, 
	       ROUND(STDEVP(Oportunidad),4) DSOportunidad
	INTO #ListadoPromDSTotalOportunidad
	FROM #ListadoConsultora_Total
	WHERE BrechaVenta>=0

	SELECT ROUND(SUM(Oportunidad_MC)*1.0/COUNT(PKEbelista),4) PromOportunidad_MC, 
	       ROUND(STDEVP(Oportunidad_MC),4) DSOportunidad_MC
	INTO #ListadoPromDSTotalOportunidad_MC
	FROM #ListadoConsultora_Total
	WHERE BrechaVenta_MC>=0

	--Se normaliza la variable Oportunidad
	UPDATE A
	SET OportunidadNor = CASE(B.DSOportunidad) WHEN 0 THEN 0 ELSE ROUND((A.Oportunidad-B.PromOportunidad)/B.DSOportunidad,4) END
	FROM #ListadoConsultora_Total A, #ListadoPromDSTotalOportunidad B
	WHERE A.BrechaVenta>=0

	UPDATE A
	SET Oportunidad_MCNor = CASE(B.DSOportunidad_MC) WHEN 0 THEN 0 ELSE ROUND((A.Oportunidad_MC-B.PromOportunidad_MC)/B.DSOportunidad_MC,4) END
	FROM #ListadoConsultora_Total A, #ListadoPromDSTotalOportunidad_MC B
	WHERE A.BrechaVenta_MC>=0

	--Se calcula el Score en base a la Oportunidad
	UPDATE #ListadoConsultora_Total  
	SET Score = (EXP(OportunidadNor))/(1 + EXP(OportunidadNor))
	WHERE BrechaVenta>=0

	UPDATE #ListadoConsultora_Total  
	SET Score_MC = (EXP(Oportunidad_MCNor))/(1 + EXP(Oportunidad_MCNor))
	WHERE BrechaVenta_MC>=0

	--Se calcula el Score_UU en base a la Oportunidad multiplicada por el número de unidades
	UPDATE #ListadoConsultora_Total  
	SET Score_UU = CASE WHEN (OportunidadNor*UnidadesTactica)>700 THEN ((EXP(700))/(1+ EXP(700)))
	ELSE (EXP(OportunidadNor*UnidadesTactica))/(1+ EXP(OportunidadNor*UnidadesTactica)) END
	WHERE BrechaVenta>=0

	UPDATE #ListadoConsultora_Total  
	SET Score_MC_UU = CASE WHEN (Oportunidad_MCNor*UnidadesTactica)>700 THEN ((EXP(700))/(1+ EXP(700)))
	ELSE (EXP(Oportunidad_MCNor*UnidadesTactica))/(1+ EXP(Oportunidad_MCNor*UnidadesTactica)) END
	WHERE BrechaVenta_MC>=0

	/** Vuelve a cargar tabla ListadoVariablesRFM **/
	PRINT 'DELETE FROM BD_ANALITICO.dbo.ARP_ListadoProbabilidades!!!'
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
	PRINT 'INSERT INTO BD_ANALITICO.dbo.ARP_ListadoVariablesRFM'
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

	--Borra los registros a la tabla ListadoProbabilidades en caso de reproceso
	--DELETE FROM BDDM01.[DATAMARTANALITICO].DBO.ARP_ListadoProbabilidades 
	PRINT 'DELETE FROM BD_ANALITICO.dbo.ARP_ListadoProbabilidades..MC'
	DELETE FROM BD_ANALITICO.dbo.ARP_ListadoProbabilidades 
	WHERE CodPais=@CodPais 
	AND AnioCampanaProceso=@AnioCampanaProceso 
	AND AnioCampanaExpo=@AnioCampanaExpo 
	AND TipoARP=@TipoARP 
	AND TipoPersonalizacion=@TipoPersonalizacion
	AND FlagMC=@FlagMC
	AND Perfil=@Perfil

	/** No usar Motor de Canibalizacion **/
	IF (@FlagMC=0)
	BEGIN

		--Carga los registros a la tabla ListadoProbabilidades
		--INSERT INTO BDDM01.[DATAMARTANALITICO].DBO.ARP_ListadoProbabilidades 
		INSERT INTO BD_ANALITICO.dbo.ARP_ListadoProbabilidades 
		(CodPais,AnioCampanaProceso,AnioCampanaExpo,PKEbelista,CodTActica,FlagTop,Probabilidad,TipoARP,TipoPersonalizacion,FlagMC,Perfil)
		SELECT @CodPais AS CodPais, @AnioCampanaProceso AS AnioCampanaProceso, @AnioCampanaExpo AS AnioCampanaExpo,PKEbelista,
			   CodTactica, FlagTop, Score_UU AS Probabilidad, @TipoARP AS TipoARP, @TipoPersonalizacion AS TipoPersonalizacion,@FlagMC,@Perfil AS Perfil
		FROM #ListadoConsultora_Total
		--WHERE Score_UU!=0

	END
	/** Usar Motor de Canibalizacion**/
	ELSE
	BEGIN

		--Carga los registros a la tabla ListadoProbabilidades
		--INSERT INTO BDDM01.[DATAMARTANALITICO].DBO.ARP_ListadoProbabilidades 
		INSERT INTO BD_ANALITICO.dbo.ARP_ListadoProbabilidades
		(CodPais,AnioCampanaProceso,AnioCampanaExpo,PKEbelista,CodTActica,FlagTop,Probabilidad,TipoARP,TipoPersonalizacion,FlagMC,Perfil)
		SELECT @CodPais AS CodPais, @AnioCampanaProceso AS AnioCampanaProceso, @AnioCampanaExpo AS AnioCampanaExpo,PKEbelista,
			   CodTactica, FlagTop, Score_MC_UU AS Probabilidad, @TipoARP AS TipoARP, @TipoPersonalizacion AS TipoPersonalizacion,@FlagMC,@Perfil AS Perfil
		FROM #ListadoConsultora_Total
		--WHERE Score_MC_UU!=0

	END

END

/** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,TipoGP,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,NULL,@FlagMC,@Perfil,NULL,@TipoGP,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

END


