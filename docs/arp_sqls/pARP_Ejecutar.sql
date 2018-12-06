CREATE PROCEDURE [dbo].[pARP_Ejecutar] @CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoPersonalizacion CHAR(3),
@TipoARP CHAR(1),@FlagCarga INT,@FlagMC INT,@Perfil VARCHAR(1),@FlagOF INT,@TipoGP INT
AS
BEGIN 

/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoPersonalizacion			CHAR(3)
DECLARE @TipoARP						CHAR(1)
DECLARE @FlagCarga						INT
DECLARE @Perfil							VARCHAR(1)
DECLARE @FlagMC							INT
DECLARE @FlagOF							INT
DECLARE @TipoGP							INT

SET @CodPais				= 'CR'		-- Codigo de país
SET @AnioCampanaProceso		= '201701'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201707'	-- Campaña de Venta
SET @TipoPersonalizacion	= 'OPT'		-- 'OPT','ODD','SR'
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
SET @FlagCarga				= 0			-- 1: Carga/Personalización | 0: Estimación
SET @FlagMC					= 0			-- 1: Con Motor de Canibalización | 0: Sin Motor de Canibalización
SET @Perfil					='X'		-- Número de Perfil | 'X': Sin Perfil
SET @FlagOF					= 1			-- 1: Incluir OF | 0: No incluir OF
SET @TipoGP					= 1         -- 1: Segmento y Región | 2: Perfil*/

DECLARE @NumPerfil						INT
DECLARE @VarPerfil						VARCHAR(1)
DECLARE @Mensaje						VARCHAR(250)
SET @NumPerfil				= 1

/** Inicio: Variables Log **/
DECLARE @FechaInicio 					DATETIME 
DECLARE @Procedimiento					VARCHAR(50)
SET @FechaInicio = GETDATE()   
SET @Procedimiento = 'pARP_Ejecutar'
/** Fin: Variables Log **/

/** 1. Ejecución sin Perfiles **/
IF (@Perfil='X')
BEGIN 

	/** 1.0. Módulo Validación de Parámetros **/
	SELECT @Mensaje=dbo.fARP_ValidaParametros(@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,@Perfil)
	--SET @Mensaje='Adv.' /** Borrar **/

	/** Si no hay errores **/
	IF (LEN(@Mensaje)=4)
	BEGIN 

		/** 1.1. Módulo Base de Consultoras **/
		EXEC pARP_BaseConsultoras @CodPais,@AnioCampanaProceso,@Perfil,@TipoPersonalizacion 

		/** 1.2. Módulo RFM **/
		EXEC pARP_RFM @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,@Perfil

		/** 1.3. Módulo Grupo Potencial **/
		EXEC pARP_GrupoPotencial @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,@Perfil,@TipoGP

		/** 1.4. Módulo Motor de Canibalización **/
		EXEC pARP_MotorCanibalizacion @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagMC,@Perfil,@TipoGP

		/** 1.5. Módulo Reglas de Negocio **/

		/** 1.5.1. Reglas de Negocio OPT **/
		IF (@TipoPersonalizacion='OPT')
		BEGIN

			EXEC pARP_ReglasNegocio_OPT @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagCarga,@FlagMC,@Perfil,@FlagOF

		END

		/** 1.5.2. Reglas de Negocio ODD **/
		IF (@TipoPersonalizacion='ODD')
		BEGIN

			EXEC pARP_ReglasNegocio_ODD @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagMC,@Perfil
			
		END

		/** 1.5.3. Reglas de Negocio SR **/
		IF (@TipoPersonalizacion='SR')
		BEGIN

			EXEC pARP_ReglasNegocio_SR @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagMC,@Perfil

		END

		/** 1.5.4. Reglas de Negocio OF **/
		IF (@TipoPersonalizacion='OF')
		BEGIN

			EXEC pARP_ReglasNegocio_OF @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagMC,@Perfil

		END

		IF (@TipoPersonalizacion='BPT')
		BEGIN

			EXEC pARP_ReglasNegocio_BPT @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagMC,@Perfil
		END

	END
	ELSE 
	BEGIN

		/** Mostrar los errores de Parámetros **/
		PRINT @Mensaje 
		RETURN 

	END

END
/** 2. Ejecución por Perfiles **/
ELSE 
BEGIN
	
	WHILE (@NumPerfil <=CAST(@Perfil AS INT))
	BEGIN

		SET @VarPerfil=CAST(@NumPerfil AS VARCHAR)

		/** 2.0. Módulo Validación de Parámetros **/

		SELECT @Mensaje=dbo.fARP_ValidaParametros(@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,@VarPerfil)
		--SET @Mensaje='Adv.' /** Borrar **/
		
		/** Si no hay errores **/
		IF (LEN(@Mensaje)=4)
		BEGIN 

			/** 2.1. Módulo Base de Consultoras **/
			EXEC pARP_BaseConsultoras @CodPais,@AnioCampanaProceso,@VarPerfil,@TipoPersonalizacion 

			/** 2.2. Módulo RFM **/
			--EXEC pARP_RFM @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,@VarPerfil
			EXEC pARP_RFM @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,@VarPerfil
			/** 2.3. Módulo Grupo Potencial **/
			EXEC pARP_GrupoPotencial @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,@VarPerfil,@TipoGP

			/** 2.4. Módulo Motor de Canibalización **/
			EXEC pARP_MotorCanibalizacion @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagMC,@VarPerfil,@TipoGP

			/** 2.5. Módulo Reglas de Negocio **/

			/** 2.5.1. Reglas de Negocio OPT **/
			IF (@TipoPersonalizacion='OPT')
			BEGIN

				EXEC pARP_ReglasNegocio_OPT @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagCarga,@FlagMC,@VarPerfil,@FlagOF

			END

			/** 2.5.2. Reglas de Negocio ODD **/
			IF (@TipoPersonalizacion='ODD')
			BEGIN

				EXEC pARP_ReglasNegocio_ODD @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagMC,@VarPerfil

			END

			/** 2.5.3. Reglas de Negocio SR **/
			IF (@TipoPersonalizacion='SR')
			BEGIN

				EXEC pARP_ReglasNegocio_SR @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagMC,@VarPerfil

			END

			/** 2.5.4. Reglas de Negocio OF **/
			IF (@TipoPersonalizacion='OF')
			BEGIN

				EXEC pARP_ReglasNegocio_OF @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagMC,@VarPerfil

			END

			IF (@TipoPersonalizacion='BPT')
			BEGIN

				EXEC pARP_ReglasNegocio_BPT @CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoARP,@FlagMC,@Perfil
			END
		
		END
		ELSE
		BEGIN 
		
		    /** Mostrar los errores de Parámetros **/
			PRINT @Mensaje 
			RETURN 

		END

		SET @NumPerfil=@NumPerfil+1

	END
END

/** Inicio: Registra Log **/
INSERT INTO BD_ANALITICO.dbo.ARP_Ejecucion_Log
(Procedimiento,CodPais,AnioCampanaProceso,AnioCampanaExpo,TipoPersonalizacion,TipoARP,FlagCarga,FlagMC,Perfil,FlagOF,TipoGP,
FechaInicio,FechaFin,Duracion)
VALUES
(@Procedimiento,@CodPais,@AnioCampanaProceso,@AnioCampanaExpo,@TipoPersonalizacion,@TipoARP,@FlagCarga,@FlagMC,@Perfil,@FlagOF,@TipoGP,
@FechaInicio,GETDATE(),DATEDIFF(MI,@FechaInicio,GETDATE()))
/** Fin: Registra Log **/

END


