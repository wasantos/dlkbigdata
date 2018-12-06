CREATE FUNCTION [dbo].[fARP_ValidaParametros] (@CodPais VARCHAR(2),@AnioCampanaProceso CHAR(6),@AnioCampanaExpo CHAR(6),@TipoPersonalizacion CHAR(3),
@TipoARP CHAR(1),@FlagCarga INT,@Perfil VARCHAR(1))
RETURNS VARCHAR(250) 
AS
BEGIN 
/*DECLARE @CodPais						VARCHAR(2)
DECLARE @AnioCampanaProceso				CHAR(6)
DECLARE @AnioCampanaExpo				CHAR(6)
DECLARE @TipoPersonalizacion			CHAR(3)
DECLARE @TipoARP						CHAR(1)
DECLARE @FlagCarga						INT
DECLARE @Perfil							VARCHAR(1)

SET @CodPais				= 'BO'		-- Codigo de país
SET @AnioCampanaProceso		= '2017'	-- Última campaña cerrada
SET @AnioCampanaExpo		= '201705'	-- Campaña de Venta
SET @TipoPersonalizacion	= 'ODD'		-- 'OPT','ODD','SR'
SET @TipoARP				= 'E'		-- 'N': Nuevas | 'E': Establecidas
SET @FlagCarga				= 1			-- 1: Carga/Personalización | 0: Estimación
SET @Perfil					='1'		-- Número de Perfil | 'X': Sin Perfil*/

DECLARE @Mensaje		VARCHAR(250)
DECLARE @NumProd		INT

SET @Mensaje = 'Adv: '
SET @NumProd = 0

/** 1. Validación para Carga **/
IF (@FlagCarga=1)
BEGIN

	/** 1.1. Valida la existencia de información en ARP_Parametros **/
	SELECT @NumProd=COUNT(DISTINCT CodCUC)
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CodPais=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo
	AND TipoARP=@TipoARP AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil 
	
	IF (@NumProd=0) SET @Mensaje=@Mensaje+'No hay parametros-'

	/** 1.2. Valida que todos los CUCs existan en la tabla DPRODUCTO **/
	IF (@NumProd - (SELECT COUNT(DISTINCT A.CodCUC)
	FROM BD_ANALITICO.dbo.ARP_Parametros A (NOLOCK)
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
	WHERE A.CodPais=@CodPais AND A.AnioCampanaProceso=@AnioCampanaProceso AND A.AnioCampanaExpo=@AnioCampanaExpo
	AND A.TipoARP=@TipoARP AND A.TipoPersonalizacion=@TipoPersonalizacion
	AND A.Perfil=@Perfil))>0 
	BEGIN

	 SET @Mensaje = 'Los Cuc No Existen en DWH_DPRODUCTO :: ' + REPLACE(REPLACE (STUFF (
( 
	SELECT DISTINCT  ' , ' , A.CODCUC 
	FROM BD_ANALITICO.dbo.ARP_Parametros A (NOLOCK)
	LEFT JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
	WHERE A.CodPais=@CodPais AND A.AnioCampanaProceso=@AnioCampanaProceso AND A.AnioCampanaExpo=@AnioCampanaExpo
	AND A.TipoARP=@TipoARP AND A.TipoPersonalizacion=@TipoPersonalizacion
	AND A.Perfil=@Perfil AND  B.CODCUC IS NULL
	FOR XML PATH('') 
)
,1,1, ''),'<CODCUC>',''),'</CODCUC>','')

 	END




	/** 1.3. Valida que las Tacticas tengan un solo Indicador Padre en ARP_Parametros **/
	IF EXISTS (SELECT CodTactica, SUM(IndicadorPadre) NumIndPadre
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CodPais=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo
	AND TipoARP=@TipoARP AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil 
	GROUP BY CodTactica
	HAVING SUM(IndicadorPadre)>1) SET @Mensaje=@Mensaje+'Error Ind.Padre-'

	/** 1.4. Valida que los CUVs no se repitan en ARP_Parametros **/
	IF (SELECT COUNT(1)-COUNT(DISTINCT CodVenta)
	FROM BD_ANALITICO.dbo.ARP_Parametros (NOLOCK)
	WHERE CodPais=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo
	AND TipoARP=@TipoARP AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil AND IndicadorPadre=1)>0 SET @Mensaje=@Mensaje+'CUV repetido-'

	/** 1.5. Valida Vinculos sin Indicador Padre en ARP_Parametros LMHM **/
	if exists (select CodTactica
	from (
	SELECT PERFIL,CodTactica, COUNT(1) CANTIDAD,
           SUM(CASE WHEN IndicadorPadre = 1 THEN 1 ELSE 0   END ) Valor
    FROM  arp_parametros 
	where TipoPersonalizacion =  @TipoPersonalizacion
	and Codpais = @CodPais 
	and AnioCampanaExpo = @AnioCampanaExpo
	and anioCampanaPRoceso = @AnioCampanaProceso
	AND TIPOARP= @TipoARP 
	GROUP BY PERFIL,CodTactica 
	) t1 where Valor = 0) SET @Mensaje=@Mensaje+'Vinculos sin Indicador Padre-'


END
ELSE
/** 2. Estimación **/
BEGIN

	/** 2.1. Valida la existencia de información en ARP_Parametros_Est **/
	SELECT @NumProd=COUNT(DISTINCT CodCUC)
	FROM BD_ANALITICO.dbo.ARP_Parametros_Est (NOLOCK)
	WHERE CodPais=@CodPais AND AnioCampanaProceso=@AnioCampanaProceso AND AnioCampanaExpo=@AnioCampanaExpo
	AND TipoARP=@TipoARP AND TipoPersonalizacion=@TipoPersonalizacion
	AND Perfil=@Perfil 
	
	IF (@NumProd=0) SET @Mensaje=@Mensaje+'No hay parametros-'

	/** 2.2. Valida que todos los CUCs existan en la tabla DPRODUCTO **/
	--IF (@NumProd - (SELECT COUNT(DISTINCT A.CodCUC)
	--FROM BD_ANALITICO.dbo.ARP_Parametros_Est A (NOLOCK)
	--INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
	--WHERE A.CodPais=@CodPais AND A.AnioCampanaProceso=@AnioCampanaProceso AND A.AnioCampanaExpo=@AnioCampanaExpo
	--AND A.TipoARP=@TipoARP AND A.TipoPersonalizacion=@TipoPersonalizacion
	--AND A.Perfil=@Perfil))>0 SET @Mensaje=@Mensaje+'Faltan CUC en base-' 

	/** 1.2. Valida que todos los CUCs existan en la tabla DPRODUCTO **/
	IF (@NumProd - (SELECT COUNT(DISTINCT A.CodCUC)
	FROM BD_ANALITICO.dbo.ARP_Parametros_EST A (NOLOCK)
	INNER JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
	WHERE A.CodPais=@CodPais AND A.AnioCampanaProceso=@AnioCampanaProceso AND A.AnioCampanaExpo=@AnioCampanaExpo
	AND A.TipoARP=@TipoARP AND A.TipoPersonalizacion=@TipoPersonalizacion
	AND A.Perfil=@Perfil))>0 
	BEGIN

	 SET @Mensaje = 'Los Cuc No Existen en DWH_DPRODUCTO :: ' + REPLACE(REPLACE (STUFF (
( 
	SELECT DISTINCT  ' , ' , A.CODCUC 
	FROM BD_ANALITICO.dbo.ARP_Parametros_EST A (NOLOCK)
	LEFT JOIN DWH_ANALITICO.dbo.DWH_DPRODUCTO B (NOLOCK) ON A.CodCUC=B.CodCUC AND B.CodPais=@CodPais
	WHERE A.CodPais=@CodPais AND A.AnioCampanaProceso=@AnioCampanaProceso AND A.AnioCampanaExpo=@AnioCampanaExpo
	AND A.TipoARP=@TipoARP AND A.TipoPersonalizacion=@TipoPersonalizacion
	AND A.Perfil=@Perfil AND  B.CODCUC IS NULL
	FOR XML PATH('') 
)
,1,1, ''),'<CODCUC>',''),'</CODCUC>','')


END

END

RETURN @Mensaje
--PRINT @Mensaje

END


