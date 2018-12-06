CREATE FUNCTION [dbo].[CalculaAnioCampana] (@AnioCampana char(6), @delta int)  
RETURNS char(6)  
AS  
BEGIN  
	DECLARE @Resultado char(6)  
	DECLARE @numero int  
	DECLARE @anio char(4)  
	DECLARE @campana char(2)  
  
    SET @Resultado = ltrim(rtrim(cast(cast(@AnioCampana as int)- 1  as varchar) ))
	SET @numero = cast(LEFT(@Resultado,4) as int) * 18  +  cast(RIGHT(@Resultado,2) as int)  
  
	SET @numero = @numero + @delta  
  
	SET @anio = cast(@numero / 18  as varchar)
	SET @campana = cast((@numero % 18) + 1  as varchar)
	IF @campana < 10  
		SET @campana = '0' + @campana  
  
		SET @Resultado = @anio +  @campana  
  
    RETURN(@Resultado) 
    --PRINT @Resultado
	/*
	select dbo.[CalculaAnioCampana]( '201601',-36)
	*/
END





