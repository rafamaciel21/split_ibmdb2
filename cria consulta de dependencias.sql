:RA 

/*
 * Script para buscar a relação entre as tabelas
 */

----------------------
-- função split 
----------------------
/*
-- função localizada mp stackoverflow 
-- https://stackoverflow.com/questions/25188979/how-can-we-parse-a-tilde-delimited-column-in-sql-to-create-several-columns/25189396#25189396



CREATE OR REPLACE FUNCTION CONV.SPLIT (
Data     VARCHAR(32000),
Delimiter VARCHAR(5))

-- RETORNO DO RESULTADO, NO QUAL O ID É UMA COLUNA INCREMENTAL E O VALUE É O REULTADO 
RETURNS TABLE (
				ID   INT,
				VALUE VARCHAR(256)
			)

LANGUAGE SQL
DETERMINISTIC

RETURN
WITH CTE_Items (ID,StartString,StopString) AS
(
	   	SELECT
	       	1 AS ID,
	       	1 AS StartString,
	       	LOCATE(Delimiter, Data) AS StopString
	   	FROM 	SYSIBM.SYSDUMMY1
	   	WHERE 	LENGTH(Delimiter)>0 AND
	     		LENGTH(Data)>0
		UNION ALL
		SELECT
		   ID + 1,
		   StopString + LENGTH(Delimiter),
		   LOCATE(Delimiter, Data, StopString + LENGTH(Delimiter))
		FROM
	   		CTE_Items
		WHERE
   			StopString > 0
)
SELECT 
	ID, 
	SUBSTRING(
				Data,
				StartString,
	   			CASE 
	   				WHEN StopString=0 THEN LENGTH(Data)
	       			ELSE StopString-StartString 
	       		END
       		)
 FROM CTE_Items;
 
 
 -- A CHAMADA DA FUNÇÃO VEM POR PASSAGEM DA MESMA COMO TABLE 
 SELECT * FROM TABLE('VALOR1-VALOR2-VALOR3','-') -- esse exemplo separa em linhas conforme o delimitador '-'
 
*/


-- FUNÇÃO QUE RETORNA 

/*
 * 
 * -- DESCARTADO MAS DEIXEI AQUI PARA VER SE PODE SER UTIL
CREATE OR REPLACE PROCEDURE CONV.CONTA_REGISTROS( 	
													IN CONSULTA VARCHAR(3000),   
													OUT QTD_REGISTROS NUMERIC(20,0)
												)
LANGUAGE SQL
BEGIN
    DECLARE CNT BIGINT DEFAULT 0;
    DECLARE C1 CURSOR FOR S1;

    -- Prepara a consulta dinâmica
    PREPARE S1 FROM 'SELECT COUNT(*) FROM (' || CONSULTA || ') AS TEMP';

    -- Abre o cursor e lê o resultado
    OPEN C1;
    FETCH C1 INTO CNT;
    CLOSE C1;

    -- Retorna a contagem
    SET QTD_REGISTROS = CNT;
END
GO*/


CREATE OR REPLACE FUNCTION CONV.BUSCA_DEPENDENCIA_TABELA(RA_SCHEMA VARCHAR(50),RA_TABELA VARCHAR(100))
RETURNS TABLE (
	TABELA_PRINCIPAL VARCHAR(100),
	TABSCHEMA_PRINCIPAL VARCHAR(50),
	TABNAME_DEPENDENTE VARCHAR(50), 
	TABSCHEMA_DEPENDENTE VARCHAR(50),
	QTD_REGISTROS NUMERIC(20,0),
	CONSULTA VARCHAR(3000)
)
LANGUAGE SQL
RETURN
WITH CONSULTA_DEP AS (
			SELECT 
				TBL.REFTABSCHEMA,
				TBL.REFTABNAME,
				TBL.TABNAME,
				TBL.TABSCHEMA,
				'SELECT * FROM '||TRIM(TBL.TABSCHEMA)||'.'||TRIM(TBL.TABNAME)||
				' AS B WHERE EXISTS (SELECT 1 FROM '||TRIM(TBL.REFTABSCHEMA)||'.'||TRIM(TBL.REFTABNAME)||
				' AS B1 WHERE '||LISTAGG( 'B1.'||TBL.COLUNA_PK||' = B.'||TBL2.COLUNA_FK, ' AND ')||')' AS CONSULTA
			FROM (
					WITH tabela_tr AS (
											SELECT 
												REFTABNAME, 
												TABNAME,
												TABSCHEMA,
												REFTABSCHEMA,
												REPLACE(REGEXP_REPLACE(TRIM(PK_COLNAMES), ' {2,}', ','), ',{2,}', ',')  AJUSTE_PK
											FROM SYSCAT.REFERENCES 
											WHERE REFTABNAME = RA_TABELA
											AND REFTABSCHEMA = RA_SCHEMA
										) 
					SELECT 
						 REFTABNAME, 
						 REFTABSCHEMA, 
						 TABNAME, 
						 TABSCHEMA,
						 COLUNA_PK.VALUE AS COLUNA_PK, 
						 COLUNA_PK.ID  
					FROM tabela_tr tr,table(CONV.SPLIT(tr.ajuste_Pk, ',')) AS COLUNA_PK
				) AS TBL 
			JOIN  
				(
					WITH tabela_tr2 AS (
											SELECT 
												REFTABNAME, 
												TABNAME,
												TABSCHEMA,
												REFTABSCHEMA,
												REPLACE(REGEXP_REPLACE(TRIM(FK_COLNAMES), ' {2,}', ','), ',{2,}', ',')  AJUSTE_FK
											FROM SYSCAT.REFERENCES 
											WHERE REFTABNAME = RA_TABELA
											AND REFTABSCHEMA = RA_SCHEMA
										) 
					SELECT 
						 REFTABNAME, 
						 REFTABSCHEMA, 
						 TABNAME, 
						 TABSCHEMA,
						 COLUNA_FK.VALUE AS COLUNA_FK, 
						 COLUNA_FK.ID
					FROM 	tabela_tr2 tr,
							table(CONV.SPLIT(tr.ajuste_Fk, ',')) AS COLUNA_FK
				) AS TBL2
			ON 	TBL.REFTABNAME 		= TBL2.REFTABNAME AND 
				TBL.TABNAME 		= TBL2.TABNAME AND 
				TBL.ID				= TBL2.ID AND 
				TBL.REFTABSCHEMA 	= TBL2.REFTABSCHEMA AND 
				TBL.TABSCHEMA 		= TBL2.TABSCHEMA
			GROUP BY 	TBL.REFTABNAME, 
						TBL.TABNAME,
						TBL.REFTABSCHEMA,
						TBL.TABSCHEMA
) 
 SELECT 
 	REFTABSCHEMA,
	REFTABNAME,
	TABNAME, 
	TABSCHEMA,
	0 AS QTD_REGISTROS,	
	CONSULTA
 FROM CONSULTA_DEP  
 



-- CHAMADA DA FUNCTION 
 SELECT * FROM TABLE(CONV.BUSCA_DEPENDENCIA_TABELA('DBA','CLIENTE_FORNECEDOR')) AS TBL
