select ehmast_ehkey,partmast_partkey, CAST(LISTAGG(CASE 
                when (CX_SEQ_NBR between 400 and  699) and CX_COD_IDIOMA = 'IN' 
                        THEN CX_TXT_LP_FM  
         When (CX_SEQ_NBR between 700 and  9999 and  CX_COD_IDIOMA = 'IN')
            THEN CX_TXT_DIBUJO_FM
        end, ' '  ON OVERFLOW TRUNCATE '...' ) WITHIN GROUP (ORDER BY CX_SEQ_NBR) AS VARCHAR2(4000)) AS ENGLISHAUTODESCRIPTION,
        CAST(LISTAGG(CASE 
                when (CX_SEQ_NBR between 400 and  699) and CX_COD_IDIOMA = 'ES' 
                        THEN CX_TXT_LP_FM  
         When (CX_SEQ_NBR between 700 and  9999 and  CX_COD_IDIOMA = 'ES')
            THEN CX_TXT_DIBUJO_FM
        end, ' ' ON OVERFLOW TRUNCATE '...') WITHIN GROUP (ORDER BY CX_SEQ_NBR) AS VARCHAR2(4000))  AS DOMESTICAUTODESCRIPTION,
                count(*) as nbr
from cdc_nd72.cxcomt 
where (cx_seq_nbr between 400 and 699 and CX_TXT_LP_FM is not null
or (cx_seq_nbr between 700 and 9999 and CX_TXT_DIBUJO_FM is not null) )
group by  ehmast_ehkey,partmast_partkey