SELECT 
        RPLIDR                                                        AS DRAWINGSETNUMBER,
        RPLIDI                                                        AS DRWSTISSUEINDEX,
        TEOPL                                                         AS DOMESTICDESCRIPTION,
        CAST(NULL AS VARCHAR2(200))                AS DOMESTICCOMMENT,
        CAST(NULL AS VARCHAR2(200))                AS ENGLISHDESCRIPTION,
        CLN                                                                AS LANGUAGE,
        NPGECN                                                         AS PAGENUMBER,
        ROWID AS A,
        TO_NUMBER(NLGEO) * 100 + NPGECN AS PAGE_LINE_NBR_DOMDESC, /* Might be used later in order to detect the first line of the first page */
        cast(999999 as number)            AS PAGE_LINE_NBR_DOMCOM,
        cast(999999 as number)            AS PAGE_LINE_NBR_ENGDESC,        
        1 AS MARKER
FROM
        T36205_ECN_EVOLUTION
WHERE 
        CLN = 'F'
AND substr(RPLID,10,3) = '   '
UNION ALL
SELECT 
        RPLIDR                                                        AS DRAWINGSETNUMBER,
        RPLIDI                                                        AS DRWSTISSUEINDEX,
        NULL                                                        AS DOMESTICDESCRIPTION,
        TOBECN                                                         AS DOMESTICCOMMENT,
        NULL                                                        AS ENGLISHDESCRIPTION,
        CLN                                                                AS LANGUAGE,
        NPGECN                                                         AS PAGENUMBER,
        ROWID AS A,
        CAST(999998        AS NUMBER)            AS PAGE_LINE_NBR_DOMDESC,
        NLGECN * 100 + NPGECN                         AS PAGE_LINE_NBR_DOMCOM,
        CAST(999998        AS NUMBER)            AS PAGE_LINE_NBR_ENGDESC,                /* Might be used later in order to detect the first line of the first page */
        2 AS MARKER
FROM
        T36206_ECN_INFO
WHERE 
        CLN = 'F'
AND substr(RPLID,10,3) = '   '
UNION ALL
SELECT 
        RPLIDR                                                        AS DRAWINGSETNUMBER,
        RPLIDI                                                        AS DRWSTISSUEINDEX,
        NULL                                                        AS DOMESTICDESCRIPTION,
        NULL                                                        AS DOMESTICCOMMENT,
        TEOPL                                                         AS ENGLISHDESCRIPTION,
        CLN                                                                AS LANGUAGE,
        NPGECN                                                         AS PAGENUMBER,
        ROWID AS A,
        CAST(999997        AS NUMBER)            AS PAGE_LINE_NBR_DOMDESC,
        CAST(999997        AS NUMBER)            AS PAGE_LINE_NBR_DOMCOM,
        TO_NUMBER(NLGEO) * 100 + NPGECN   AS PAGE_LINE_NBR_ENGDESC, /* Might be used later in order to detect the first line of the first page */
        3 AS MARKER
FROM
        T36205_ECN_EVOLUTION
WHERE 
        CLN = 'E'
AND substr(RPLID,10,3) = '   '
UNION ALL
SELECT 
        RPL                                                                AS DRAWINGSETNUMBER,
        IEOPL                                                        AS DRWSTISSUEINDEX,
        NULL                                                         AS DOMESTICDESCRIPTION,
        NULL                                                         AS DOMESTICCOMMENT,
        NULL                                                         AS ENGLISHDESCRIPTION,
        NULL                                                         AS LANGUAGE,
        NULL                                                         AS PAGENUMBER,
        ROWID                                                         AS A,
        CAST(999996        AS NUMBER)                        AS PAGE_LINE_NBR_DOMDESC, 
        CAST(999996        AS NUMBER)                        AS PAGE_LINE_NBR_DOMCOM, 
        CAST(999996        AS NUMBER)                        AS PAGE_LINE_NBR_ENGDESC, 
        4 AS MARKER
FROM
        T36284_ECN