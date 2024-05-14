SELECT
        A.CACCDSNUMBER AS A_CACCDSNUMBER,
        A.CACCDSISSUEINDEX AS A_CACCDSISSUEINDEX,
        B.CACCCINUMBER AS B_CACCCINUMBER,
        LNK.CREATESTAMPA2 AS CREATEDATE,
        LNK.UPDATEDATE AS UPDATEDATE,
        LNK.AMOUNTA7 AS QUANTITY,
        LNK.UNITA7 AS UNIT,
        CAST(get_program() as varchar(10)) AS PROGRAM,
        CAST('PASS-SSCI' AS VARCHAR2(20)) AS SOURCESYSTEM,
        A.TEAM_RESTRICT AS TEAM_O_CACCDS,
        B.TEAM_RESTRICT AS TEAM_O_CACCCI,
        A.INTERNALISSUE AS INTERNALISSUE_O_CACCDS,
        B.INTERNALISSUE AS INTERNALISSUE_O_CACCCI,
        NVL(LNK.managedByBOMConsolidation, 0) IS_MANAGED_BY_BOM,
        NVL(LNK.managedByCad, 0) IS_MANAGED_BY_CAD,
        NVL(LNK.managedByCadXML, 0) IS_MANAGED_BY_CADXML,
        CASE
        WHEN NVL(LNK.managedByBOMConsolidation, 0) = 1
        THEN 'Managed By BOM Consolidation'
        WHEN NVL(LNK.managedByCad, 0) = 1
        THEN 'Managed By Cad'
        WHEN NVL(LNK.managedByCadXML, 0) = 1
        THEN 'Managed By Cad XML'
        ELSE 'Metadata only link'
        END AS LINKTYPE,
        1 AS MARKER,
        A.ROWID AS A,
        LNK.ROWID AS LNK,
        B.ROWID AS B,
        CAST(NULL AS ROWID) AS RID4
FROM
        WRK_O_CACCDS A,
        WRK_O_CACCCI B,
        WRK_2S10_L_CACCDS_CACCCIM LNK
WHERE
        A.IDA2A2_ITERATION = LNK.FROM_IDA3A5
        AND LNK.TO_IDA3B5 = B.IDA2A2_MASTER
        AND LNK.TYPEDEFNAME IN('com.airbus.site.AdapPUL', 'wt.part.WTPartUsageLink')
        AND LNK.IS_ACTIVE = 1
        AND A.SOURCESYSTEM = 'PASS-SSCI'
UNION ALL
SELECT
        A.CACCDSNUMBER                                     AS A_CACCDSNUMBER
        , A.CACCDSISSUEINDEX                         AS A_CACCDSISSUEINDEX
        , B.CACCCINUMBER                                AS B_CACCCINUMBER
        , NULL                                                        AS CREATEDATE
        , NULL                                                        AS UPDATEDATE
    , LNK.QUANTITY                                         AS QUANTITY
    , LNK.UNIT                                                AS UNIT
        , A.PROGRAM                                         AS PROGRAM
        , CAST('SPRINT' AS VARCHAR2(10)) AS SOURCESYSTEM
        , A.TEAM_RESTRICT                                 AS TEAM_O_CACCDS
        , B.TEAM_RESTRICT                                AS TEAM_O_CACCCI
        , A.INTERNALISSUE                                AS INTERNALISSUE_O_CACCDS
        , B.INTERNALISSUE                                AS INTERNALISSUE_O_CACCCI
        , NULL                                                        AS IS_MANAGED_BY_BOM
        , NULL                                                        AS IS_MANAGED_BY_CAD
        , NULL                                                        AS IS_MANAGED_BY_CADXML
        , LNK.LINKTYPE                                          AS LINKTYPE
        , 2                                                         AS MARKER
        , A.ROWID                                                 AS A
        , LNK.ROWID                                         AS LNK
        , B.ROWID                                                 AS B
        , NULL                                                        AS RID4
FROM
        WRK_O_CACCDS A,
        WRK_O_CACCCI B,
        WRK_ND72_ALL_LINK LNK
WHERE
        A.IDENTNUMBER  =  LNK.FATHER
        AND B.IDENTNUMBER = LNK.SON
        AND A.SOURCESYSTEM = 'SPRINT'
        AND A.IS_REL_ISSUE = 1
        AND LNK.FILTER_ISSUE in ('IS_REL', 'IS_BOTH')
UNION ALL
SELECT
        A.CACCDSNUMBER                                     AS A_CACCDSNUMBER
        , A.CACCDSISSUEINDEX                         AS A_CACCDSISSUEINDEX
        , B.CACCCINUMBER                                AS B_CACCCINUMBER
        , NULL                                                        AS CREATEDATE
        , NULL                                                        AS UPDATEDATE
    , LNK.QUANTITY                                         AS QUANTITY
    , LNK.UNIT                                                AS UNIT
        , A.PROGRAM                                         AS PROGRAM
        , CAST('SPRINT' AS VARCHAR2(10)) AS SOURCESYSTEM
        , A.TEAM_RESTRICT                                 AS TEAM_O_CACCDS
        , B.TEAM_RESTRICT                                AS TEAM_O_CACCCI
        , A.INTERNALISSUE                                AS INTERNALISSUE_O_CACCDS
        , B.INTERNALISSUE                                AS INTERNALISSUE_O_CACCCI
        , NULL                                                        AS IS_MANAGED_BY_BOM
        , NULL                                                        AS IS_MANAGED_BY_CAD
        , NULL                                                        AS IS_MANAGED_BY_CADXML
        , LNK.LINKTYPE                                          AS LINKTYPE
        , 3                                                         AS MARKER
        , A.ROWID                                                 AS A
        , LNK.ROWID                                         AS LNK
        , B.ROWID                                                 AS B
        , NULL                                                        AS RID4
FROM
        WRK_O_CACCDS A,
        WRK_O_CACCCI B,
        WRK_ND72_ALL_LINK LNK
WHERE
        A.IDENTNUMBER  =  LNK.FATHER
        AND B.IDENTNUMBER = LNK.SON
        AND A.SOURCESYSTEM = 'SPRINT'
        AND A.IS_LAST_ISSUE = 1
        AND A.IS_REL_ISSUE = 0
        AND LNK.FILTER_ISSUE in ('IS_LAST_WIP', 'IS_BOTH') 