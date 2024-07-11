USE schema JPDCLEDW_ACCESS ;
create or replace view CIM01KOKYA(
	"kokyano",
	"zipcode",
	"todofukenname",
	"seibetukbn",
	"birthday",
	"birthdayedit",
	"kokyakbn",
	"kokyataiokbn",
	"kokyasts",
	"dmtenpoflg",
	"dmtsuhanflg",
	"teltenpoflg",
	"teltsuhanflg",
	"firstmediacode",
	"deleteflg",
	"carrername",
	"insertdate",
	"inserttime",
	"insertid",
	"updatedate",
	"rank",
	"hcardcontractdate",
	"hcardcanceldate",
	"hcardclass",
	"cardkokyano",
	"fskbn",
	"rowid01",
	"rowid02",
	"todofukencode",
	"testusrflg",
	"name1",
	"name2",
	"kana1",
	"kana2",
	"city",
	"addr",
	"tatemono",
	"mail_pc",
	"tel",
	"taikai_flg",
	"point",
	"sndflg",
	"webno",
	"mail_mobile",
	"tel_mobile",
	"tel_other",
	"main_tel",
	"main_mail",
	"pastpaykbn",
	"buyruikei",
	"buyruikeisamplein",
	"buyfirst",
	"buylast_estheup4",
	"buylast_estheupcab",
	"buylast_soniclift",
	"buylast_acg",
	"buylast_gel",
	"buynum_estheup4",
	"buynum_estheupcab",
	"buynum_soniclift",
	"buynum_acg",
	"buynum_gel",
	"nopay_yen",
	"channel_regist",
	"mailmemb_flg",
	"mailbihada_flg",
	"mailcilabo_flg",
	"addr_kana",
	"mailtenki_flg",
	"mailstore_flg",
	"sndnewsletter_flg",
	"sndmousikomi_flg",
	"hcardupdate",
	"hcardgroupno",
	"okurijo_memo",
	"pointguide_flg",
	"eccontractdate",
	"faxno",
	"memo",
	"updateid",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select kokyano as "kokyano",
    zipcode as "zipcode",
    todofukenname as "todofukenname",
    seibetukbn as "seibetukbn",
    birthday as "birthday",
    birthdayedit as "birthdayedit",
    kokyakbn as "kokyakbn",
    kokyataiokbn as "kokyataiokbn",
    kokyasts as "kokyasts",
    dmtenpoflg as "dmtenpoflg",
    dmtsuhanflg as "dmtsuhanflg",
    teltenpoflg as "teltenpoflg",
    teltsuhanflg as "teltsuhanflg",
    firstmediacode as "firstmediacode",
    deleteflg as "deleteflg",
    carrername as "carrername",
    insertdate as "insertdate",
    inserttime as "inserttime",
    insertid as "insertid",
    updatedate as "updatedate",
    rank as "rank",
    hcardcontractdate as "hcardcontractdate",
    hcardcanceldate as "hcardcanceldate",
    hcardclass as "hcardclass",
    cardkokyano as "cardkokyano",
    fskbn as "fskbn",
    rowid01 as "rowid01",
    rowid02 as "rowid02",
    todofukencode as "todofukencode",
    testusrflg as "testusrflg",
    name1 as "name1",
    name2 as "name2",
    kana1 as "kana1",
    kana2 as "kana2",
    city as "city",
    addr as "addr",
    tatemono as "tatemono",
    mail_pc as "mail_pc",
    tel as "tel",
    taikai_flg as "taikai_flg",
    point as "point",
    sndflg as "sndflg",
    webno as "webno",
    mail_mobile as "mail_mobile",
    tel_mobile as "tel_mobile",
    tel_other as "tel_other",
    main_tel as "main_tel",
    main_mail as "main_mail",
    pastpaykbn as "pastpaykbn",
    buyruikei as "buyruikei",
    buyruikeisamplein as "buyruikeisamplein",
    buyfirst as "buyfirst",
    buylast_estheup4 as "buylast_estheup4",
    buylast_estheupcab as "buylast_estheupcab",
    buylast_soniclift as "buylast_soniclift",
    buylast_acg as "buylast_acg",
    buylast_gel as "buylast_gel",
    buynum_estheup4 as "buynum_estheup4",
    buynum_estheupcab as "buynum_estheupcab",
    buynum_soniclift as "buynum_soniclift",
    buynum_acg as "buynum_acg",
    buynum_gel as "buynum_gel",
    nopay_yen as "nopay_yen",
    channel_regist as "channel_regist",
    mailmemb_flg as "mailmemb_flg",
    mailbihada_flg as "mailbihada_flg",
    mailcilabo_flg as "mailcilabo_flg",
    addr_kana as "addr_kana",
    mailtenki_flg as "mailtenki_flg",
    mailstore_flg as "mailstore_flg",
    sndnewsletter_flg as "sndnewsletter_flg",
    sndmousikomi_flg as "sndmousikomi_flg",
    hcardupdate as "hcardupdate",
    hcardgroupno as "hcardgroupno",
    okurijo_memo as "okurijo_memo",
    pointguide_flg as "pointguide_flg",
    eccontractdate as "eccontractdate",
    faxno as "faxno",
    memo as "memo",
    updateid as "updateid",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdcledw_integration.cim01kokya;
create or replace view CIM05OPERA(
	"opecode",
	"opename",
	"bumoncode",
	"logincode",
	"ciflg",
	"join_rec_upddate"
) as 
select  
opecode as "opecode",
opename as "opename",
bumoncode as "bumoncode",
logincode as "logincode",
ciflg as "ciflg",
join_rec_upddate as "join_rec_upddate"
from dev_dna_core.jpdcledw_integration.cim05opera;
create or replace view CIM08SHKOS_BUNKAI_QV(
	"itemcode",
	"itemname",
	"itemcname",
	"tanka",
	"chubuncode",
	"chubunname",
	"chubuncname",
	"daibuncode",
	"daibunname",
	"daibuncname",
	"daidaibuncode",
	"daidaibunname",
	"daidaibuncname",
	"bunkai_itemcode",
	"bunkai_itemname",
	"bunkai_itemcname",
	"bunkai_tanka",
	"bunkai_kossu",
	"bunkai_kosritu",
	"insertdate"
) as
select itemcode as "itemcode",
    itemname as "itemname",
    itemcname as "itemcname",
    tanka as "tanka",
    chubuncode as "chubuncode",
    chubunname as "chubunname",
    chubuncname as "chubuncname",
    daibuncode as "daibuncode",
    daibunname as "daibunname",
    daibuncname as "daibuncname",
    daidaibuncode as "daidaibuncode",
    daidaibunname as "daidaibunname",
    daidaibuncname as "daidaibuncname",
    bunkai_itemcode as "bunkai_itemcode",
    bunkai_itemname as "bunkai_itemname",
    bunkai_itemcname as "bunkai_itemcname",
    bunkai_tanka as "bunkai_tanka",
    bunkai_kossu as "bunkai_kossu",
    bunkai_kosritu as "bunkai_kosritu",
    insertdate as "insertdate"
from dev_dna_core.jpdcledw_integration.cim08shkos_bunkai_qv;
create or replace view CLD_M(
	"ymd_dt",
	"year",
	"year_445",
	"year_15",
	"year_20",
	"half",
	"half_nm",
	"half_445",
	"half_445_nm",
	"half_15",
	"half_15_nm",
	"half_20",
	"half_20_nm",
	"quarter",
	"quarter_nm",
	"quarter_445",
	"quarter_445_nm",
	"quarter_15",
	"quarter_15_nm",
	"quarter_20",
	"quarter_20_nm",
	"month",
	"month_nm",
	"month_445",
	"month_445_nm",
	"month_15",
	"month_15_nm",
	"month_20",
	"month_20_nm",
	"ymonth_445",
	"ymonth_15",
	"ymonth_20",
	"week_ms",
	"week_ss",
	"mweek_445",
	"mweek_15ms",
	"mweek_15ss",
	"mweek_20",
	"mweek_445_iso",
	"mweek_15ms_iso",
	"mweek_15ss_iso",
	"mweek_20_iso",
	"day",
	"day_of_week",
	"opr_flg",
	"sls_flg"
) as
select ymd_dt as "ymd_dt",
    year as "year",
    year_445 as "year_445",
    year_15 as "year_15",
    year_20 as "year_20",
    half as "half",
    half_nm as "half_nm",
    half_445 as "half_445",
    half_445_nm as "half_445_nm",
    half_15 as "half_15",
    half_15_nm as "half_15_nm",
    half_20 as "half_20",
    half_20_nm as "half_20_nm",
    quarter as "quarter",
    quarter_nm as "quarter_nm",
    quarter_445 as "quarter_445",
    quarter_445_nm as "quarter_445_nm",
    quarter_15 as "quarter_15",
    quarter_15_nm as "quarter_15_nm",
    quarter_20 as "quarter_20",
    quarter_20_nm as "quarter_20_nm",
    month as "month",
    month_nm as "month_nm",
    month_445 as "month_445",
    month_445_nm as "month_445_nm",
    month_15 as "month_15",
    month_15_nm as "month_15_nm",
    month_20 as "month_20",
    month_20_nm as "month_20_nm",
    ymonth_445 as "ymonth_445",
    ymonth_15 as "ymonth_15",
    ymonth_20 as "ymonth_20",
    week_ms as "week_ms",
    week_ss as "week_ss",
    mweek_445 as "mweek_445",
    mweek_15ms as "mweek_15ms",
    mweek_15ss as "mweek_15ss",
    mweek_20 as "mweek_20",
    mweek_445_iso as "mweek_445_iso",
    mweek_15ms_iso as "mweek_15ms_iso",
    mweek_15ss_iso as "mweek_15ss_iso",
    mweek_20_iso as "mweek_20_iso",
    day as "day",
    day_of_week as "day_of_week",
    opr_flg as "opr_flg",
    sls_flg as "sls_flg"
from dev_dna_core.jpdcledw_integration.cld_m;
create or replace view C_TBDMSNDHIST(
	"c_disendid",
	"c_diusrid",
	"c_dsdmnumber",
	"c_dsdmsendkubun",
	"c_dsdmsenddate",
	"c_dsdmname",
	"c_dsextension1",
	"c_dsextension2",
	"c_dsextension3",
	"c_dsextension4",
	"c_dsextension5",
	"c_dsdmimportid",
	"dsprep",
	"dsren",
	"diprepusr",
	"direnusr",
	"dielimflg",
	"dselim",
	"dielimusr",
	"c_diusrchanelid",
	"source_file_date",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select c_disendid as "c_disendid",
    c_diusrid as "c_diusrid",
    c_dsdmnumber as "c_dsdmnumber",
    c_dsdmsendkubun as "c_dsdmsendkubun",
    c_dsdmsenddate as "c_dsdmsenddate",
    c_dsdmname as "c_dsdmname",
    c_dsextension1 as "c_dsextension1",
    c_dsextension2 as "c_dsextension2",
    c_dsextension3 as "c_dsextension3",
    c_dsextension4 as "c_dsextension4",
    c_dsextension5 as "c_dsextension5",
    c_dsdmimportid as "c_dsdmimportid",
    dsprep as "dsprep",
    dsren as "dsren",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimflg as "dielimflg",
    dselim as "dselim",
    dielimusr as "dielimusr",
    c_diusrchanelid as "c_diusrchanelid",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdclitg_integration.c_tbdmsndhist;
create or replace view C_TBECKESAI(
	"c_dikesaiid",
	"diorderid",
	"diecusrid",
	"dishukkasts",
	"disokoid",
	"dsuriagedt",
	"dinyukinsts",
	"dicardnyukinsts",
	"dsnyukindt",
	"direnkeists",
	"dicancel",
	"c_dsexchangeflg",
	"c_dshoryutypekbn",
	"dihoryu",
	"c_dsshorttodokedate",
	"c_dsshortdeliverytime",
	"c_dstodokedate",
	"c_dsdeliverytime",
	"dstodokesei",
	"dstodokemei",
	"dstodokeseikana",
	"dstodokemeikana",
	"dstodokezip",
	"c_dstodokeprefcd",
	"dstodokepref",
	"dstodokecity",
	"dstodokeaddr",
	"dstodoketatemono",
	"dstodokeaddrkana",
	"dstodoketel",
	"dstodokefax",
	"dibillincluded",
	"dihaisokeitai",
	"c_dstodaydeliveryflg",
	"c_dsshukkadate",
	"dskessaihoho",
	"c_dicardid",
	"dspaymentclass",
	"c_dsdividetimes",
	"c_dsgmoorderid",
	"dicardtradeid",
	"dicardtradepass",
	"dicardsts",
	"dicardcancel",
	"diauthprc",
	"dsauthdt",
	"dscarddt",
	"dicardcancelprc",
	"c_dsacceptno",
	"c_dinptransactionid",
	"c_dioldnptransactionid1",
	"c_dioldnptransactionid2",
	"c_dspaymentflg",
	"ditotalprc",
	"c_didiscountprc",
	"c_didiscountall",
	"diordertax",
	"dihaisoprc",
	"c_dicollectprc",
	"c_ditoujitsuhaisoprc",
	"diseikyuprc",
	"c_diregdiscticketprc",
	"dinyukinprc",
	"diseikyuremain",
	"dihenkinzumiprc",
	"c_dsdiffprckbn",
	"diusepoint",
	"c_diexchangepoint",
	"c_diorderavailablepoint",
	"c_dikakutokuyoteipoint",
	"c_dsnoshinshomemo",
	"c_dsnohinshoautomemo",
	"c_dsokurijomemo",
	"c_dsnmemoerrflg",
	"c_dsomemoerrflg",
	"divouchercode",
	"c_dscctraceurl",
	"c_dsusrtanmatsukbn",
	"c_dsusrtraceurl",
	"c_dspickingclasskbn",
	"c_dspointdate1",
	"c_dspointdate2",
	"c_dspointdate3",
	"c_dipoint1",
	"c_dipoint2",
	"c_dipoint3",
	"c_dsregularnum",
	"c_dsreizoreitokubun",
	"c_dsrikusokubun",
	"c_dspostshipflg",
	"c_dsreservepostshipflg",
	"c_dsnonshipmailflg",
	"c_dsreserveflg",
	"c_dsfreekbn",
	"c_dsnptorokusts",
	"c_dsnpsyuseists",
	"c_dsnpcancelsts",
	"c_dsnpshukkasts",
	"c_dsnpsaitorokusts",
	"c_dsrakutenuriagests",
	"c_dsnpprintdatagetflg",
	"dsprep",
	"dsren",
	"dselim",
	"diprepusr",
	"direnusr",
	"dielimusr",
	"dielimflg",
	"c_dishukkashijicondid",
	"c_diadjustprc",
	"c_dicardtypekbn",
	"c_dihenkinprcinputdiff",
	"c_dihenkinprcinputdifftotal",
	"c_dsrakutengmoorderid",
	"c_dirakutencardtradeid",
	"c_dirakutencardtradepass",
	"c_dsrakutensyuseists",
	"c_dsrakutencancelsts",
	"c_didiscountticketissuekesaiid",
	"ditodokeid",
	"c_dsgmoshoptransactionid",
	"c_dsgmotransactionid",
	"c_dsgmoauthprckbn",
	"c_dsgmotransactionsts",
	"source_file_date",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select c_dikesaiid as "c_dikesaiid",
    diorderid as "diorderid",
    diecusrid as "diecusrid",
    dishukkasts as "dishukkasts",
    disokoid as "disokoid",
    dsuriagedt as "dsuriagedt",
    dinyukinsts as "dinyukinsts",
    dicardnyukinsts as "dicardnyukinsts",
    dsnyukindt as "dsnyukindt",
    direnkeists as "direnkeists",
    dicancel as "dicancel",
    c_dsexchangeflg as "c_dsexchangeflg",
    c_dshoryutypekbn as "c_dshoryutypekbn",
    dihoryu as "dihoryu",
    c_dsshorttodokedate as "c_dsshorttodokedate",
    c_dsshortdeliverytime as "c_dsshortdeliverytime",
    c_dstodokedate as "c_dstodokedate",
    c_dsdeliverytime as "c_dsdeliverytime",
    dstodokesei as "dstodokesei",
    dstodokemei as "dstodokemei",
    dstodokeseikana as "dstodokeseikana",
    dstodokemeikana as "dstodokemeikana",
    dstodokezip as "dstodokezip",
    c_dstodokeprefcd as "c_dstodokeprefcd",
    dstodokepref as "dstodokepref",
    dstodokecity as "dstodokecity",
    dstodokeaddr as "dstodokeaddr",
    dstodoketatemono as "dstodoketatemono",
    dstodokeaddrkana as "dstodokeaddrkana",
    dstodoketel as "dstodoketel",
    dstodokefax as "dstodokefax",
    dibillincluded as "dibillincluded",
    dihaisokeitai as "dihaisokeitai",
    c_dstodaydeliveryflg as "c_dstodaydeliveryflg",
    c_dsshukkadate as "c_dsshukkadate",
    dskessaihoho as "dskessaihoho",
    c_dicardid as "c_dicardid",
    dspaymentclass as "dspaymentclass",
    c_dsdividetimes as "c_dsdividetimes",
    c_dsgmoorderid as "c_dsgmoorderid",
    dicardtradeid as "dicardtradeid",
    dicardtradepass as "dicardtradepass",
    dicardsts as "dicardsts",
    dicardcancel as "dicardcancel",
    diauthprc as "diauthprc",
    dsauthdt as "dsauthdt",
    dscarddt as "dscarddt",
    dicardcancelprc as "dicardcancelprc",
    c_dsacceptno as "c_dsacceptno",
    c_dinptransactionid as "c_dinptransactionid",
    c_dioldnptransactionid1 as "c_dioldnptransactionid1",
    c_dioldnptransactionid2 as "c_dioldnptransactionid2",
    c_dspaymentflg as "c_dspaymentflg",
    ditotalprc as "ditotalprc",
    c_didiscountprc as "c_didiscountprc",
    c_didiscountall as "c_didiscountall",
    diordertax as "diordertax",
    dihaisoprc as "dihaisoprc",
    c_dicollectprc as "c_dicollectprc",
    c_ditoujitsuhaisoprc as "c_ditoujitsuhaisoprc",
    diseikyuprc as "diseikyuprc",
    c_diregdiscticketprc as "c_diregdiscticketprc",
    dinyukinprc as "dinyukinprc",
    diseikyuremain as "diseikyuremain",
    dihenkinzumiprc as "dihenkinzumiprc",
    c_dsdiffprckbn as "c_dsdiffprckbn",
    diusepoint as "diusepoint",
    c_diexchangepoint as "c_diexchangepoint",
    c_diorderavailablepoint as "c_diorderavailablepoint",
    c_dikakutokuyoteipoint as "c_dikakutokuyoteipoint",
    c_dsnoshinshomemo as "c_dsnoshinshomemo",
    c_dsnohinshoautomemo as "c_dsnohinshoautomemo",
    c_dsokurijomemo as "c_dsokurijomemo",
    c_dsnmemoerrflg as "c_dsnmemoerrflg",
    c_dsomemoerrflg as "c_dsomemoerrflg",
    divouchercode as "divouchercode",
    c_dscctraceurl as "c_dscctraceurl",
    c_dsusrtanmatsukbn as "c_dsusrtanmatsukbn",
    c_dsusrtraceurl as "c_dsusrtraceurl",
    c_dspickingclasskbn as "c_dspickingclasskbn",
    c_dspointdate1 as "c_dspointdate1",
    c_dspointdate2 as "c_dspointdate2",
    c_dspointdate3 as "c_dspointdate3",
    c_dipoint1 as "c_dipoint1",
    c_dipoint2 as "c_dipoint2",
    c_dipoint3 as "c_dipoint3",
    c_dsregularnum as "c_dsregularnum",
    c_dsreizoreitokubun as "c_dsreizoreitokubun",
    c_dsrikusokubun as "c_dsrikusokubun",
    c_dspostshipflg as "c_dspostshipflg",
    c_dsreservepostshipflg as "c_dsreservepostshipflg",
    c_dsnonshipmailflg as "c_dsnonshipmailflg",
    c_dsreserveflg as "c_dsreserveflg",
    c_dsfreekbn as "c_dsfreekbn",
    c_dsnptorokusts as "c_dsnptorokusts",
    c_dsnpsyuseists as "c_dsnpsyuseists",
    c_dsnpcancelsts as "c_dsnpcancelsts",
    c_dsnpshukkasts as "c_dsnpshukkasts",
    c_dsnpsaitorokusts as "c_dsnpsaitorokusts",
    c_dsrakutenuriagests as "c_dsrakutenuriagests",
    c_dsnpprintdatagetflg as "c_dsnpprintdatagetflg",
    dsprep as "dsprep",
    dsren as "dsren",
    dselim as "dselim",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimusr as "dielimusr",
    dielimflg as "dielimflg",
    c_dishukkashijicondid as "c_dishukkashijicondid",
    c_diadjustprc as "c_diadjustprc",
    c_dicardtypekbn as "c_dicardtypekbn",
    c_dihenkinprcinputdiff as "c_dihenkinprcinputdiff",
    c_dihenkinprcinputdifftotal as "c_dihenkinprcinputdifftotal",
    c_dsrakutengmoorderid as "c_dsrakutengmoorderid",
    c_dirakutencardtradeid as "c_dirakutencardtradeid",
    c_dirakutencardtradepass as "c_dirakutencardtradepass",
    c_dsrakutensyuseists as "c_dsrakutensyuseists",
    c_dsrakutencancelsts as "c_dsrakutencancelsts",
    c_didiscountticketissuekesaiid as "c_didiscountticketissuekesaiid",
    ditodokeid as "ditodokeid",
    c_dsgmoshoptransactionid as "c_dsgmoshoptransactionid",
    c_dsgmotransactionid as "c_dsgmotransactionid",
    c_dsgmoauthprckbn as "c_dsgmoauthprckbn",
    c_dsgmotransactionsts as "c_dsgmotransactionsts",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdclitg_integration.c_tbeckesai;
create or replace view C_TBECREGULARMEISAI(
	"c_dsregularmeisaiid",
	"c_dsdeleveryym",
	"c_dstodokedate",
	"c_diregularcontractid",
	"c_diusrid",
	"c_diregularcontractdate",
	"dirouteid",
	"diid",
	"dsitemid",
	"c_dipromid",
	"c_diregularcourseid",
	"ditodokeid",
	"dstodokesei",
	"dstodokemei",
	"dstodokeseikana",
	"dstodokemeikana",
	"dstodokezip",
	"c_dstodokeprefcd",
	"dstodokepref",
	"dstodokecity",
	"dstodokeaddr",
	"dstodoketatemono",
	"dstodokeaddrkana",
	"dstodoketel",
	"dstodokefax",
	"c_dsdosokbn",
	"dskessaihoho",
	"c_dicardid",
	"dspaymentclass",
	"dihaisokeitai",
	"c_dsyupacketkahiflg",
	"c_dsdeliveryrulekbn",
	"c_dsdeliveryruledatekbn",
	"c_dsdeliveryruleweekkbn",
	"c_dsdeliveryruledowkbn",
	"c_dsdeliverytime",
	"c_dsnoshinshomemo",
	"c_dsokurijomemo",
	"c_dicancelflg",
	"c_dsordercreatekbn",
	"c_dscontractchangekbn",
	"c_dsschedulechg05kbn",
	"c_dscategoryfreeflg",
	"c_dsschedulechg02kbn",
	"c_dsschedulechg04kbn",
	"c_dsschedulechg08kbn",
	"c_dsschedulechg06kbn",
	"c_dsschedulechg07kbn",
	"c_dsschedulechg09kbn",
	"c_dsschedulechg01kbn",
	"c_dsschedulechg03kbn",
	"c_dsteikifirstflg",
	"c_diregularresult",
	"c_dsregularresultflg",
	"c_dsschedulehaneiflg",
	"c_dsnextcreateflg",
	"c_dsnextdeliverydate",
	"dsprep",
	"dsren",
	"dselim",
	"diprepusr",
	"direnusr",
	"dielimusr",
	"dielimflg",
	"c_didiscountticketissuekesaiid",
	"c_dsordercreateflg",
	"source_file_date",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select c_dsregularmeisaiid as "c_dsregularmeisaiid",
    c_dsdeleveryym as "c_dsdeleveryym",
    c_dstodokedate as "c_dstodokedate",
    c_diregularcontractid as "c_diregularcontractid",
    c_diusrid as "c_diusrid",
    c_diregularcontractdate as "c_diregularcontractdate",
    dirouteid as "dirouteid",
    diid as "diid",
    dsitemid as "dsitemid",
    c_dipromid as "c_dipromid",
    c_diregularcourseid as "c_diregularcourseid",
    ditodokeid as "ditodokeid",
    dstodokesei as "dstodokesei",
    dstodokemei as "dstodokemei",
    dstodokeseikana as "dstodokeseikana",
    dstodokemeikana as "dstodokemeikana",
    dstodokezip as "dstodokezip",
    c_dstodokeprefcd as "c_dstodokeprefcd",
    dstodokepref as "dstodokepref",
    dstodokecity as "dstodokecity",
    dstodokeaddr as "dstodokeaddr",
    dstodoketatemono as "dstodoketatemono",
    dstodokeaddrkana as "dstodokeaddrkana",
    dstodoketel as "dstodoketel",
    dstodokefax as "dstodokefax",
    c_dsdosokbn as "c_dsdosokbn",
    dskessaihoho as "dskessaihoho",
    c_dicardid as "c_dicardid",
    dspaymentclass as "dspaymentclass",
    dihaisokeitai as "dihaisokeitai",
    c_dsyupacketkahiflg as "c_dsyupacketkahiflg",
    c_dsdeliveryrulekbn as "c_dsdeliveryrulekbn",
    c_dsdeliveryruledatekbn as "c_dsdeliveryruledatekbn",
    c_dsdeliveryruleweekkbn as "c_dsdeliveryruleweekkbn",
    c_dsdeliveryruledowkbn as "c_dsdeliveryruledowkbn",
    c_dsdeliverytime as "c_dsdeliverytime",
    c_dsnoshinshomemo as "c_dsnoshinshomemo",
    c_dsokurijomemo as "c_dsokurijomemo",
    c_dicancelflg as "c_dicancelflg",
    c_dsordercreatekbn as "c_dsordercreatekbn",
    c_dscontractchangekbn as "c_dscontractchangekbn",
    c_dsschedulechg05kbn as "c_dsschedulechg05kbn",
    c_dscategoryfreeflg as "c_dscategoryfreeflg",
    c_dsschedulechg02kbn as "c_dsschedulechg02kbn",
    c_dsschedulechg04kbn as "c_dsschedulechg04kbn",
    c_dsschedulechg08kbn as "c_dsschedulechg08kbn",
    c_dsschedulechg06kbn as "c_dsschedulechg06kbn",
    c_dsschedulechg07kbn as "c_dsschedulechg07kbn",
    c_dsschedulechg09kbn as "c_dsschedulechg09kbn",
    c_dsschedulechg01kbn as "c_dsschedulechg01kbn",
    c_dsschedulechg03kbn as "c_dsschedulechg03kbn",
    c_dsteikifirstflg as "c_dsteikifirstflg",
    c_diregularresult as "c_diregularresult",
    c_dsregularresultflg as "c_dsregularresultflg",
    c_dsschedulehaneiflg as "c_dsschedulehaneiflg",
    c_dsnextcreateflg as "c_dsnextcreateflg",
    c_dsnextdeliverydate as "c_dsnextdeliverydate",
    dsprep as "dsprep",
    dsren as "dsren",
    dselim as "dselim",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimusr as "dielimusr",
    dielimflg as "dielimflg",
    c_didiscountticketissuekesaiid as "c_didiscountticketissuekesaiid",
    c_dsordercreateflg as "c_dsordercreateflg",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdclitg_integration.c_tbecregularmeisai;
create or replace view C_TBECREGULAROPERATEHIST(
	"c_dsregularoperatehistid",
	"c_dsregularoperatedate",
	"c_dschgtargetkbn",
	"c_dshaneikikankbn",
	"c_dshaneiunitkbn",
	"c_dsregularmemo",
	"c_diinputusrid",
	"c_dsinputusrname",
	"c_dsinputtelcompanycd",
	"c_dsregularmeisaiid",
	"c_dsdeleveryym",
	"c_dstodokedate",
	"c_diregularcontractid",
	"c_diusrid",
	"c_diregularcontractdate",
	"dirouteid",
	"diid",
	"dsitemid",
	"c_dipromid",
	"c_diregularcourseid",
	"ditodokeid",
	"dstodokesei",
	"dstodokemei",
	"dstodokeseikana",
	"dstodokemeikana",
	"dstodokezip",
	"c_dstodokeprefcd",
	"dstodokepref",
	"dstodokecity",
	"dstodokeaddr",
	"dstodoketatemono",
	"dstodokeaddrkana",
	"dstodoketel",
	"dstodokefax",
	"c_dsdosokbn",
	"dskessaihoho",
	"c_dicardid",
	"dspaymentclass",
	"dihaisokeitai",
	"c_dsyupacketkahiflg",
	"c_dsdeliveryrulekbn",
	"c_dsdeliveryruledatekbn",
	"c_dsdeliveryruleweekkbn",
	"c_dsdeliveryruledowkbn",
	"c_dsdeliverytime",
	"c_dsnoshinshomemo",
	"c_dsokurijomemo",
	"c_dicancelflg",
	"c_dsordercreatekbn",
	"c_dscontractchangekbn",
	"c_dsschedulechg05kbn",
	"c_dscategoryfreeflg",
	"c_dsschedulechg02kbn",
	"c_dsschedulechg04kbn",
	"c_dsschedulechg08kbn",
	"c_dsschedulechg06kbn",
	"c_dsschedulechg07kbn",
	"c_dsschedulechg09kbn",
	"c_dsschedulechg01kbn",
	"c_dsschedulechg03kbn",
	"c_dsteikifirstflg",
	"c_diregularresult",
	"c_dsregularresultflg",
	"c_dsschedulehaneiflg",
	"c_dsnextcreateflg",
	"c_dsnextdeliverydate",
	"dsprep",
	"dsren",
	"dselim",
	"diprepusr",
	"direnusr",
	"dielimusr",
	"dielimflg",
	"source_file_date",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select c_dsregularoperatehistid as "c_dsregularoperatehistid",
    c_dsregularoperatedate as "c_dsregularoperatedate",
    c_dschgtargetkbn as "c_dschgtargetkbn",
    c_dshaneikikankbn as "c_dshaneikikankbn",
    c_dshaneiunitkbn as "c_dshaneiunitkbn",
    c_dsregularmemo as "c_dsregularmemo",
    c_diinputusrid as "c_diinputusrid",
    c_dsinputusrname as "c_dsinputusrname",
    c_dsinputtelcompanycd as "c_dsinputtelcompanycd",
    c_dsregularmeisaiid as "c_dsregularmeisaiid",
    c_dsdeleveryym as "c_dsdeleveryym",
    c_dstodokedate as "c_dstodokedate",
    c_diregularcontractid as "c_diregularcontractid",
    c_diusrid as "c_diusrid",
    c_diregularcontractdate as "c_diregularcontractdate",
    dirouteid as "dirouteid",
    diid as "diid",
    dsitemid as "dsitemid",
    c_dipromid as "c_dipromid",
    c_diregularcourseid as "c_diregularcourseid",
    ditodokeid as "ditodokeid",
    dstodokesei as "dstodokesei",
    dstodokemei as "dstodokemei",
    dstodokeseikana as "dstodokeseikana",
    dstodokemeikana as "dstodokemeikana",
    dstodokezip as "dstodokezip",
    c_dstodokeprefcd as "c_dstodokeprefcd",
    dstodokepref as "dstodokepref",
    dstodokecity as "dstodokecity",
    dstodokeaddr as "dstodokeaddr",
    dstodoketatemono as "dstodoketatemono",
    dstodokeaddrkana as "dstodokeaddrkana",
    dstodoketel as "dstodoketel",
    dstodokefax as "dstodokefax",
    c_dsdosokbn as "c_dsdosokbn",
    dskessaihoho as "dskessaihoho",
    c_dicardid as "c_dicardid",
    dspaymentclass as "dspaymentclass",
    dihaisokeitai as "dihaisokeitai",
    c_dsyupacketkahiflg as "c_dsyupacketkahiflg",
    c_dsdeliveryrulekbn as "c_dsdeliveryrulekbn",
    c_dsdeliveryruledatekbn as "c_dsdeliveryruledatekbn",
    c_dsdeliveryruleweekkbn as "c_dsdeliveryruleweekkbn",
    c_dsdeliveryruledowkbn as "c_dsdeliveryruledowkbn",
    c_dsdeliverytime as "c_dsdeliverytime",
    c_dsnoshinshomemo as "c_dsnoshinshomemo",
    c_dsokurijomemo as "c_dsokurijomemo",
    c_dicancelflg as "c_dicancelflg",
    c_dsordercreatekbn as "c_dsordercreatekbn",
    c_dscontractchangekbn as "c_dscontractchangekbn",
    c_dsschedulechg05kbn as "c_dsschedulechg05kbn",
    c_dscategoryfreeflg as "c_dscategoryfreeflg",
    c_dsschedulechg02kbn as "c_dsschedulechg02kbn",
    c_dsschedulechg04kbn as "c_dsschedulechg04kbn",
    c_dsschedulechg08kbn as "c_dsschedulechg08kbn",
    c_dsschedulechg06kbn as "c_dsschedulechg06kbn",
    c_dsschedulechg07kbn as "c_dsschedulechg07kbn",
    c_dsschedulechg09kbn as "c_dsschedulechg09kbn",
    c_dsschedulechg01kbn as "c_dsschedulechg01kbn",
    c_dsschedulechg03kbn as "c_dsschedulechg03kbn",
    c_dsteikifirstflg as "c_dsteikifirstflg",
    c_diregularresult as "c_diregularresult",
    c_dsregularresultflg as "c_dsregularresultflg",
    c_dsschedulehaneiflg as "c_dsschedulehaneiflg",
    c_dsnextcreateflg as "c_dsnextcreateflg",
    c_dsnextdeliverydate as "c_dsnextdeliverydate",
    dsprep as "dsprep",
    dsren as "dsren",
    dselim as "dselim",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimusr as "dielimusr",
    dielimflg as "dielimflg",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdclitg_integration.c_tbecregularoperatehist;
create or replace view C_TBTELCOMPANYMST(
	"c_dstelcompanycd",
	"c_dstelcompayname",
	"didisporder",
	"dielimflg"
) as 
select  
c_dstelcompanycd as "c_dstelcompanycd",
c_dstelcompayname as "c_dstelcompayname",
didisporder as "didisporder",
dielimflg as "dielimflg"
from dev_dna_core.jpdcledw_integration.c_tbtelcompanymst;
create or replace view DM_KESAI_MART_DLY(
	"kokyano",
	"saleno_key",
	"saleno",
	"order_dt",
	"ship_dt",
	"pl_order_dt",
	"pl_ship_dt",
	"f_order",
	"f_ship445",
	"channel",
	"juchkbn",
	"kesai_itemcode",
	"setitemnm",
	"total_price",
	"qty",
	"誕生日",
	"顧客現在ランク",
	"性別コード",
	"職業",
	"itemcode",
	"itemname",
	"itemcname",
	"tanka",
	"chubuncode",
	"chubunname",
	"chubuncname",
	"daibuncode",
	"daibunname",
	"daibuncname",
	"daidaibuncode",
	"daidaibunname",
	"daidaibuncname",
	"bunkai_itemcode",
	"bunkai_itemname",
	"bunkai_itemcname",
	"bunkai_tanka",
	"bunkai_kossu",
	"bunkai_kosritu",
	"kose_itemcode",
	"koseiocode",
	"kosecode",
	"suryo",
	"koseritsu",
	"mds_itemcode",
	"mds_itemname",
	"attr01",
	"attr02",
	"attr03",
	"attr04",
	"attr05",
	"attr06",
	"attr07",
	"attr08",
	"attr09",
	"attr10",
	"i13_itemcode",
	"settanpinsetkbn",
	"teikikeiyaku",
	"f",
	"latest_stage",
	"stage_ym",
	"y_order_f",
	"y_ship_f"
) as
select kokyano as "kokyano",
    saleno_key as "saleno_key",
    saleno as "saleno",
    order_dt as "order_dt",
    ship_dt as "ship_dt",
    pl_order_dt as "pl_order_dt",
    pl_ship_dt as "pl_ship_dt",
    f_order as "f_order",
    f_ship445 as "f_ship445",
    channel as "channel",
    juchkbn as "juchkbn",
    kesai_itemcode as "kesai_itemcode",
    setitemnm as "setitemnm",
    total_price as "total_price",
    qty as "qty",
    "誕生日" as "誕生日",
    "顧客現在ランク" as "顧客現在ランク",
    "性別コード" as "性別コード",
    "職業" as "職業",
    itemcode as "itemcode",
    itemname as "itemname",
    itemcname as "itemcname",
    tanka as "tanka",
    chubuncode as "chubuncode",
    chubunname as "chubunname",
    chubuncname as "chubuncname",
    daibuncode as "daibuncode",
    daibunname as "daibunname",
    daibuncname as "daibuncname",
    daidaibuncode as "daidaibuncode",
    daidaibunname as "daidaibunname",
    daidaibuncname as "daidaibuncname",
    bunkai_itemcode as "bunkai_itemcode",
    bunkai_itemname as "bunkai_itemname",
    bunkai_itemcname as "bunkai_itemcname",
    bunkai_tanka as "bunkai_tanka",
    bunkai_kossu as "bunkai_kossu",
    bunkai_kosritu as "bunkai_kosritu",
    kose_itemcode as "kose_itemcode",
    koseiocode as "koseiocode",
    kosecode as "kosecode",
    suryo as "suryo",
    koseritsu as "koseritsu",
    mds_itemcode as "mds_itemcode",
    mds_itemname as "mds_itemname",
    attr01 as "attr01",
    attr02 as "attr02",
    attr03 as "attr03",
    attr04 as "attr04",
    attr05 as "attr05",
    attr06 as "attr06",
    attr07 as "attr07",
    attr08 as "attr08",
    attr09 as "attr09",
    attr10 as "attr10",
    i13_itemcode as "i13_itemcode",
    settanpinsetkbn as "settanpinsetkbn",
    teikikeiyaku as "teikikeiyaku",
    f as "f",
    latest_stage as "latest_stage",
    stage_ym as "stage_ym",
    y_order_f as "y_order_f",
    y_ship_f as "y_ship_f"
from dev_dna_core.jpdcledw_integration.dm_kesai_mart_dly;
create or replace view DM_KESAI_MART_DLY_GENERAL(
	"kokyano",
	"saleno_key",
	"saleno",
	"gyono",
	"bun_gyono",
	"order_dt",
	"ship_dt",
	"tokuiname",
	"storename",
	"f_order",
	"f_ship445",
	"channel",
	"dspromcode",
	"dspromname",
	"juchkbn",
	"meisaikbn",
	"h_o_item_code",
	"h_o_item_name",
	"h_o_item_cname",
	"h_o_item_anbun_qty",
	"h_item_code",
	"z_o_item_code",
	"z_item_code",
	"ratio",
	"z_item_suryo",
	"z_item_hen_suryo",
	"anbun_amount_tax110_ex",
	"z_item_amount_tax_ex",
	"ratio2",
	"anbun_soryo",
	"anbun_point_tax_ex",
	"anbun_tokuten",
	"gts",
	"gts_qty",
	"ciw_discount",
	"ciw_point",
	"ciw_return",
	"ciw_return_qty",
	"nts",
	"teikikeiyaku",
	"f",
	"meisainukikingaku",
	"warimaenukikingaku",
	"soryo",
	"point",
	"discount",
	"h_o_suryo",
	"hensu",
	"y_order_f",
	"y_ship_f",
	"h_koseritsu",
	"z_koseritsu",
	"h_bun_suryo",
	"z_bun_suryo",
	"c_diregularcontractid",
	"smkeiroid",
	"uketsuketelcompanycd",
	"diordercode",
	"kesaiid",
	"diorderid",
	"new_discount",
	"ciw_discount_notax",
	"sub_cnt",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select kokyano as "kokyano",
    saleno_key as "saleno_key",
    saleno as "saleno",
    gyono as "gyono",
    bun_gyono as "bun_gyono",
    order_dt as "order_dt",
    ship_dt as "ship_dt",
    tokuiname as "tokuiname",
    storename as "storename",
    f_order as "f_order",
    f_ship445 as "f_ship445",
    channel as "channel",
    dspromcode as "dspromcode",
    dspromname as "dspromname",
    juchkbn as "juchkbn",
    meisaikbn as "meisaikbn",
    h_o_item_code as "h_o_item_code",
    h_o_item_name as "h_o_item_name",
    h_o_item_cname as "h_o_item_cname",
    h_o_item_anbun_qty as "h_o_item_anbun_qty",
    h_item_code as "h_item_code",
    z_o_item_code as "z_o_item_code",
    z_item_code as "z_item_code",
    ratio as "ratio",
    z_item_suryo as "z_item_suryo",
    z_item_hen_suryo as "z_item_hen_suryo",
    anbun_amount_tax110_ex as "anbun_amount_tax110_ex",
    z_item_amount_tax_ex as "z_item_amount_tax_ex",
    ratio2 as "ratio2",
    anbun_soryo as "anbun_soryo",
    anbun_point_tax_ex as "anbun_point_tax_ex",
    anbun_tokuten as "anbun_tokuten",
    gts as "gts",
    gts_qty as "gts_qty",
    ciw_discount as "ciw_discount",
    ciw_point as "ciw_point",
    ciw_return as "ciw_return",
    ciw_return_qty as "ciw_return_qty",
    nts as "nts",
    teikikeiyaku as "teikikeiyaku",
    f as "f",
    meisainukikingaku as "meisainukikingaku",
    warimaenukikingaku as "warimaenukikingaku",
    soryo as "soryo",
    point as "point",
    discount as "discount",
    h_o_suryo as "h_o_suryo",
    hensu as "hensu",
    y_order_f as "y_order_f",
    y_ship_f as "y_ship_f",
    h_koseritsu as "h_koseritsu",
    z_koseritsu as "z_koseritsu",
    h_bun_suryo as "h_bun_suryo",
    z_bun_suryo as "z_bun_suryo",
    c_diregularcontractid as "c_diregularcontractid",
    smkeiroid as "smkeiroid",
    uketsuketelcompanycd as "uketsuketelcompanycd",
    diordercode as "diordercode",
    kesaiid as "kesaiid",
    diorderid as "diorderid",
    new_discount as "new_discount",
    ciw_discount_notax as "ciw_discount_notax",
    sub_cnt as "sub_cnt",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdcledw_integration.dm_kesai_mart_dly_general;
create or replace view DM_KESAI_MART_DLY_GTS_V(
	"saleno",
	"出荷年月日（通常カレンダー基準",
	"j&j日付",
	"ship_jnj_month_view",
	"ship_jnj_month_name",
	"shipment_week_jnj",
	"shipment_week_jnj_number",
	"ship_jnj_month_year",
	"着荷日年月日（通常カレンダー基準",
	"delivery_jnj_month_view",
	"delivery_jnj_month_name",
	"delivery_week_jnj",
	"delivery_week_jnj_number",
	"delivery_jnj_month_year",
	"チャネル",
	"得意先",
	"販売商品",
	"販売商品区分",
	"商品構成",
	"在庫商品コード(事前)",
	"在庫商品コード",
	"受注区分コード",
	"受注区分名",
	"gts",
	"gts_qty",
	"ciw_return",
	"ciw_return_qty",
	"ciw_discount",
	"ciw_point",
	"nts",
	"店舗名",
	"品目分類値コード1",
	"品目分類値コード2",
	"品目分類値コード3",
	"品目グループ名",
	"部門7追加属性1",
	"部門7追加属性2",
	"部門7追加属性3",
	"部門7追加属性4",
	"部門7追加属性5",
	"部門7追加属性6",
	"部門7追加属性7",
	"部門7追加属性8",
	"部門7追加属性9",
	"部門7追加属性10"
) as 
select  
saleno as "saleno",
	"出荷年月日（通常カレンダー基準",
	"j&j日付",
ship_jnj_month_view as "ship_jnj_month_view",
ship_jnj_month_name as "ship_jnj_month_name",
shipment_week_jnj as "shipment_week_jnj",
shipment_week_jnj_number as "shipment_week_jnj_number",
ship_jnj_month_year as "ship_jnj_month_year",
	"着荷日年月日（通常カレンダー基準",
delivery_jnj_month_view as "delivery_jnj_month_view",
delivery_jnj_month_name as "delivery_jnj_month_name",
delivery_week_jnj as "delivery_week_jnj",
delivery_week_jnj_number as "delivery_week_jnj_number",
delivery_jnj_month_year as "delivery_jnj_month_year",
"チャネル",
"得意先",
"販売商品",
"販売商品区分",
"商品構成",
"在庫商品コード(事前)",
"在庫商品コード",
"受注区分コード",
"受注区分名",
	gts as "gts",
	gts_qty as "gts_qty",
	ciw_return as "ciw_return",
	ciw_return_qty as "ciw_return_qty",
	ciw_discount as "ciw_discount",
	ciw_point as "ciw_point",
	nts as "nts",
	"店舗名",
	"品目分類値コード1",
	"品目分類値コード2",
	"品目分類値コード3",
	"品目グループ名",
	"部門7追加属性1",
	"部門7追加属性2",
	"部門7追加属性3",
	"部門7追加属性4",
	"部門7追加属性5",
	"部門7追加属性6",
	"部門7追加属性7",
	"部門7追加属性8",
	"部門7追加属性9",
	"部門7追加属性10"
from dev_dna_core.jpdcledw_integration.dm_kesai_mart_dly_gts_v;
create or replace view DM_KESAI_MART_RAKUTEN(
	"kokyano",
	"saleno_key",
	"saleno",
	"order_dt",
	"channel",
	"juchkbn",
	"h_o_item_code",
	"h_o_item_name",
	"h_o_item_cname",
	"h_o_item_anbun_qty",
	"h_item_code",
	"z_item_code",
	"z_item_suryo",
	"gts",
	"gts_qty",
	"ciw_discount",
	"ciw_point",
	"ciw_return",
	"ciw_return_qty",
	"nts",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as 
select  
kokyano as "kokyano",
saleno_key as "saleno_key",
saleno as "saleno",
order_dt as "order_dt",
channel as "channel",
juchkbn as "juchkbn",
h_o_item_code as "h_o_item_code",
h_o_item_name as "h_o_item_name",
h_o_item_cname as "h_o_item_cname",
h_o_item_anbun_qty as "h_o_item_anbun_qty",
h_item_code as "h_item_code",
z_item_code as "z_item_code",
z_item_suryo as "z_item_suryo",
gts as "gts",
gts_qty as "gts_qty",
ciw_discount as "ciw_discount",
ciw_point as "ciw_point",
ciw_return as "ciw_return",
ciw_return_qty as "ciw_return_qty",
nts as "nts",
inserted_date as "inserted_date",
inserted_by as "inserted_by",
updated_date as "updated_date",
updated_by as "updated_by"
from dev_dna_core.jpdcledw_integration.dm_kesai_mart_rakuten;
create or replace view DM_USER_ATTR(
	"kokyano",
	"zipcode",
	"todofukenname",
	"seibetukbn",
	"birthday",
	"birthday_md",
	"kokyakbn",
	"dmtenpoflg",
	"dmtsuhanflg",
	"teltenpoflg",
	"teltsuhanflg",
	"firstmediacode",
	"taikai_flg",
	"webno",
	"order_cnt_call_1m",
	"order_amt_call_1m",
	"point_usage_cnt_call_1m",
	"point_usage_amt_call_1m",
	"coupon_usage_cnt_call_1m",
	"coupon_usage_amt_call_1m",
	"order_cnt_call_3m",
	"order_amt_call_3m",
	"point_usage_cnt_call_3m",
	"point_usage_amt_call_3m",
	"coupon_usage_cnt_call_3m",
	"coupon_usage_amt_call_3m",
	"order_cnt_call_6m",
	"order_amt_call_6m",
	"point_usage_cnt_call_6m",
	"point_usage_amt_call_6m",
	"coupon_usage_cnt_call_6m",
	"coupon_usage_amt_call_6m",
	"order_cnt_call_1y",
	"order_amt_call_1y",
	"point_usage_cnt_call_1y",
	"point_usage_amt_call_1y",
	"coupon_usage_cnt_call_1y",
	"coupon_usage_amt_call_1y",
	"order_cnt_call_2y",
	"order_amt_call_2y",
	"point_usage_cnt_call_2y",
	"point_usage_amt_call_2y",
	"coupon_usage_cnt_call_2y",
	"coupon_usage_amt_call_2y",
	"order_cnt_call",
	"order_amt_call",
	"point_usage_cnt_call",
	"point_usage_amt_call",
	"coupon_usage_cnt_call",
	"coupon_usage_amt_call",
	"order_cnt_web_1m",
	"order_amt_web_1m",
	"point_usage_cnt_web_1m",
	"point_usage_amt_web_1m",
	"coupon_usage_cnt_web_1m",
	"coupon_usage_amt_web_1m",
	"order_cnt_web_3m",
	"order_amt_web_3m",
	"point_usage_cnt_web_3m",
	"point_usage_amt_web_3m",
	"coupon_usage_cnt_web_3m",
	"coupon_usage_amt_web_3m",
	"order_cnt_web_6m",
	"order_amt_web_6m",
	"point_usage_cnt_web_6m",
	"point_usage_amt_web_6m",
	"coupon_usage_cnt_web_6m",
	"coupon_usage_amt_web_6m",
	"order_cnt_web_1y",
	"order_amt_web_1y",
	"point_usage_cnt_web_1y",
	"point_usage_amt_web_1y",
	"coupon_usage_cnt_web_1y",
	"coupon_usage_amt_web_1y",
	"order_cnt_web_2y",
	"order_amt_web_2y",
	"point_usage_cnt_web_2y",
	"point_usage_amt_web_2y",
	"coupon_usage_cnt_web_2y",
	"coupon_usage_amt_web_2y",
	"order_cnt_web",
	"order_amt_web",
	"point_usage_cnt_web",
	"point_usage_amt_web",
	"coupon_usage_cnt_web",
	"coupon_usage_amt_web",
	"order_cnt_store_1m",
	"order_amt_store_1m",
	"point_usage_cnt_store_1m",
	"point_usage_amt_store_1m",
	"coupon_usage_cnt_store_1m",
	"coupon_usage_amt_store_1m",
	"order_cnt_store_3m",
	"order_amt_store_3m",
	"point_usage_cnt_store_3m",
	"point_usage_amt_store_3m",
	"coupon_usage_cnt_store_3m",
	"coupon_usage_amt_store_3m",
	"order_cnt_store_6m",
	"order_amt_store_6m",
	"point_usage_cnt_store_6m",
	"point_usage_amt_store_6m",
	"coupon_usage_cnt_store_6m",
	"coupon_usage_amt_store_6m",
	"order_cnt_store_1y",
	"order_amt_store_1y",
	"point_usage_cnt_store_1y",
	"point_usage_amt_store_1y",
	"coupon_usage_cnt_store_1y",
	"coupon_usage_amt_store_1y",
	"order_cnt_store_2y",
	"order_amt_store_2y",
	"point_usage_cnt_store_2y",
	"point_usage_amt_store_2y",
	"coupon_usage_cnt_store_2y",
	"coupon_usage_amt_store_2y",
	"order_cnt_store",
	"order_amt_store",
	"point_usage_cnt_store",
	"point_usage_amt_store",
	"coupon_usage_cnt_store",
	"coupon_usage_amt_store",
	"ltv_order_cnt_call",
	"ltv_order_cnt_web",
	"ltv_order_cnt_store",
	"i_order_dt",
	"l_order_dt",
	"i_channel",
	"l_channel",
	"lbag_reaction_cnt",
	"i_lbag_order_dt",
	"l_lbag_order_dt",
	"outlet_reaction_cnt",
	"i_outlet_order_dt",
	"l_outlet_order_dt",
	"coupon_usage_ratio",
	"coupon_usage_amt_average",
	"i_coupon_usage_dt",
	"l_coupon_usage_dt",
	"point_usage_ratio",
	"tradeup_date_vc100",
	"tradedown_date_vc100",
	"standard_new_date_vc100",
	"premium_new_date_vc100",
	"trade_flag_vc100",
	"tradeup_date_aid",
	"tradedown_date_aid",
	"standard_new_date_aid",
	"premium_new_date_aid",
	"trade_flag_aid",
	"tradeup_date_acgel",
	"tradedown_date_acgel",
	"standard_new_date_acgel",
	"premium_new_date_acgel",
	"trade_flag_acgel",
	"i_order_dt_inc_sample",
	"l_order_dt_web",
	"l_order_dt_call",
	"l_order_dt_store",
	"register_dt",
	"biken_flg",
	"register_card_flg",
	"other_adv_flg",
	"main_store",
	"familysale_class",
	"main_channel_bddm",
	"promo_stage",
	"next_promo_stage_amt",
	"next_promo_stage_point",
	"point_tobe_granted",
	"stage",
	"stage_monthly",
	"totalprc_this_year",
	"expired_point_this_month",
	"expired_point_next_month",
	"outcall_exc_flg",
	"outcall_hist_flg_3m",
	"subscription_flg",
	"ecpropensity",
	"ccpropensity",
	"acgelpropensity",
	"vc100propensity",
	"cluster5_cd",
	"cluster5_nm",
	"order_amt_call_1y_term_start",
	"order_amt_web_1y_term_start",
	"order_amt_store_1y_term_start",
	"order_cnt_call_this_year",
	"order_cnt_web_this_year",
	"order_cnt_store_this_year",
	"order_sku_cnt_1y",
	"order_sku_cnt_1y_term_start",
	"order_cnt_call_exc_sub_1y",
	"order_cnt_web_exc_sub_1y",
	"order_cnt_store_exc_sub_1y"
) as
select kokyano as "kokyano",
    zipcode as "zipcode",
    todofukenname as "todofukenname",
    seibetukbn as "seibetukbn",
    birthday as "birthday",
    birthday_md as "birthday_md",
    kokyakbn as "kokyakbn",
    dmtenpoflg as "dmtenpoflg",
    dmtsuhanflg as "dmtsuhanflg",
    teltenpoflg as "teltenpoflg",
    teltsuhanflg as "teltsuhanflg",
    firstmediacode as "firstmediacode",
    taikai_flg as "taikai_flg",
    webno as "webno",
    order_cnt_call_1m as "order_cnt_call_1m",
    order_amt_call_1m as "order_amt_call_1m",
    point_usage_cnt_call_1m as "point_usage_cnt_call_1m",
    point_usage_amt_call_1m as "point_usage_amt_call_1m",
    coupon_usage_cnt_call_1m as "coupon_usage_cnt_call_1m",
    coupon_usage_amt_call_1m as "coupon_usage_amt_call_1m",
    order_cnt_call_3m as "order_cnt_call_3m",
    order_amt_call_3m as "order_amt_call_3m",
    point_usage_cnt_call_3m as "point_usage_cnt_call_3m",
    point_usage_amt_call_3m as "point_usage_amt_call_3m",
    coupon_usage_cnt_call_3m as "coupon_usage_cnt_call_3m",
    coupon_usage_amt_call_3m as "coupon_usage_amt_call_3m",
    order_cnt_call_6m as "order_cnt_call_6m",
    order_amt_call_6m as "order_amt_call_6m",
    point_usage_cnt_call_6m as "point_usage_cnt_call_6m",
    point_usage_amt_call_6m as "point_usage_amt_call_6m",
    coupon_usage_cnt_call_6m as "coupon_usage_cnt_call_6m",
    coupon_usage_amt_call_6m as "coupon_usage_amt_call_6m",
    order_cnt_call_1y as "order_cnt_call_1y",
    order_amt_call_1y as "order_amt_call_1y",
    point_usage_cnt_call_1y as "point_usage_cnt_call_1y",
    point_usage_amt_call_1y as "point_usage_amt_call_1y",
    coupon_usage_cnt_call_1y as "coupon_usage_cnt_call_1y",
    coupon_usage_amt_call_1y as "coupon_usage_amt_call_1y",
    order_cnt_call_2y as "order_cnt_call_2y",
    order_amt_call_2y as "order_amt_call_2y",
    point_usage_cnt_call_2y as "point_usage_cnt_call_2y",
    point_usage_amt_call_2y as "point_usage_amt_call_2y",
    coupon_usage_cnt_call_2y as "coupon_usage_cnt_call_2y",
    coupon_usage_amt_call_2y as "coupon_usage_amt_call_2y",
    order_cnt_call as "order_cnt_call",
    order_amt_call as "order_amt_call",
    point_usage_cnt_call as "point_usage_cnt_call",
    point_usage_amt_call as "point_usage_amt_call",
    coupon_usage_cnt_call as "coupon_usage_cnt_call",
    coupon_usage_amt_call as "coupon_usage_amt_call",
    order_cnt_web_1m as "order_cnt_web_1m",
    order_amt_web_1m as "order_amt_web_1m",
    point_usage_cnt_web_1m as "point_usage_cnt_web_1m",
    point_usage_amt_web_1m as "point_usage_amt_web_1m",
    coupon_usage_cnt_web_1m as "coupon_usage_cnt_web_1m",
    coupon_usage_amt_web_1m as "coupon_usage_amt_web_1m",
    order_cnt_web_3m as "order_cnt_web_3m",
    order_amt_web_3m as "order_amt_web_3m",
    point_usage_cnt_web_3m as "point_usage_cnt_web_3m",
    point_usage_amt_web_3m as "point_usage_amt_web_3m",
    coupon_usage_cnt_web_3m as "coupon_usage_cnt_web_3m",
    coupon_usage_amt_web_3m as "coupon_usage_amt_web_3m",
    order_cnt_web_6m as "order_cnt_web_6m",
    order_amt_web_6m as "order_amt_web_6m",
    point_usage_cnt_web_6m as "point_usage_cnt_web_6m",
    point_usage_amt_web_6m as "point_usage_amt_web_6m",
    coupon_usage_cnt_web_6m as "coupon_usage_cnt_web_6m",
    coupon_usage_amt_web_6m as "coupon_usage_amt_web_6m",
    order_cnt_web_1y as "order_cnt_web_1y",
    order_amt_web_1y as "order_amt_web_1y",
    point_usage_cnt_web_1y as "point_usage_cnt_web_1y",
    point_usage_amt_web_1y as "point_usage_amt_web_1y",
    coupon_usage_cnt_web_1y as "coupon_usage_cnt_web_1y",
    coupon_usage_amt_web_1y as "coupon_usage_amt_web_1y",
    order_cnt_web_2y as "order_cnt_web_2y",
    order_amt_web_2y as "order_amt_web_2y",
    point_usage_cnt_web_2y as "point_usage_cnt_web_2y",
    point_usage_amt_web_2y as "point_usage_amt_web_2y",
    coupon_usage_cnt_web_2y as "coupon_usage_cnt_web_2y",
    coupon_usage_amt_web_2y as "coupon_usage_amt_web_2y",
    order_cnt_web as "order_cnt_web",
    order_amt_web as "order_amt_web",
    point_usage_cnt_web as "point_usage_cnt_web",
    point_usage_amt_web as "point_usage_amt_web",
    coupon_usage_cnt_web as "coupon_usage_cnt_web",
    coupon_usage_amt_web as "coupon_usage_amt_web",
    order_cnt_store_1m as "order_cnt_store_1m",
    order_amt_store_1m as "order_amt_store_1m",
    point_usage_cnt_store_1m as "point_usage_cnt_store_1m",
    point_usage_amt_store_1m as "point_usage_amt_store_1m",
    coupon_usage_cnt_store_1m as "coupon_usage_cnt_store_1m",
    coupon_usage_amt_store_1m as "coupon_usage_amt_store_1m",
    order_cnt_store_3m as "order_cnt_store_3m",
    order_amt_store_3m as "order_amt_store_3m",
    point_usage_cnt_store_3m as "point_usage_cnt_store_3m",
    point_usage_amt_store_3m as "point_usage_amt_store_3m",
    coupon_usage_cnt_store_3m as "coupon_usage_cnt_store_3m",
    coupon_usage_amt_store_3m as "coupon_usage_amt_store_3m",
    order_cnt_store_6m as "order_cnt_store_6m",
    order_amt_store_6m as "order_amt_store_6m",
    point_usage_cnt_store_6m as "point_usage_cnt_store_6m",
    point_usage_amt_store_6m as "point_usage_amt_store_6m",
    coupon_usage_cnt_store_6m as "coupon_usage_cnt_store_6m",
    coupon_usage_amt_store_6m as "coupon_usage_amt_store_6m",
    order_cnt_store_1y as "order_cnt_store_1y",
    order_amt_store_1y as "order_amt_store_1y",
    point_usage_cnt_store_1y as "point_usage_cnt_store_1y",
    point_usage_amt_store_1y as "point_usage_amt_store_1y",
    coupon_usage_cnt_store_1y as "coupon_usage_cnt_store_1y",
    coupon_usage_amt_store_1y as "coupon_usage_amt_store_1y",
    order_cnt_store_2y as "order_cnt_store_2y",
    order_amt_store_2y as "order_amt_store_2y",
    point_usage_cnt_store_2y as "point_usage_cnt_store_2y",
    point_usage_amt_store_2y as "point_usage_amt_store_2y",
    coupon_usage_cnt_store_2y as "coupon_usage_cnt_store_2y",
    coupon_usage_amt_store_2y as "coupon_usage_amt_store_2y",
    order_cnt_store as "order_cnt_store",
    order_amt_store as "order_amt_store",
    point_usage_cnt_store as "point_usage_cnt_store",
    point_usage_amt_store as "point_usage_amt_store",
    coupon_usage_cnt_store as "coupon_usage_cnt_store",
    coupon_usage_amt_store as "coupon_usage_amt_store",
    ltv_order_cnt_call as "ltv_order_cnt_call",
    ltv_order_cnt_web as "ltv_order_cnt_web",
    ltv_order_cnt_store as "ltv_order_cnt_store",
    i_order_dt as "i_order_dt",
    l_order_dt as "l_order_dt",
    i_channel as "i_channel",
    l_channel as "l_channel",
    lbag_reaction_cnt as "lbag_reaction_cnt",
    i_lbag_order_dt as "i_lbag_order_dt",
    l_lbag_order_dt as "l_lbag_order_dt",
    outlet_reaction_cnt as "outlet_reaction_cnt",
    i_outlet_order_dt as "i_outlet_order_dt",
    l_outlet_order_dt as "l_outlet_order_dt",
    coupon_usage_ratio as "coupon_usage_ratio",
    coupon_usage_amt_average as "coupon_usage_amt_average",
    i_coupon_usage_dt as "i_coupon_usage_dt",
    l_coupon_usage_dt as "l_coupon_usage_dt",
    point_usage_ratio as "point_usage_ratio",
    tradeup_date_vc100 as "tradeup_date_vc100",
    tradedown_date_vc100 as "tradedown_date_vc100",
    standard_new_date_vc100 as "standard_new_date_vc100",
    premium_new_date_vc100 as "premium_new_date_vc100",
    trade_flag_vc100 as "trade_flag_vc100",
    tradeup_date_aid as "tradeup_date_aid",
    tradedown_date_aid as "tradedown_date_aid",
    standard_new_date_aid as "standard_new_date_aid",
    premium_new_date_aid as "premium_new_date_aid",
    trade_flag_aid as "trade_flag_aid",
    tradeup_date_acgel as "tradeup_date_acgel",
    tradedown_date_acgel as "tradedown_date_acgel",
    standard_new_date_acgel as "standard_new_date_acgel",
    premium_new_date_acgel as "premium_new_date_acgel",
    trade_flag_acgel as "trade_flag_acgel",
    i_order_dt_inc_sample as "i_order_dt_inc_sample",
    l_order_dt_web as "l_order_dt_web",
    l_order_dt_call as "l_order_dt_call",
    l_order_dt_store as "l_order_dt_store",
    register_dt as "register_dt",
    biken_flg as "biken_flg",
    register_card_flg as "register_card_flg",
    other_adv_flg as "other_adv_flg",
    main_store as "main_store",
    familysale_class as "familysale_class",
    main_channel_bddm as "main_channel_bddm",
    promo_stage as "promo_stage",
    next_promo_stage_amt as "next_promo_stage_amt",
    next_promo_stage_point as "next_promo_stage_point",
    point_tobe_granted as "point_tobe_granted",
    stage as "stage",
    stage_monthly as "stage_monthly",
    totalprc_this_year as "totalprc_this_year",
    expired_point_this_month as "expired_point_this_month",
    expired_point_next_month as "expired_point_next_month",
    outcall_exc_flg as "outcall_exc_flg",
    outcall_hist_flg_3m as "outcall_hist_flg_3m",
    subscription_flg as "subscription_flg",
    ecpropensity as "ecpropensity",
    ccpropensity as "ccpropensity",
    acgelpropensity as "acgelpropensity",
    vc100propensity as "vc100propensity",
    cluster5_cd as "cluster5_cd",
    cluster5_nm as "cluster5_nm",
    order_amt_call_1y_term_start as "order_amt_call_1y_term_start",
    order_amt_web_1y_term_start as "order_amt_web_1y_term_start",
    order_amt_store_1y_term_start as "order_amt_store_1y_term_start",
    order_cnt_call_this_year as "order_cnt_call_this_year",
    order_cnt_web_this_year as "order_cnt_web_this_year",
    order_cnt_store_this_year as "order_cnt_store_this_year",
    order_sku_cnt_1y as "order_sku_cnt_1y",
    order_sku_cnt_1y_term_start as "order_sku_cnt_1y_term_start",
    order_cnt_call_exc_sub_1y as "order_cnt_call_exc_sub_1y",
    order_cnt_web_exc_sub_1y as "order_cnt_web_exc_sub_1y",
    order_cnt_store_exc_sub_1y as "order_cnt_store_exc_sub_1y"
from dev_dna_core.jpdcledw_integration.dm_user_attr;
create or replace view DM_USER_ATTR_SPENDING_TREND_V(
	"yyyy",
	"kokyano",
	"main_channel",
	"order_amt_term_start",
	"order_amt_term_end",
	"order_amt_total"
) as
select yyyy as "yyyy",
    kokyano as "kokyano",
    main_channel as "main_channel",
    order_amt_term_start as "order_amt_term_start",
    order_amt_term_end as "order_amt_term_end",
    order_amt_total as "order_amt_total"
from dev_dna_core.jpdcledw_integration.dm_user_attr_spending_trend_v;
create or replace view DM_USER_STATUS(
	"base",
	"kokyano",
	"dt",
	"status"
) as
select base as "base",
    kokyano as "kokyano",
    dt as "dt",
    status as "status"
from dev_dna_core.jpdcledw_integration.dm_user_status;
create or replace view EDW_MDS_JP_DCL_MT_H_PRODUCT(
	"ci-code",
	"happy bag flag",
	"outlet flag",
	"family sale flag",
	"flag01",
	"flag02",
	"flag03",
	"flag04",
	"flag05",
	"flag06",
	"flag07",
	"flag08",
	"flag09",
	"flag10",
	"description"
) as 
select  
"ci-code" as "ci-code",
"happy bag flag" as "happy bag flag",
"outlet flag"  as "outlet flag" ,
"family sale flag" as "family sale flag",
flag01 as "flag01",
flag02 as "flag02",
flag03 as "flag03",
flag04 as "flag04",
flag05 as "flag05",
flag06 as "flag06",
flag07 as "flag07",
flag08 as "flag08",
flag09 as "flag09",
flag10 as "flag10",
description as "description"
from dev_dna_core.jpdcledw_integration.edw_mds_jp_dcl_mt_h_product;
create or replace view EDW_MDS_JP_DCL_PRODUCT_MASTER(
	"id",
	"muid",
	"versionname",
	"versionnumber",
	"version_id",
	"versionflag",
	"name",
	"code",
	"changetrackingmask",
	"itemcode",
	"itemname",
	"attr01",
	"attr02",
	"attr03",
	"attr04",
	"attr05",
	"attr06",
	"attr07",
	"attr08",
	"attr09",
	"attr10",
	"column01",
	"column02",
	"column03",
	"column04",
	"column05",
	"column06",
	"column07",
	"column08",
	"column09",
	"column10",
	"column11",
	"column12",
	"column13",
	"column14",
	"column15",
	"column16",
	"column17",
	"column18",
	"column19",
	"column20",
	"column21",
	"column22",
	"column23",
	"enterdatetime",
	"enterusername",
	"enterversionnumber",
	"lastchgdatetime",
	"lastchgusername",
	"lastchgversionnumber",
	"validationstatus",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select id as "id",
    muid as "muid",
    versionname as "versionname",
    versionnumber as "versionnumber",
    version_id as "version_id",
    versionflag as "versionflag",
    name as "name",
    code as "code",
    changetrackingmask as "changetrackingmask",
    itemcode as "itemcode",
    itemname as "itemname",
    attr01 as "attr01",
    attr02 as "attr02",
    attr03 as "attr03",
    attr04 as "attr04",
    attr05 as "attr05",
    attr06 as "attr06",
    attr07 as "attr07",
    attr08 as "attr08",
    attr09 as "attr09",
    attr10 as "attr10",
    column01 as "column01",
    column02 as "column02",
    column03 as "column03",
    column04 as "column04",
    column05 as "column05",
    column06 as "column06",
    column07 as "column07",
    column08 as "column08",
    column09 as "column09",
    column10 as "column10",
    column11 as "column11",
    column12 as "column12",
    column13 as "column13",
    column14 as "column14",
    column15 as "column15",
    column16 as "column16",
    column17 as "column17",
    column18 as "column18",
    column19 as "column19",
    column20 as "column20",
    column21 as "column21",
    column22 as "column22",
    column23 as "column23",
    enterdatetime as "enterdatetime",
    enterusername as "enterusername",
    enterversionnumber as "enterversionnumber",
    lastchgdatetime as "lastchgdatetime",
    lastchgusername as "lastchgusername",
    lastchgversionnumber as "lastchgversionnumber",
    validationstatus as "validationstatus",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdcledw_integration.edw_mds_jp_dcl_product_master;
create or replace view ITEM_HANBAI_P_ZAIKO_C_V(
	"h_o_item_code",
	"z_item_code"
) as 
select  
h_o_item_code as "h_o_item_code",
z_item_code as "z_item_code"
from dev_dna_core.jpdcledw_integration.item_hanbai_p_zaiko_c_v;
create or replace view ITEM_HANBAI_V(
	"h_itemcode",
	"h_itemname",
	"diid",
	"tanka_sales",
	"syutoku_kbn"
) as 
select  
h_itemcode as "h_itemcode",
h_itemname as "h_itemname",
diid as "diid",
tanka_sales as "tanka_sales",
syutoku_kbn as "syutoku_kbn"
from dev_dna_core.jpdcledw_integration.item_hanbai_v;
create or replace view ITEM_ZAIKO_V(
	"z_itemcode",
	"z_itemname",
	"itemkbn",
	"syutoku_kbn",
	"bumon7_add_attr1",
	"bumon7_add_attr2",
	"bumon7_add_attr3",
	"bumon7_add_attr4",
	"bumon7_add_attr5",
	"bumon7_add_attr6",
	"bumon7_add_attr7",
	"bumon7_add_attr8",
	"bumon7_add_attr9",
	"bumon7_add_attr10"
) as 
select  
z_itemcode as "z_itemcode",
z_itemname as "z_itemname",
itemkbn as "itemkbn",
syutoku_kbn as "syutoku_kbn",
bumon7_add_attr1 as "bumon7_add_attr1",
bumon7_add_attr2 as "bumon7_add_attr2",
bumon7_add_attr3 as "bumon7_add_attr3",
bumon7_add_attr4 as "bumon7_add_attr4",
bumon7_add_attr5 as "bumon7_add_attr5",
bumon7_add_attr6 as "bumon7_add_attr6",
bumon7_add_attr7 as "bumon7_add_attr7",
bumon7_add_attr8 as "bumon7_add_attr8",
bumon7_add_attr9 as "bumon7_add_attr9",
bumon7_add_attr10 as "bumon7_add_attr10"
from dev_dna_core.jpdcledw_integration.item_zaiko_v;
create or replace view KESAI_H_DATA_MART_MV(
	"saleno_key",
	"saleno",
	"juchkbn",
	"juchym",
	"juchdate",
	"juchquarter",
	"juchjigyoki",
	"kokyano",
	"torikeikbn",
	"cancelflg",
	"hanrocode",
	"syohanrobunname",
	"chuhanrobunname",
	"daihanrobunname",
	"mediacode",
	"soryo",
	"tax",
	"sogokei",
	"tenpocode",
	"shukaym",
	"shukadate",
	"shukaquarter",
	"shukajigyoki",
	"zipcode",
	"todofukencode",
	"riyopoint",
	"happenpoint",
	"kessaikbn",
	"cardcorpcode",
	"henreasoncode",
	"motoinsertid",
	"motoinsertdate",
	"motoupdatedate",
	"insertdate",
	"inserttime",
	"insertid",
	"updatedate",
	"updatetime",
	"updateid",
	"rank",
	"dispsaleno",
	"kesaiid",
	"ordercode",
	"maker",
	"todofuken_code",
	"henreasonname",
	"uketsukeusrid",
	"uketsuketelcompanycd",
	"smkeiroid",
	"dipromid",
	"saleno_trm",
	"dicollectprc",
	"ditoujitsuhaisoprc",
	"didiscountall",
	"c_didiscountprc",
	"point_exchange",
	"logincode",
	"shukkasts",
	"divouchercode",
	"ditaxrate",
	"diseikyuremain",
	"dinyukinsts",
	"dicardnyukinsts",
	"disokoid",
	"dihaisokeitai",
	"shukadate_p",
	"kakokbn",
	"port_uniq_flg"
) as
select saleno_key as "saleno_key",
    saleno as "saleno",
    juchkbn as "juchkbn",
    juchym as "juchym",
    juchdate as "juchdate",
    juchquarter as "juchquarter",
    juchjigyoki as "juchjigyoki",
    kokyano as "kokyano",
    torikeikbn as "torikeikbn",
    cancelflg as "cancelflg",
    hanrocode as "hanrocode",
    syohanrobunname as "syohanrobunname",
    chuhanrobunname as "chuhanrobunname",
    daihanrobunname as "daihanrobunname",
    mediacode as "mediacode",
    soryo as "soryo",
    tax as "tax",
    sogokei as "sogokei",
    tenpocode as "tenpocode",
    shukaym as "shukaym",
    shukadate as "shukadate",
    shukaquarter as "shukaquarter",
    shukajigyoki as "shukajigyoki",
    zipcode as "zipcode",
    todofukencode as "todofukencode",
    riyopoint as "riyopoint",
    happenpoint as "happenpoint",
    kessaikbn as "kessaikbn",
    cardcorpcode as "cardcorpcode",
    henreasoncode as "henreasoncode",
    motoinsertid as "motoinsertid",
    motoinsertdate as "motoinsertdate",
    motoupdatedate as "motoupdatedate",
    insertdate as "insertdate",
    inserttime as "inserttime",
    insertid as "insertid",
    updatedate as "updatedate",
    updatetime as "updatetime",
    updateid as "updateid",
    rank as "rank",
    dispsaleno as "dispsaleno",
    kesaiid as "kesaiid",
    ordercode as "ordercode",
    maker as "maker",
    todofuken_code as "todofuken_code",
    henreasonname as "henreasonname",
    uketsukeusrid as "uketsukeusrid",
    uketsuketelcompanycd as "uketsuketelcompanycd",
    smkeiroid as "smkeiroid",
    dipromid as "dipromid",
    saleno_trm as "saleno_trm",
    dicollectprc as "dicollectprc",
    ditoujitsuhaisoprc as "ditoujitsuhaisoprc",
    didiscountall as "didiscountall",
    c_didiscountprc as "c_didiscountprc",
    point_exchange as "point_exchange",
    logincode as "logincode",
    shukkasts as "shukkasts",
    divouchercode as "divouchercode",
    ditaxrate as "ditaxrate",
    diseikyuremain as "diseikyuremain",
    dinyukinsts as "dinyukinsts",
    dicardnyukinsts as "dicardnyukinsts",
    disokoid as "disokoid",
    dihaisokeitai as "dihaisokeitai",
    shukadate_p as "shukadate_p",
    kakokbn as "kakokbn",
    port_uniq_flg as "port_uniq_flg"
from dev_dna_core.jpdcledw_integration.kesai_h_data_mart_mv;
create or replace view KESAI_H_DATA_MART_MV_KIZUNA(
	"saleno_key",
	"saleno",
	"juchkbn",
	"juchym",
	"juchdate",
	"juchquarter",
	"juchjigyoki",
	"kokyano",
	"torikeikbn",
	"cancelflg",
	"hanrocode",
	"syohanrobunname",
	"chuhanrobunname",
	"daihanrobunname",
	"mediacode",
	"soryo",
	"tax",
	"sogokei",
	"tenpocode",
	"shukaym",
	"shukadate",
	"shukaquarter",
	"shukajigyoki",
	"zipcode",
	"todofukencode",
	"riyopoint",
	"happenpoint",
	"kessaikbn",
	"cardcorpcode",
	"henreasoncode",
	"motoinsertid",
	"motoinsertdate",
	"motoupdatedate",
	"insertdate",
	"inserttime",
	"insertid",
	"updatedate",
	"updatetime",
	"updateid",
	"rank",
	"dispsaleno",
	"kesaiid",
	"ordercode",
	"maker",
	"todofuken_code",
	"henreasonname",
	"uketsukeusrid",
	"uketsuketelcompanycd",
	"smkeiroid",
	"dipromid",
	"saleno_trm",
	"dicollectprc",
	"ditoujitsuhaisoprc",
	"didiscountall",
	"c_didiscountprc",
	"point_exchange",
	"logincode",
	"shukkasts",
	"divouchercode",
	"ditaxrate",
	"diseikyuremain",
	"dinyukinsts",
	"dicardnyukinsts",
	"disokoid",
	"dihaisokeitai",
	"shukadate_p",
	"kakokbn",
	"port_uniq_flg"
) as 
select saleno_key as "saleno_key",
    saleno as "saleno",
    juchkbn as "juchkbn",
    juchym as "juchym",
    juchdate as "juchdate",
    juchquarter as "juchquarter",
    juchjigyoki as "juchjigyoki",
    kokyano as "kokyano",
    torikeikbn as "torikeikbn",
    cancelflg as "cancelflg",
    hanrocode as "hanrocode",
    syohanrobunname as "syohanrobunname",
    chuhanrobunname as "chuhanrobunname",
    daihanrobunname as "daihanrobunname",
    mediacode as "mediacode",
    soryo as "soryo",
    tax as "tax",
    sogokei as "sogokei",
    tenpocode as "tenpocode",
    shukaym as "shukaym",
    shukadate as "shukadate",
    shukaquarter as "shukaquarter",
    shukajigyoki as "shukajigyoki",
    zipcode as "zipcode",
    todofukencode as "todofukencode",
    riyopoint as "riyopoint",
    happenpoint as "happenpoint",
    kessaikbn as "kessaikbn",
    cardcorpcode as "cardcorpcode",
    henreasoncode as "henreasoncode",
    motoinsertid as "motoinsertid",
    motoinsertdate as "motoinsertdate",
    motoupdatedate as "motoupdatedate",
    insertdate as "insertdate",
    inserttime as "inserttime",
    insertid as "insertid",
    updatedate as "updatedate",
    updatetime as "updatetime",
    updateid as "updateid",
    rank as "rank",
    dispsaleno as "dispsaleno",
    kesaiid as "kesaiid",
    ordercode as "ordercode",
    maker as "maker",
    todofuken_code as "todofuken_code",
    henreasonname as "henreasonname",
    uketsukeusrid as "uketsukeusrid",
    uketsuketelcompanycd as "uketsuketelcompanycd",
    smkeiroid as "smkeiroid",
    dipromid as "dipromid",
    saleno_trm as "saleno_trm",
    dicollectprc as "dicollectprc",
    ditoujitsuhaisoprc as "ditoujitsuhaisoprc",
    didiscountall as "didiscountall",
    c_didiscountprc as "c_didiscountprc",
    point_exchange as "point_exchange",
    logincode as "logincode",
    shukkasts as "shukkasts",
    divouchercode as "divouchercode",
    ditaxrate as "ditaxrate",
    diseikyuremain as "diseikyuremain",
    dinyukinsts as "dinyukinsts",
    dicardnyukinsts as "dicardnyukinsts",
    disokoid as "disokoid",
    dihaisokeitai as "dihaisokeitai",
    shukadate_p as "shukadate_p",
    kakokbn as "kakokbn",
    port_uniq_flg as "port_uniq_flg"
from dev_dna_core.jpdcledw_integration.kesai_h_data_mart_mv_kizuna;
create or replace view KESAI_M_DATA_MART_MV(
	"saleno_key",
	"saleno",
	"gyono",
	"bun_gyono",
	"meisaikbn",
	"itemcode",
	"setitemnm",
	"bun_itemcode",
	"diid",
	"disetid",
	"suryo",
	"tanka",
	"kingaku",
	"meisainukikingaku",
	"wariritu",
	"hensu",
	"warimaekomitanka",
	"warimaekomikingaku",
	"warimaenukikingaku",
	"meisaitax",
	"dispsaleno",
	"kesaiid",
	"saleno_trim",
	"diorderid",
	"henpinsts",
	"c_dspointitemflg",
	"c_diitemtype",
	"c_diadjustprc",
	"ditotalprc",
	"c_diitemtotalprc",
	"c_didiscountmeisai",
	"bun_suryo",
	"bun_tanka",
	"bun_kingaku",
	"bun_meisainukikingaku",
	"bun_wariritu",
	"bun_hensu",
	"bun_warimaekomitanka",
	"bun_warimaekomikingaku",
	"bun_warimaenukikingaku",
	"bun_meisaitax",
	"maker",
	"kakokbn",
	"saleno_p",
	"gyono_p",
	"bun_gyono_p"
) as
select saleno_key as "saleno_key",
    saleno as "saleno",
    gyono as "gyono",
    bun_gyono as "bun_gyono",
    meisaikbn as "meisaikbn",
    itemcode as "itemcode",
    setitemnm as "setitemnm",
    bun_itemcode as "bun_itemcode",
    diid as "diid",
    disetid as "disetid",
    suryo as "suryo",
    tanka as "tanka",
    kingaku as "kingaku",
    meisainukikingaku as "meisainukikingaku",
    wariritu as "wariritu",
    hensu as "hensu",
    warimaekomitanka as "warimaekomitanka",
    warimaekomikingaku as "warimaekomikingaku",
    warimaenukikingaku as "warimaenukikingaku",
    meisaitax as "meisaitax",
    dispsaleno as "dispsaleno",
    kesaiid as "kesaiid",
    saleno_trim as "saleno_trim",
    diorderid as "diorderid",
    henpinsts as "henpinsts",
    c_dspointitemflg as "c_dspointitemflg",
    c_diitemtype as "c_diitemtype",
    c_diadjustprc as "c_diadjustprc",
    ditotalprc as "ditotalprc",
    c_diitemtotalprc as "c_diitemtotalprc",
    c_didiscountmeisai as "c_didiscountmeisai",
    bun_suryo as "bun_suryo",
    bun_tanka as "bun_tanka",
    bun_kingaku as "bun_kingaku",
    bun_meisainukikingaku as "bun_meisainukikingaku",
    bun_wariritu as "bun_wariritu",
    bun_hensu as "bun_hensu",
    bun_warimaekomitanka as "bun_warimaekomitanka",
    bun_warimaekomikingaku as "bun_warimaekomikingaku",
    bun_warimaenukikingaku as "bun_warimaenukikingaku",
    bun_meisaitax as "bun_meisaitax",
    maker as "maker",
    kakokbn as "kakokbn",
    saleno_p as "saleno_p",
    gyono_p as "gyono_p",
    bun_gyono_p as "bun_gyono_p"
from dev_dna_core.jpdcledw_integration.kesai_m_data_mart_mv;
create or replace view KESAI_M_DATA_MART_MV_KIZUNA(
	"saleno_key",
	"saleno",
	"gyono",
	"bun_gyono",
	"meisaikbn",
	"itemcode",
	"setitemnm",
	"bun_itemcode",
	"diid",
	"disetid",
	"suryo",
	"tanka",
	"kingaku",
	"meisainukikingaku",
	"wariritu",
	"hensu",
	"warimaekomitanka",
	"warimaekomikingaku",
	"warimaenukikingaku",
	"meisaitax",
	"dispsaleno",
	"kesaiid",
	"saleno_trim",
	"diorderid",
	"henpinsts",
	"c_dspointitemflg",
	"c_diitemtype",
	"c_diadjustprc",
	"ditotalprc",
	"c_diitemtotalprc",
	"c_didiscountmeisai",
	"bun_suryo",
	"bun_tanka",
	"bun_kingaku",
	"bun_meisainukikingaku",
	"bun_wariritu",
	"bun_hensu",
	"bun_warimaekomitanka",
	"bun_warimaekomikingaku",
	"bun_warimaenukikingaku",
	"bun_meisaitax",
	"maker",
	"kakokbn",
	"saleno_p",
	"gyono_p",
	"bun_gyono_p"
) as
select saleno_key as "saleno_key",
    saleno as "saleno",
    gyono as "gyono",
    bun_gyono as "bun_gyono",
    meisaikbn as "meisaikbn",
    itemcode as "itemcode",
    setitemnm as "setitemnm",
    bun_itemcode as "bun_itemcode",
    diid as "diid",
    disetid as "disetid",
    suryo as "suryo",
    tanka as "tanka",
    kingaku as "kingaku",
    meisainukikingaku as "meisainukikingaku",
    wariritu as "wariritu",
    hensu as "hensu",
    warimaekomitanka as "warimaekomitanka",
    warimaekomikingaku as "warimaekomikingaku",
    warimaenukikingaku as "warimaenukikingaku",
    meisaitax as "meisaitax",
    dispsaleno as "dispsaleno",
    kesaiid as "kesaiid",
    saleno_trim as "saleno_trim",
    diorderid as "diorderid",
    henpinsts as "henpinsts",
    c_dspointitemflg as "c_dspointitemflg",
    c_diitemtype as "c_diitemtype",
    c_diadjustprc as "c_diadjustprc",
    ditotalprc as "ditotalprc",
    c_diitemtotalprc as "c_diitemtotalprc",
    c_didiscountmeisai as "c_didiscountmeisai",
    bun_suryo as "bun_suryo",
    bun_tanka as "bun_tanka",
    bun_kingaku as "bun_kingaku",
    bun_meisainukikingaku as "bun_meisainukikingaku",
    bun_wariritu as "bun_wariritu",
    bun_hensu as "bun_hensu",
    bun_warimaekomitanka as "bun_warimaekomitanka",
    bun_warimaekomikingaku as "bun_warimaekomikingaku",
    bun_warimaenukikingaku as "bun_warimaenukikingaku",
    bun_meisaitax as "bun_meisaitax",
    maker as "maker",
    kakokbn as "kakokbn",
    saleno_p as "saleno_p",
    gyono_p as "gyono_p",
    bun_gyono_p as "bun_gyono_p"
from dev_dna_core.jpdcledw_integration.kesai_m_data_mart_mv_kizuna;
create or replace view KR_054_CAL_V(
	"yymm",
	"db_refresh_date"
) as 
select  
yymm as "yymm",
db_refresh_date as "db_refresh_date"
from dev_dna_core.jpdcledw_integration.kr_054_cal_v;
create or replace view KR_054_PFUYO_SUM(
	"fuyo_label",
	"point_ym",
	"point_yy",
	"point_mm",
	"point"
) as 
select  
fuyo_label as "fuyo_label",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
point as "point"
from dev_dna_core.jpdcledw_integration.kr_054_pfuyo_sum;
create or replace view KR_054_PLOST_SUM(
	"lost_label",
	"lost_ym",
	"lost_yy",
	"lost_mm",
	"lost_calc_yy",
	"lost_calc_mm",
	"lpoint"
) as 
select  
lost_label as "lost_label",
lost_ym as "lost_ym",
lost_yy as "lost_yy",
lost_mm as "lost_mm",
lost_calc_yy as "lost_calc_yy",
lost_calc_mm as "lost_calc_mm",
lpoint as "lpoint"
from dev_dna_core.jpdcledw_integration.kr_054_plost_sum;
create or replace view KR_054_PLYOTEI_SUM(
	"yotei_label",
	"yotei_ym",
	"yotei_yy",
	"yotei_mm",
	"point_ym",
	"point_yy",
	"point_mm",
	"ypoint"
) as 
select  
yotei_label as "yotei_label",
yotei_ym as "yotei_ym",
yotei_yy as "yotei_yy",
yotei_mm as "yotei_mm",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
ypoint as "ypoint"
from dev_dna_core.jpdcledw_integration.kr_054_plyotei_sum;
create or replace view KR_054_PUSE_SUM(
	"use_label",
	"use_ym",
	"use_yy",
	"use_mm",
	"point_ym",
	"point_yy",
	"point_mm",
	"upoint"
) as 
select  
use_label as "use_label",
use_ym as "use_ym",
use_yy as "use_yy",
use_mm as "use_mm",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
upoint as "upoint"
from dev_dna_core.jpdcledw_integration.kr_054_puse_sum;
create or replace view KR_054_RESULT(
	"no",
	"lg_item",
	"md_item",
	"sm_kb",
	"sm_nm",
	"pt_sum",
	"pt_pdt",
	"pt_rsn"
) as 
select  
no as "no",
lg_item as "lg_item",
md_item as "md_item",
sm_kb as "sm_kb",
sm_nm as "sm_nm",
pt_sum as "pt_sum",
pt_pdt as "pt_pdt",
pt_rsn as "pt_rsn"
from dev_dna_core.jpdcledw_integration.kr_054_result;
create or replace view KR_054_SFUYO_SUM(
	"fuyo_label",
	"point_ym",
	"point_yy",
	"point_mm",
	"point"
) as 
select  
fuyo_label as "fuyo_label",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
point as "point"
from dev_dna_core.jpdcledw_integration.kr_054_sfuyo_sum;
create or replace view KR_054_SLOST_SUM(
	"lost_label",
	"lost_ym",
	"lost_yy",
	"lost_mm",
	"lpoint"
) as 
select  
lost_label as "lost_label",
lost_ym as "lost_ym",
lost_yy as "lost_yy",
lost_mm as "lost_mm",
lpoint as "lpoint"
from dev_dna_core.jpdcledw_integration.kr_054_slost_sum;
create or replace view KR_054_SLYOTEI_SUM(
	"yotei_label",
	"yotei_ym",
	"yotei_yy",
	"yotei_mm",
	"point_ym",
	"point_yy",
	"point_mm",
	"ypoint"
) as 
select  
yotei_label as "yotei_label",
yotei_ym as "yotei_ym",
yotei_yy as "yotei_yy",
yotei_mm as "yotei_mm",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
ypoint as "ypoint"
from dev_dna_core.jpdcledw_integration.kr_054_slyotei_sum;
create or replace view KR_054_SSOUTEI_SUM(
	"fuyo_label",
	"point_ym",
	"point_yy",
	"point_mm",
	"point"
) as 
select  
fuyo_label as "fuyo_label",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
point as "point"
from dev_dna_core.jpdcledw_integration.kr_054_ssoutei_sum;
create or replace view KR_054_SUSE_SUM(
	"use_label",
	"use_ym",
	"use_yy",
	"use_mm",
	"point_ym",
	"point_yy",
	"point_mm",
	"upoint"
) as 
select  
use_label as "use_label",
use_ym as "use_ym",
use_yy as "use_yy",
use_mm as "use_mm",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
upoint as "upoint"
from dev_dna_core.jpdcledw_integration.kr_054_suse_sum;
create or replace view KR_FREQUENCY_1YN_900(
	"saleno",
	"kokyano",
	"now_rowno",
	"pre_rowno",
	"now_juchdate",
	"pre_juchdate",
	"elapsed",
	"insertdate",
	"shukadate_p"
) as 
select  
saleno as "saleno",
kokyano as "kokyano",
now_rowno as "now_rowno",
pre_rowno as "pre_rowno",
now_juchdate as "now_juchdate",
pre_juchdate as "pre_juchdate",
elapsed as "elapsed",
insertdate as "insertdate",
shukadate_p as "shukadate_p"
from dev_dna_core.jpdcledw_integration.kr_frequency_1yn_900;
create or replace view KR_NEW_STAGE_POINT(
	"yyyymm",
	"kokyano",
	"usrid",
	"thistotalprc",
	"stage",
	"thispoint",
	"prevpoint",
	"point",
	"insertdate"
) as
select yyyymm as "yyyymm",
    kokyano as "kokyano",
    usrid as "usrid",
    thistotalprc as "thistotalprc",
    stage as "stage",
    thispoint as "thispoint",
    prevpoint as "prevpoint",
    point as "point",
    insertdate as "insertdate"
from dev_dna_core.jpdcledw_integration.kr_new_stage_point;
create or replace view REPORT_006_A_NEWLY_ADDED(
	"channel_name",
	"channel_id",
	"yymm",
	"total",
	"day_of_week",
	"report_exec_date",
	"year_445",
	"month_445",
	"month_445_nm"
) as 
select  
channel_name as "channel_name",
channel_id as "channel_id",
yymm as "yymm",
total as "total",
day_of_week as "day_of_week",
report_exec_date as "report_exec_date",
year_445 as "year_445",
month_445 as "month_445",
month_445_nm as "month_445_nm"
from dev_dna_core.jpdcledw_integration.report_006_a_newly_added;
create or replace view REPORT_006_D(
	"契約日付",
	"nextid",
	"opecode",
	"opename",
	"定期契約顧客no.",
	"item_id",
	"item_name",
	"cnt_item",
	"total_revenue",
	"diitemsalescost",
	"contract_kubun",
	"order_id",
	"kessai_id",
	"userid",
	"logincode"
) as
select "契約日付",
    nextid as "nextid",
    opecode as "opecode",
    opename as "opename",
    "定期契約顧客no.",
    item_id as "item_id",
    item_name as "item_name",
    cnt_item as "cnt_item",
    total_revenue as "total_revenue",
    diitemsalescost as "diitemsalescost",
    contract_kubun as "contract_kubun",
    order_id as "order_id",
    kessai_id as "kessai_id",
    userid as "userid",
    logincode as "logincode"
from dev_dna_core.jpdcledw_integration.report_006_d;
create or replace view SYOUHINCD_HENKAN_QV(
	"itemcode",
	"koseiocode"
) as
select itemcode as "itemcode",
	koseiocode as "koseiocode"
from dev_dna_core.jpdcledw_integration.syouhincd_henkan_qv;
create or replace view TBECORDER(
	"diorderid",
	"diordercode",
	"diecusrid",
	"dssei",
	"dsmei",
	"dsseikana",
	"dsmeikana",
	"c_dimembtype",
	"ditodokeid",
	"dstodokesei",
	"dstodokemei",
	"dstodokeseikana",
	"dstodokemeikana",
	"dstodokezip",
	"c_dstodokeprefcd",
	"dstodokepref",
	"dstodokecity",
	"dstodokeaddr",
	"dstodoketatemono",
	"dstodokeaddrkana",
	"dstodoketel",
	"dstodokefax",
	"dipromid",
	"dirouteid",
	"dsorderdt",
	"c_dskeiyaktodokedate",
	"ditotalprc",
	"dihaisoprc",
	"ditaxrate",
	"c_dsreserveflg",
	"diusepoint",
	"c_diintroducepointflg",
	"c_diintroduceid",
	"dipoint",
	"dimonthlypoint",
	"diavailablepoint",
	"dihoryu",
	"c_dshoryuriyumemo",
	"dicancel",
	"dishukkasts",
	"dsuriagedt",
	"dsordermemo",
	"c_dikakutokuyoteipoint",
	"c_dsorderkbn",
	"c_dsdosokbn",
	"c_diallhenpinflg",
	"c_dshaisoshikibetsukbn",
	"diordertax",
	"c_didiscountprc",
	"c_didiscountall",
	"diseikyuprc",
	"dinyukinprc",
	"diseikyuremain",
	"dihenkinzumiprc",
	"dihaisokeitai",
	"c_dstorikomiid",
	"c_dsorderimportdate",
	"c_diuketsukeusrid",
	"c_dsuketsuketelcompanycd",
	"c_dsuketsukeusrname",
	"c_diinputusrid",
	"c_dsinputusrname",
	"c_dsinputtelcompanycd",
	"c_dilastupdusrid",
	"c_dslastupdusrname",
	"c_dslastupdtelcompanycd",
	"c_dipendingstsid",
	"c_dspendingcorrdate",
	"c_dsdeliveryfreeflg",
	"c_dscollectfreeflg",
	"c_dicollectprc",
	"c_ditoujitsuhaisoprc",
	"c_diclassid",
	"c_dsusrsts",
	"c_dspointitemincludeflg",
	"c_diranktargetprc",
	"c_diranktotalprc",
	"c_dipaymentprc",
	"c_diordernum",
	"c_diordersamplenum",
	"c_dsorderreferdate",
	"c_diregdiscticketprc",
	"c_dsregularautocreateflg",
	"c_diexchangepoint",
	"c_dspointtargetprc",
	"c_dsrewriteno",
	"c_dspotsalesno",
	"c_dstempocode",
	"c_dstemponame",
	"c_dstenposalesno",
	"c_dsorigpotsalesno",
	"c_dsupdateflg",
	"c_dsfreekbn",
	"c_dsdeliverydemandflg",
	"c_dstoujitsuhaisofreeflg",
	"dibillincluded",
	"dspackageflg",
	"c_dssamplelogicd",
	"dsprep",
	"dsren",
	"dselim",
	"diprepusr",
	"direnusr",
	"dielimusr",
	"dielimflg",
	"c_diyoyakuhenpinflg",
	"c_dsordercompmailsendkbn",
	"c_diadjustprc",
	"c_dihenkinprcinputdiff",
	"c_dihenkinprcinputdifftotal",
	"c_diadjustpoint",
	"source_file_date",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select diorderid as "diorderid",
    diordercode as "diordercode",
    diecusrid as "diecusrid",
    dssei as "dssei",
    dsmei as "dsmei",
    dsseikana as "dsseikana",
    dsmeikana as "dsmeikana",
    c_dimembtype as "c_dimembtype",
    ditodokeid as "ditodokeid",
    dstodokesei as "dstodokesei",
    dstodokemei as "dstodokemei",
    dstodokeseikana as "dstodokeseikana",
    dstodokemeikana as "dstodokemeikana",
    dstodokezip as "dstodokezip",
    c_dstodokeprefcd as "c_dstodokeprefcd",
    dstodokepref as "dstodokepref",
    dstodokecity as "dstodokecity",
    dstodokeaddr as "dstodokeaddr",
    dstodoketatemono as "dstodoketatemono",
    dstodokeaddrkana as "dstodokeaddrkana",
    dstodoketel as "dstodoketel",
    dstodokefax as "dstodokefax",
    dipromid as "dipromid",
    dirouteid as "dirouteid",
    dsorderdt as "dsorderdt",
    c_dskeiyaktodokedate as "c_dskeiyaktodokedate",
    ditotalprc as "ditotalprc",
    dihaisoprc as "dihaisoprc",
    ditaxrate as "ditaxrate",
    c_dsreserveflg as "c_dsreserveflg",
    diusepoint as "diusepoint",
    c_diintroducepointflg as "c_diintroducepointflg",
    c_diintroduceid as "c_diintroduceid",
    dipoint as "dipoint",
    dimonthlypoint as "dimonthlypoint",
    diavailablepoint as "diavailablepoint",
    dihoryu as "dihoryu",
    c_dshoryuriyumemo as "c_dshoryuriyumemo",
    dicancel as "dicancel",
    dishukkasts as "dishukkasts",
    dsuriagedt as "dsuriagedt",
    dsordermemo as "dsordermemo",
    c_dikakutokuyoteipoint as "c_dikakutokuyoteipoint",
    c_dsorderkbn as "c_dsorderkbn",
    c_dsdosokbn as "c_dsdosokbn",
    c_diallhenpinflg as "c_diallhenpinflg",
    c_dshaisoshikibetsukbn as "c_dshaisoshikibetsukbn",
    diordertax as "diordertax",
    c_didiscountprc as "c_didiscountprc",
    c_didiscountall as "c_didiscountall",
    diseikyuprc as "diseikyuprc",
    dinyukinprc as "dinyukinprc",
    diseikyuremain as "diseikyuremain",
    dihenkinzumiprc as "dihenkinzumiprc",
    dihaisokeitai as "dihaisokeitai",
    c_dstorikomiid as "c_dstorikomiid",
    c_dsorderimportdate as "c_dsorderimportdate",
    c_diuketsukeusrid as "c_diuketsukeusrid",
    c_dsuketsuketelcompanycd as "c_dsuketsuketelcompanycd",
    c_dsuketsukeusrname as "c_dsuketsukeusrname",
    c_diinputusrid as "c_diinputusrid",
    c_dsinputusrname as "c_dsinputusrname",
    c_dsinputtelcompanycd as "c_dsinputtelcompanycd",
    c_dilastupdusrid as "c_dilastupdusrid",
    c_dslastupdusrname as "c_dslastupdusrname",
    c_dslastupdtelcompanycd as "c_dslastupdtelcompanycd",
    c_dipendingstsid as "c_dipendingstsid",
    c_dspendingcorrdate as "c_dspendingcorrdate",
    c_dsdeliveryfreeflg as "c_dsdeliveryfreeflg",
    c_dscollectfreeflg as "c_dscollectfreeflg",
    c_dicollectprc as "c_dicollectprc",
    c_ditoujitsuhaisoprc as "c_ditoujitsuhaisoprc",
    c_diclassid as "c_diclassid",
    c_dsusrsts as "c_dsusrsts",
    c_dspointitemincludeflg as "c_dspointitemincludeflg",
    c_diranktargetprc as "c_diranktargetprc",
    c_diranktotalprc as "c_diranktotalprc",
    c_dipaymentprc as "c_dipaymentprc",
    c_diordernum as "c_diordernum",
    c_diordersamplenum as "c_diordersamplenum",
    c_dsorderreferdate as "c_dsorderreferdate",
    c_diregdiscticketprc as "c_diregdiscticketprc",
    c_dsregularautocreateflg as "c_dsregularautocreateflg",
    c_diexchangepoint as "c_diexchangepoint",
    c_dspointtargetprc as "c_dspointtargetprc",
    c_dsrewriteno as "c_dsrewriteno",
    c_dspotsalesno as "c_dspotsalesno",
    c_dstempocode as "c_dstempocode",
    c_dstemponame as "c_dstemponame",
    c_dstenposalesno as "c_dstenposalesno",
    c_dsorigpotsalesno as "c_dsorigpotsalesno",
    c_dsupdateflg as "c_dsupdateflg",
    c_dsfreekbn as "c_dsfreekbn",
    c_dsdeliverydemandflg as "c_dsdeliverydemandflg",
    c_dstoujitsuhaisofreeflg as "c_dstoujitsuhaisofreeflg",
    dibillincluded as "dibillincluded",
    dspackageflg as "dspackageflg",
    c_dssamplelogicd as "c_dssamplelogicd",
    dsprep as "dsprep",
    dsren as "dsren",
    dselim as "dselim",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimusr as "dielimusr",
    dielimflg as "dielimflg",
    c_diyoyakuhenpinflg as "c_diyoyakuhenpinflg",
    c_dsordercompmailsendkbn as "c_dsordercompmailsendkbn",
    c_diadjustprc as "c_diadjustprc",
    c_dihenkinprcinputdiff as "c_dihenkinprcinputdiff",
    c_dihenkinprcinputdifftotal as "c_dihenkinprcinputdifftotal",
    c_diadjustpoint as "c_diadjustpoint",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdclitg_integration.tbecorder;
create or replace view TBECORDERMEISAI(
	"dimeisaiid",
	"diorderid",
	"disetid",
	"diid",
	"c_diitemtype",
	"dsitemid",
	"dsitemname",
	"c_dsitemnameryaku",
	"diusualprc",
	"ditotalprc",
	"diitemtax",
	"diitemnum",
	"c_diitemtotalprc",
	"c_didiscountmeisai",
	"dishukkasts",
	"c_dipresentgrantcord",
	"c_dikesaiid",
	"c_dsregularmeisaiid",
	"c_dsordertype",
	"c_disubscriptionkubun",
	"c_diregularcourseid",
	"dipromid",
	"c_disetitemprc",
	"c_dssetitemkbn",
	"c_dssetitemflg",
	"c_dspickingflg",
	"c_dspointflg",
	"c_dspointitemflg",
	"c_dsnoshinshooutputflg",
	"dicancel",
	"c_dshenpinflg",
	"dsshukaflg",
	"dihenpinyoteinum",
	"c_dinoshinshoitemprc",
	"c_dssampleclasscd",
	"c_dssamplelogicd",
	"disokoid",
	"dirouteid",
	"c_distockchanelid",
	"c_dihikiatenum",
	"c_diadjustprc",
	"c_dsdemandkbn",
	"c_dsdiscpritekiyoflg",
	"c_didiscountrate",
	"c_dsmessage",
	"c_diorderlineno",
	"c_dikesailineno",
	"c_dspotsalesno",
	"c_dipotlineno",
	"disetmeisaiid",
	"c_difrontsortorder",
	"c_dinondispflg",
	"c_dsfreekbn",
	"c_diusepoint",
	"dsprep",
	"dsren",
	"dselim",
	"diprepusr",
	"direnusr",
	"dielimusr",
	"dielimflg",
	"c_diwarnitemid",
	"c_diyoyakuhenpinflg",
	"c_dihenpinzuminum",
	"c_diregulardiscountrate",
	"c_dstaxkbn",
	"ditaxrate",
	"source_file_date",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select dimeisaiid as "dimeisaiid",
    diorderid as "diorderid",
    disetid as "disetid",
    diid as "diid",
    c_diitemtype as "c_diitemtype",
    dsitemid as "dsitemid",
    dsitemname as "dsitemname",
    c_dsitemnameryaku as "c_dsitemnameryaku",
    diusualprc as "diusualprc",
    ditotalprc as "ditotalprc",
    diitemtax as "diitemtax",
    diitemnum as "diitemnum",
    c_diitemtotalprc as "c_diitemtotalprc",
    c_didiscountmeisai as "c_didiscountmeisai",
    dishukkasts as "dishukkasts",
    c_dipresentgrantcord as "c_dipresentgrantcord",
    c_dikesaiid as "c_dikesaiid",
    c_dsregularmeisaiid as "c_dsregularmeisaiid",
    c_dsordertype as "c_dsordertype",
    c_disubscriptionkubun as "c_disubscriptionkubun",
    c_diregularcourseid as "c_diregularcourseid",
    dipromid as "dipromid",
    c_disetitemprc as "c_disetitemprc",
    c_dssetitemkbn as "c_dssetitemkbn",
    c_dssetitemflg as "c_dssetitemflg",
    c_dspickingflg as "c_dspickingflg",
    c_dspointflg as "c_dspointflg",
    c_dspointitemflg as "c_dspointitemflg",
    c_dsnoshinshooutputflg as "c_dsnoshinshooutputflg",
    dicancel as "dicancel",
    c_dshenpinflg as "c_dshenpinflg",
    dsshukaflg as "dsshukaflg",
    dihenpinyoteinum as "dihenpinyoteinum",
    c_dinoshinshoitemprc as "c_dinoshinshoitemprc",
    c_dssampleclasscd as "c_dssampleclasscd",
    c_dssamplelogicd as "c_dssamplelogicd",
    disokoid as "disokoid",
    dirouteid as "dirouteid",
    c_distockchanelid as "c_distockchanelid",
    c_dihikiatenum as "c_dihikiatenum",
    c_diadjustprc as "c_diadjustprc",
    c_dsdemandkbn as "c_dsdemandkbn",
    c_dsdiscpritekiyoflg as "c_dsdiscpritekiyoflg",
    c_didiscountrate as "c_didiscountrate",
    c_dsmessage as "c_dsmessage",
    c_diorderlineno as "c_diorderlineno",
    c_dikesailineno as "c_dikesailineno",
    c_dspotsalesno as "c_dspotsalesno",
    c_dipotlineno as "c_dipotlineno",
    disetmeisaiid as "disetmeisaiid",
    c_difrontsortorder as "c_difrontsortorder",
    c_dinondispflg as "c_dinondispflg",
    c_dsfreekbn as "c_dsfreekbn",
    c_diusepoint as "c_diusepoint",
    dsprep as "dsprep",
    dsren as "dsren",
    dselim as "dselim",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimusr as "dielimusr",
    dielimflg as "dielimflg",
    c_diwarnitemid as "c_diwarnitemid",
    c_diyoyakuhenpinflg as "c_diyoyakuhenpinflg",
    c_dihenpinzuminum as "c_dihenpinzuminum",
    c_diregulardiscountrate as "c_diregulardiscountrate",
    c_dstaxkbn as "c_dstaxkbn",
    ditaxrate as "ditaxrate",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdclitg_integration.tbecordermeisai;
create or replace view TBPROMOTION(
	"dipromid",
	"dipromcateid",
	"dspromname",
	"dspromcode",
	"dipromregistflg",
	"dipromenqflg",
	"dipromorderflg",
	"dipromdivsts",
	"dsvalidfrom",
	"dsvalidto",
	"dspcurl",
	"dsmburl",
	"diinvalidsts",
	"dspcinvalidurl",
	"dsmbinvalidurl",
	"c_diaffiliatekubun",
	"c_dsorderendtag",
	"c_dskoukokuhi",
	"c_dsdistributenum",
	"c_dipublishkubun",
	"c_dipromcompanyid",
	"c_didisppriority",
	"c_dicampaignid",
	"c_diredirectflg",
	"dsunredirectsts",
	"dsunredirecturlpc",
	"dsunredirecturlmobile",
	"dsprep",
	"dsren",
	"diprepusr",
	"direnusr",
	"dielimflg",
	"c_csid",
	"c_dipcviewflg",
	"c_dideptid",
	"source_file_date",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as
select dipromid as "dipromid",
    dipromcateid as "dipromcateid",
    dspromname as "dspromname",
    dspromcode as "dspromcode",
    dipromregistflg as "dipromregistflg",
    dipromenqflg as "dipromenqflg",
    dipromorderflg as "dipromorderflg",
    dipromdivsts as "dipromdivsts",
    dsvalidfrom as "dsvalidfrom",
    dsvalidto as "dsvalidto",
    dspcurl as "dspcurl",
    dsmburl as "dsmburl",
    diinvalidsts as "diinvalidsts",
    dspcinvalidurl as "dspcinvalidurl",
    dsmbinvalidurl as "dsmbinvalidurl",
    c_diaffiliatekubun as "c_diaffiliatekubun",
    c_dsorderendtag as "c_dsorderendtag",
    c_dskoukokuhi as "c_dskoukokuhi",
    c_dsdistributenum as "c_dsdistributenum",
    c_dipublishkubun as "c_dipublishkubun",
    c_dipromcompanyid as "c_dipromcompanyid",
    c_didisppriority as "c_didisppriority",
    c_dicampaignid as "c_dicampaignid",
    c_diredirectflg as "c_diredirectflg",
    dsunredirectsts as "dsunredirectsts",
    dsunredirecturlpc as "dsunredirecturlpc",
    dsunredirecturlmobile as "dsunredirecturlmobile",
    dsprep as "dsprep",
    dsren as "dsren",
    diprepusr as "diprepusr",
    direnusr as "direnusr",
    dielimflg as "dielimflg",
    c_csid as "c_csid",
    c_dipcviewflg as "c_dipcviewflg",
    c_dideptid as "c_dideptid",
    source_file_date as "source_file_date",
    inserted_date as "inserted_date",
    inserted_by as "inserted_by",
    updated_date as "updated_date",
    updated_by as "updated_by"
from dev_dna_core.jpdclitg_integration.tbpromotion;
create or replace view TEIKIKEIYAKU_DATA_MART_UNI(
	"c_diregularcontractid",
	"c_diusrid",
	"dirouteid",
	"keiyakubi",
	"shokai_ym",
	"kaiyakubi",
	"c_dsregularmeisaiid",
	"header_flg",
	"c_dsdeleveryym",
	"dsitemid",
	"c_diregularcourseid",
	"diitemsalesprc",
	"c_dsordercreatekbn",
	"c_dscontractchangekbn",
	"c_dicancelflg",
	"kaiyaku_kbn",
	"contract_kbn",
	"diordercode",
	"c_dikesaiid",
	"dimeisaiid",
	"maker",
	"kaiyakumoushidebi"
) as
select c_diregularcontractid as "c_diregularcontractid",
    c_diusrid as "c_diusrid",
    dirouteid as "dirouteid",
    keiyakubi as "keiyakubi",
    shokai_ym as "shokai_ym",
    kaiyakubi as "kaiyakubi",
    c_dsregularmeisaiid as "c_dsregularmeisaiid",
    header_flg as "header_flg",
    c_dsdeleveryym as "c_dsdeleveryym",
    dsitemid as "dsitemid",
    c_diregularcourseid as "c_diregularcourseid",
    diitemsalesprc as "diitemsalesprc",
    c_dsordercreatekbn as "c_dsordercreatekbn",
    c_dscontractchangekbn as "c_dscontractchangekbn",
    c_dicancelflg as "c_dicancelflg",
    kaiyaku_kbn as "kaiyaku_kbn",
    contract_kbn as "contract_kbn",
    diordercode as "diordercode",
    c_dikesaiid as "c_dikesaiid",
    dimeisaiid as "dimeisaiid",
    maker as "maker",
    kaiyakumoushidebi as "kaiyakumoushidebi"
from dev_dna_core.jpdcledw_integration.teikikeiyaku_data_mart_uni;
create or replace view TM13ITEM_QV(
	"itemcode",
	"itemname",
	"itemnamer",
	"itemkbn",
	"bunruicode2",
	"bunruicode3",
	"bunruicode5",
	"settanpinkbncode",
	"settanpinsetkbn",
	"teikikeiyaku"
) as
select
itemcode as "itemcode",
itemname as "itemname",
itemnamer as "itemnamer",
itemkbn as "itemkbn",
bunruicode2 as "bunruicode2",
bunruicode3 as "bunruicode3",
bunruicode5 as "bunruicode5",
settanpinkbncode as "settanpinkbncode",
settanpinsetkbn as "settanpinsetkbn",
teikikeiyaku as "teikikeiyaku"
from dev_dna_core.jpdcledw_integration.tm13item_qv;
create or replace view TM14SHKOS_QV(
	"itemcode",
	"kosecode",
	"suryo",
	"kosetanka",
	"koseritsu",
	"koseanbuntanka",
	"motoinsertdate",
	"motoupdatedate",
	"insertdate",
	"inserttime",
	"insertid",
	"updatedate",
	"updatetime",
	"updateid"
) as
select itemcode as "itemcode",
    kosecode as "kosecode",
    suryo as "suryo",
    kosetanka as "kosetanka",
    koseritsu as "koseritsu",
    koseanbuntanka as "koseanbuntanka",
    motoinsertdate as "motoinsertdate",
    motoupdatedate as "motoupdatedate",
    insertdate as "insertdate",
    inserttime as "inserttime",
    insertid as "insertid",
    updatedate as "updatedate",
    updatetime as "updatetime",
    updateid as "updateid"
from dev_dna_core.jpdcledw_integration.tm14shkos_qv;
create or replace view TM22KOKYASTS(
	"kokyano",
	"firstjuchdate",
	"firstkonyudate",
	"zaisekidays",
	"zaisekimonth",
	"firsttsuhandate",
	"firsttenpodate",
	"ruikaisu",
	"ruikingaku",
	"ruiindays",
	"lastjuchdate",
	"juchukeikadays",
	"lastkonyudate",
	"konyukeikadays",
	"nenkaisu",
	"nenkingaku",
	"nenindays",
	"nengelryo",
	"tsukigelryo",
	"juchurkbncode",
	"konyurkbncode",
	"ruifkbncode",
	"nenfkbncode",
	"ruiikbncode",
	"nenikbncode",
	"ruimkbncode",
	"nenmkbncode1",
	"nenmkbncode2",
	"nenmkbncode3",
	"nenmkbncode4",
	"nenmkbncode5",
	"tsukigkbncode",
	"segkbncode",
	"maindaibuncode",
	"mainchubuncode",
	"mainsyobuncode",
	"mainsaibuncode",
	"maintenpocode",
	"insertdate",
	"inserttime",
	"insertid",
	"updatedate",
	"updatetime",
	"updateid",
	"bk_kokyano",
	"bk_maindaibuncode",
	"bk_mainchubuncode",
	"bk_mainsyobuncode",
	"bk_mainsaibuncode",
	"bk_firstjuchdate",
	"bk_firstkonyudate",
	"bk_firsttsuhandate",
	"bk_firsttenpodate",
	"bk_zaisekidays",
	"bk_zaisekimonth",
	"bk_ruikaisu",
	"bk_ruikingaku",
	"bk_ruiindays",
	"bk_nenkaisu",
	"bk_nenkingaku",
	"bk_nenindays",
	"bk_nengelryo",
	"inserted_date",
	"inserted_by",
	"updated_date",
	"updated_by"
) as 
select  
kokyano as "kokyano",
firstjuchdate as "firstjuchdate",
firstkonyudate as "firstkonyudate",
zaisekidays as "zaisekidays",
zaisekimonth as "zaisekimonth",
firsttsuhandate as "firsttsuhandate",
firsttenpodate as "firsttenpodate",
ruikaisu as "ruikaisu",
ruikingaku as "ruikingaku",
ruiindays as "ruiindays",
lastjuchdate as "lastjuchdate",
juchukeikadays as "juchukeikadays",
lastkonyudate as "lastkonyudate",
konyukeikadays as "konyukeikadays",
nenkaisu as "nenkaisu",
nenkingaku as "nenkingaku",
nenindays as "nenindays",
nengelryo as "nengelryo",
tsukigelryo as "tsukigelryo",
juchurkbncode as "juchurkbncode",
konyurkbncode as "konyurkbncode",
ruifkbncode as "ruifkbncode",
nenfkbncode as "nenfkbncode",
ruiikbncode as "ruiikbncode",
nenikbncode as "nenikbncode",
ruimkbncode as "ruimkbncode",
nenmkbncode1 as "nenmkbncode1",
nenmkbncode2 as "nenmkbncode2",
nenmkbncode3 as "nenmkbncode3",
nenmkbncode4 as "nenmkbncode4",
nenmkbncode5 as "nenmkbncode5",
tsukigkbncode as "tsukigkbncode",
segkbncode as "segkbncode",
maindaibuncode as "maindaibuncode",
mainchubuncode as "mainchubuncode",
mainsyobuncode as "mainsyobuncode",
mainsaibuncode as "mainsaibuncode",
maintenpocode as "maintenpocode",
insertdate as "insertdate",
inserttime as "inserttime",
insertid as "insertid",
updatedate as "updatedate",
updatetime as "updatetime",
updateid as "updateid",
bk_kokyano as "bk_kokyano",
bk_maindaibuncode as "bk_maindaibuncode",
bk_mainchubuncode as "bk_mainchubuncode",
bk_mainsyobuncode as "bk_mainsyobuncode",
bk_mainsaibuncode as "bk_mainsaibuncode",
bk_firstjuchdate as "bk_firstjuchdate",
bk_firstkonyudate as "bk_firstkonyudate",
bk_firsttsuhandate as "bk_firsttsuhandate",
bk_firsttenpodate as "bk_firsttenpodate",
bk_zaisekidays as "bk_zaisekidays",
bk_zaisekimonth as "bk_zaisekimonth",
bk_ruikaisu as "bk_ruikaisu",
bk_ruikingaku as "bk_ruikingaku",
bk_ruiindays as "bk_ruiindays",
bk_nenkaisu as "bk_nenkaisu",
bk_nenkingaku as "bk_nenkingaku",
bk_nenindays as "bk_nenindays",
bk_nengelryo as "bk_nengelryo",
inserted_date as "inserted_date",
inserted_by as "inserted_by",
updated_date as "updated_date",
updated_by as "updated_by"
from dev_dna_core.jpdcledw_integration.tm22kokyasts;
create or replace view VW_3MONTHACTIVEUSERV2(
	"yyyymm_s",
	"yyyymm_e",
	"yyyymm",
	"customer_id",
	"age",
	"channel",
	"rank",
	"flag_user",
	"flag",
	"f",
	"salesamount",
	"purchasecount",
	"avedayssincelastorder"
) as 
(with prekesai as -- 1
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        datediff(
            month,
            to_date(
                (
                    lag(to_char(h."juchdate")) OVER (
                        PARTITION BY h."kokyano"
                        ORDER BY to_char(h."juchdate")
                    )
                ),
                'YYYYMMDD'
            ),
            "order_dt"
        ) month_since_last_order,
        -- 前回受注日との間隔（月）
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(day, "order_dt", current_timestamp()) <= 92
        AND datediff(day, "order_dt", current_timestamp()) >= 2
),
new_user as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai as t1
    WHERE datediff(day, "order_dt", current_timestamp()) <= 32
        and datediff(day, "order_dt", current_timestamp()) >= 2
    GROUP BY t1."customer_id"
),
kesai as (
    SELECT *,
        CASE
            WHEN current_month_user = 1 then CASE
                WHEN 'nu.new_user' = 1 THEN 'new'
                WHEN 'lu.lapsed_user' = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai as "t1"
        left outer join new_user as "nu" on 't1."customer_id"' = 'nu."id_new"'
        left outer join lapsed_user as "lu" on 't1."customer_id"' = 'lu."id_lapsed"'
        left outer join current_month_user as "cmu" on 't1."customer_id"' = 'cmu."id_cmu"'
),
prekesai2 as --2
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 3
        AND datediff(month, "order_dt", current_timestamp()) >= 1
),
new_user2 as(
    SELECT t1."customer_id" id_new,
        1 as new_user
    FROM prekesai2 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user2 as (
    SELECT t1."customer_id" id_lapsed,
        1 as lapsed_user
    FROM prekesai2 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user2 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai2 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 1
    GROUP BY t1."customer_id"
),
kesai2 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai2 as t1
        left outer join new_user2 as nu on t1."customer_id" = 'nu."id_new"'
        left outer join lapsed_user2 as lu on t1."customer_id" = 'lu."id_lapsed"'
        left outer join current_month_user2 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai3 as --3
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 4
        AND datediff(month, "order_dt", current_timestamp()) >= 2
),
new_user3 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai3 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user3 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai3 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user3 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai3 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 2
    GROUP BY t1."customer_id"
),
kesai3 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai3 as t1
        left outer join new_user3 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user3 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user3 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai4 as --4
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 5
        AND datediff(month, "order_dt", current_timestamp()) >= 3
),
new_user4 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai4 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user4 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai4 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user4 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai4 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 3
    GROUP BY t1."customer_id"
),
kesai4 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai4 as t1
        left outer join new_user4 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user4 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user4 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai5 as --5
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 6
        AND datediff(month, "order_dt", current_timestamp()) >= 4
),
new_user5 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai5 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user5 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai5 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user5 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai5 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 4
    GROUP BY t1."customer_id"
),
kesai5 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai5 as t1
        left outer join new_user5 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user5 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user5 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai6 as --6
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 7
        AND datediff(month, "order_dt", current_timestamp()) >= 5
),
new_user6 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai6 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user6 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai6 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user6 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai6 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 5
    GROUP BY t1."customer_id"
),
kesai6 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai6 as t1
        left outer join new_user6 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user6 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user6 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai7 as --7
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 8
        AND datediff(month, "order_dt", current_timestamp()) >= 6
),
new_user7 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai7 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user7 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai7 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user7 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai7 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 6
    GROUP BY t1."customer_id"
),
kesai7 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai7 as t1
        left outer join new_user7 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user7 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user7 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai8 as --8
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR (
                h."shukkasts" = '1060'
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 9
        AND datediff(month, "order_dt", current_timestamp()) >= 7
),
new_user8 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai8 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user8 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai8 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user8 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai8 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 7
    GROUP BY t1."customer_id"
),
kesai8 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai8 as t1
        left outer join new_user8 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user8 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user8 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai12 as --2
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 10
        AND datediff(month, "order_dt", current_timestamp()) >= 8
),
new_user12 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai12 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user12 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai12 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user12 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai12 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 8
    GROUP BY t1."customer_id"
),
kesai12 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai12 as t1
        left outer join new_user12 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user12 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user12 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai13 as --3
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 11
        AND datediff(month, "order_dt", current_timestamp()) >= 9
),
new_user13 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai13 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user13 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai13 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user13 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai13 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 9
    GROUP BY t1."customer_id"
),
kesai13 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai13 as t1
        left outer join new_user13 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user13 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user13 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai14 as --4
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 12
        AND datediff(month, "order_dt", current_timestamp()) >= 10
),
new_user14 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai14 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user14 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai14 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user14 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai14 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 10
    GROUP BY t1."customer_id"
),
kesai14 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai14 as t1
        left outer join new_user14 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user14 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user14 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai15 as --5
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = '1'
            OR h."shukkasts" = '1060'
            AND (h."shukadate" IS NOT NULL)
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 13
        AND datediff(month, "order_dt", current_timestamp()) >= 11
),
new_user15 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai15 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user15 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai15 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user15 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai15 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 11
    GROUP BY t1."customer_id"
),
kesai15 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai15 as t1
        left outer join new_user15 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user15 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user15 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai16 as --6
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 14
        AND datediff(month, "order_dt", current_timestamp()) >= 12
),
new_user16 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai16 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user16 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai16 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user16 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai16 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 12
    GROUP BY t1."customer_id"
),
kesai16 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai16 as t1
        left outer join new_user16 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user16 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user16 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai17 as --7
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 15
        AND datediff(month, "order_dt", current_timestamp()) >= 13
),
new_user17 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai17 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user17 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai17 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user17 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai17 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 13
    GROUP BY t1."customer_id"
),
kesai17 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai17 as t1
        left outer join new_user17 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user17 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user17 as cmu on t1."customer_id" = cmu."id_cmu"
),
prekesai18 as --8
(
    SELECT c."kokyano" "customer_id",
        --顧客番号
        h."saleno",
        -- 受注番号
        try_to_date(to_char(h."juchdate"), 'yyyymmdd') "order_dt",
        -- 受注日
        --    h.juchdate order_dt,
        --    lag(h.juchdate) OVER (PARTITION BY h.kokyano ORDER BY h.juchdate) as pre_order_dt,
        k900."elapsed" "days_since_last_order",
        -- 前回受注日との間隔（日）
        h."daihanrobunname" "channel",
        -- "チャネル",
        DENSE_RANK() OVER (
            PARTITION BY c."kokyano"
            ORDER BY h."juchdate",
                h."saleno"
        ) F,
        -- 期間ない購買番号
        CASE
            WHEN h."juchdate" = kokyast."firstkonyudate" THEN 'new' -- 新規受注
            WHEN k900."elapsed" >= 365 THEN 'lapsed' -- 休眠復帰受注
            ELSE 'existing' -- 既存受注
        END as "flag",
        h."sogokei" - h."tax" as "nukikingaku",
        -- 税抜き金額
        c."birthday",
        -- 誕生日
        FLOOR(
            (
                CAST(
                    REPLACE(to_date(current_timestamp()), '-', '') AS INTEGER
                ) - c."birthday"
            ) / 100000
        ) * 10 as "age",
        -- 現在年代
        c."rank" --顧客現在ランク
    FROM JPDCLEDW_ACCESS.KESAI_H_DATA_MART_MV h --- ヘッダーテーブル
        INNER JOIN JPDCLEDW_ACCESS.CIM01KOKYA c --- 顧客テーブル
        ON h."kokyano" = c."kokyano"
        INNER JOIN JPDCLEDW_ACCESS.TM22KOKYASTS kokyast on h."kokyano" = kokyast."kokyano"
        LEFT OUTER JOIN JPDCLEDW_ACCESS.KR_FREQUENCY_1YN_900 k900 ON h."saleno" = k900."saleno"
    WHERE "nukikingaku" > 0 -- 1円以上
        -- 出荷ステータス「1060：出荷済」 AND 出荷日_P に日付がある
        -- または、過去区分が「1：過去」
        AND (
            h."kakokbn" = 1
            OR (
                h."shukkasts" = 1060
                AND h."shukadate" IS NOT NULL
            )
        )
        AND "channel" in ('Web', '通販')
        AND datediff(month, "order_dt", current_timestamp()) <= 16
        AND datediff(month, "order_dt", current_timestamp()) >= 14
),
new_user18 as(
    SELECT t1."customer_id" "id_new",
        1 as new_user
    FROM prekesai18 as t1
    WHERE t1."flag" = 'new'
    GROUP BY t1."customer_id"
),
lapsed_user18 as (
    SELECT t1."customer_id" "id_lapsed",
        1 as lapsed_user
    FROM prekesai18 as t1
    WHERE t1."flag" = 'lapsed'
    GROUP BY t1."customer_id"
),
current_month_user18 as (
    SELECT t1."customer_id" "id_cmu",
        1 as current_month_user
    FROM prekesai18 as t1
    WHERE datediff(month, "order_dt", current_timestamp()) = 14
    GROUP BY t1."customer_id"
),
kesai18 as (
    SELECT *,
        CASE
            WHEN cmu.current_month_user = 1 then CASE
                WHEN nu.new_user = 1 THEN 'new'
                WHEN lu.lapsed_user = 1 THEN 'lapsed'
                ELSE 'existing'
            END
            ELSE 'existing_wo_buy'
        END as "flag_user"
    FROM prekesai18 as t1
        left outer join new_user18 as nu on t1."customer_id" = nu."id_new"
        left outer join lapsed_user18 as lu on t1."customer_id" = lu."id_lapsed"
        left outer join current_month_user18 as cmu on t1."customer_id" = cmu."id_cmu"
),
final as (
    SELECT to_char(
            dateadd(month, -2, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(dateadd(month, 0, current_timestamp()), 'YYYYMM') as "yyyymm_e",
        'as of' || to_char(dateadd(day, 0, GETDATE()), 'YYYYMMDD') as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai bs
    WHERE datediff(day, "order_dt", current_timestamp()) <= 92
        and datediff(day, "order_dt", current_timestamp()) >= 2
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -3, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -1, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -1, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai2 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -4, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -2, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -2, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai3 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -5, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -3, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -3, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai4 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -6, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -4, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -4, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai5 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -7, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -5, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -5, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai6 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -8, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -6, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -6, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai7 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -9, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -7, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -7, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai8 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -10, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -8, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -8, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai12 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -11, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -9, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -9, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai13 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -12, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -10, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -10, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai14 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -13, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -11, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -11, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai15 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -14, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -12, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -12, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai16 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -15, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -13, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -13, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai17 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
    UNION ALL
    SELECT to_char(
            dateadd(month, -16, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_s",
        to_char(
            dateadd(month, -14, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm_e",
        to_char(
            dateadd(month, -14, current_timestamp()),
            'YYYYMM'
        ) as "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag",
        MAX("F") AS "f",
        SUM("nukikingaku") AS "salesamount",
        COUNT(DISTINCT "saleno") AS "purchasecount",
        min("days_since_last_order") as "avedayssincelastorder"
    FROM kesai18 bs
    GROUP BY "yyyymm_s",
        "yyyymm_e",
        "yyyymm",
        "customer_id",
        "age",
        "channel",
        "rank",
        "flag_user",
        "flag"
)
select *
from final);
create or replace view WK_D23483_NOHINDATA_0825(
	"受注id",
	"決済id",
	"顧客id",
	"顧客no",
	"受注基準日",
	"基準金額",
	"tsu-han",
	"web",
	"call",
	"store"
) as 
select  
"受注id",
	"決済id",
	"顧客id",
	"顧客no",
	"受注基準日",
	"基準金額",
	"tsu-han",
	web as "web",
	call as "call",
	store as "store"
from dev_dna_core.jpdcledw_integration.wk_d23483_nohindata_0825;
create or replace view WK_D23484_NOHINDATA_FINAL(
	"usrid",
	"point",
	"rankdt",
	"sum"
) as 
select  
usrid as "usrid",
point as "point",
rankdt as "rankdt",
sum as "sum"
from dev_dna_core.jpdcledw_integration.wk_d23484_nohindata_final;
create or replace view "cewq028破損商品"(
	"返品no",
	"返品明細no",
	"返品日",
	"商品",
	"商品名",
	"数量",
	"単価",
	"金額",
	"運送会社コード",
	"キャンセルフラグ",
	"削除フラグ",
	"返品区分",
	"抽出日付",
	"受注no",
	"送り状no"
) as
select
    "返品no",
    "返品明細no",
    "返品日",
    "商品",
    "商品名",
    "数量",
    "単価",
    "金額",
    "運送会社コード",
    "キャンセルフラグ",
    "削除フラグ",
    "返品区分",
    "抽出日付",
    "受注no",
    "送り状no"
from dev_dna_core.jpdcledw_integration."cewq028破損商品";
create or replace view "wqwt34明細出力リスト"(
	"売上no",
	"得意先コード",
	"得意先名",
	"得意先名略",
	"得意先名カナ",
	"受注区分",
	"アイテム区分",
	"アイテム区分名",
	"専用伝票番号",
	"商品コード",
	"janコード",
	"取引先品目コード",
	"商品名",
	"バラ数量",
	"単価",
	"金額",
	"小売単価",
	"対象月次締日",
	"都道府県別集計用",
	"得意先名2",
	"地区名称",
	"千趣会除2",
	"統合商品名",
	"品目分類値コード1",
	"集計用名称",
	"販促単価",
	"販促費抽出",
	"集計用得意先名",
	"廃盤品目コード",
	"納品日",
	"出荷日",
	"プロセスタイプコード",
	"受注番号",
	"取引区分",
	"得意先属性付与no",
	"請求先名",
	"請求先コード",
	"売上区分",
	"伝票税抜合計金額",
	"消費税等金額",
	"伝票総合計金額",
	"得意先向備考メモ",
	"代表出荷先コード",
	"代表出荷先名",
	"消費税率"
) as 
select  
"売上no",
	"得意先コード",
	"得意先名",
	"得意先名略",
	"得意先名カナ",
	"受注区分",
	"アイテム区分",
	"アイテム区分名",
	"専用伝票番号",
	"商品コード",
	"janコード",
	"取引先品目コード",
	"商品名",
	"バラ数量",
	"単価",
	"金額",
	"小売単価",
	"対象月次締日",
	"都道府県別集計用",
	"得意先名2",
	"地区名称",
	"千趣会除2",
	"統合商品名",
	"品目分類値コード1",
	"集計用名称",
	"販促単価",
	"販促費抽出",
	"集計用得意先名",
	"廃盤品目コード",
	"納品日",
	"出荷日",
	"プロセスタイプコード",
	"受注番号",
	"取引区分",
	"得意先属性付与no",
	"請求先名",
	"請求先コード",
	"売上区分",
	"伝票税抜合計金額",
	"消費税等金額",
	"伝票総合計金額",
	"得意先向備考メモ",
	"代表出荷先コード",
	"代表出荷先名",
	"消費税率"
from dev_dna_core.jpdcledw_integration."wqwt34明細出力リスト";
create or replace view "wqwt74戦略商品別売上ｐ"(
	"出荷年月日",
	"チャネル",
	"通信経路大区分",
	"取引形態区分",
	"得意先",
	"販売商品",
	"商品構成",
	"在庫商品コード",
	"受注区分コード",
	"c受注区分名",
	"数量",
	"返品数量",
	"純売上数量",
	"按分前金額_調整前",
	"明細金額_調整前",
	"按分前金額_調整後",
	"明細金額_調整後",
	"伝票値引按分後売上金額",
	"税抜按分利用ポイント",
	"税込按分利用ポイント",
	"チャネル大区分",
	"チャネル中区分",
	"チャネル小区分",
	"売上計上部門コード",
	"売上計上部門名",
	"返品理由コード",
	"返品理由",
	"店舗名",
	"区分2",
	"商品群",
	"重点商品",
	"品目分類値コード1",
	"品目分類値コード2",
	"品目分類値コード3",
	"部門7追加属性1",
	"部門7追加属性2",
	"部門7追加属性3",
	"部門7追加属性4",
	"部門7追加属性5",
	"部門7追加属性6",
	"部門7追加属性7",
	"部門7追加属性8",
	"部門7追加属性9",
	"部門7追加属性10",
	"商品群_20期",
	"内訳①_20期",
	"内訳②_20期",
	"内訳③_20期",
	"重点商品予備1",
	"重点商品予備2",
	"重点商品予備3",
	"重点商品予備4",
	"重点商品予備5",
	"置き換え_名称",
	"需給予測1",
	"需給予測2",
	"需給予測3"
) as
select "出荷年月日",
    "チャネル",
    "通信経路大区分",
    "取引形態区分",
    "得意先",
    "販売商品",
    "商品構成",
    "在庫商品コード",
    "受注区分コード",
    "c受注区分名",
    "数量",
    "返品数量",
    "純売上数量",
    "按分前金額_調整前",
    "明細金額_調整前",
    "按分前金額_調整後",
    "明細金額_調整後",
    "伝票値引按分後売上金額",
    "税抜按分利用ポイント",
    "税込按分利用ポイント",
    "チャネル大区分",
    "チャネル中区分",
    "チャネル小区分",
    "売上計上部門コード",
    "売上計上部門名",
    "返品理由コード",
    "返品理由",
    "店舗名",
    "区分2",
    "商品群",
    "重点商品",
    "品目分類値コード1",
    "品目分類値コード2",
    "品目分類値コード3",
    "部門7追加属性1",
    "部門7追加属性2",
    "部門7追加属性3",
    "部門7追加属性4",
    "部門7追加属性5",
    "部門7追加属性6",
    "部門7追加属性7",
    "部門7追加属性8",
    "部門7追加属性9",
    "部門7追加属性10",
    "商品群_20期",
    "内訳①_20期",
    "内訳②_20期",
    "内訳③_20期",
    "重点商品予備1",
    "重点商品予備2",
    "重点商品予備3",
    "重点商品予備4",
    "重点商品予備5",
    "置き換え_名称",
    "需給予測1",
    "需給予測2",
    "需給予測3"
from dev_dna_core.jpdcledw_integration."wqwt74戦略商品別売上ｐ";

USE schema JPNEDW_ACCESS ;
create or replace view DM_INTEGRATION_DLY(
	"jcp_data_source",
	"jcp_plan_type",
	"jcp_analysis_type",
	"jcp_data_category",
	"jcp_date",
	"jcp_cstm_cd",
	"jcp_str_cd",
	"jcp_chn_cd",
	"jcp_chn_offc_cd",
	"jcp_jan_cd",
	"jcp_plan_item_type",
	"jcp_new_item_type",
	"jcp_account",
	"jcp_create_date",
	"jcp_load_date",
	"jcp_unit_prc",
	"jcp_qty",
	"jcp_amt",
	"so_rcv_dt",
	"so_ws_cd",
	"so_rtl_type",
	"so_rtl_cd",
	"so_trade_type",
	"so_shp_num",
	"so_trade_cd",
	"so_dep_cd",
	"so_chg_cd",
	"so_person_in_charge",
	"so_person_name",
	"so_rtl_name",
	"so_rtl_ho_cd",
	"so_rtl_address_cd",
	"so_data_type",
	"so_opt_fld",
	"so_item_nm",
	"so_item_cd_typ",
	"so_qty_type",
	"so_price",
	"so_price_type",
	"so_shp_ws_cd",
	"so_rep_name_kanji",
	"so_rep_info",
	"so_rtl_name_kanji",
	"so_item_nm_kanji",
	"so_unt_prc",
	"so_net_prc",
	"so_sales_chan_type",
	"si_calday",
	"si_fiscper",
	"si_material",
	"si_jcp_create_dt",
	"si_jcp_pan_flg",
	"si_jcp_445_ymd_dt",
	"si_jcp_update_flg",
	"si_jcp_update_dt",
	"pn_cddfc",
	"pn_cddmp",
	"pn_account_sub_cd",
	"pn_plan_type",
	"pn_promotion_nm",
	"pn_item_group",
	"tp_promo_cd",
	"tp_promo_nm",
	"tp_cstm_cd",
	"tp_val_fixed_type",
	"tp_val_fixed_appl_cnt",
	"tp_sap_cnt_dt",
	"tp_sap_cancel_dt",
	"tp_promo_status_cd",
	"tp_approve_status_cd",
	"tp_rslt_status_cd",
	"tp_file_status_cd",
	"tp_cstctr_cd",
	"tp_tsp_acnt_cd",
	"tp_bme_promo_cd",
	"tp_unit_cost",
	"tp_apl_create_emp_cd",
	"tp_apl_apply_emp_cd",
	"tp_apl_approve_emp_cd",
	"tp_apl_target_chn_cd",
	"tp_apl_branch_cd",
	"tp_apl_payee_typ_cd",
	"tp_apl_direct_flg",
	"tp_apl_midst_appl_flg",
	"tp_apl_fiscal_year",
	"tp_apl_create_dt",
	"tp_apl_apply_dt",
	"tp_apl_approve_dt",
	"tp_apl_apply_begin_dt",
	"tp_apl_apply_end_dt",
	"tp_apl_comment1",
	"tp_apl_update_dt",
	"tp_apl_fix_emp_cd",
	"tp_apl_fix_dt",
	"tp_apl_update_emp_cd",
	"tp_apl_appl_rel_flg",
	"tp_apld_cstm_head_office_cd",
	"tp_apld_contract_no",
	"tp_apld_update_dt",
	"tp_res_apply_emp_cd",
	"tp_res_approve_emp_cd",
	"tp_res_recognize_emp_cd",
	"tp_res_payee_cd",
	"tp_res_branch_cd",
	"tp_res_payee_typ_cd",
	"tp_res_direct_flg",
	"tp_res_fiscal_year",
	"tp_res_create_dt",
	"tp_res_apply_dt",
	"tp_res_approve_dt",
	"tp_res_recognize_dt",
	"tp_res_payment_dt",
	"tp_res_payment_plan_dt",
	"tp_res_trnsfr_dtl_cd",
	"tp_res_comment1",
	"tp_res_update_dt",
	"tp_res_tax_flg",
	"tp_resd_cstm_head_office_cd",
	"tp_resd_payee_typ_cd",
	"tp_resd_contract_no",
	"tp_resd_comment1",
	"tp_resd_direct_dtl_flg",
	"tp_resd_servey_total_dt",
	"tp_resd_rslt_input_dt",
	"tp_resd_cancel_dt",
	"tp_resd_update_dt",
	"tp_resd_reason_cd",
	"tp_resd_tax_rate",
	"tp_resd_tax_amt",
	"plnt",
	"sales_grp",
	"so_id",
	"so_jcp_rec_seq"
) as
select jcp_data_source as "jcp_data_source",
    jcp_plan_type as "jcp_plan_type",
    jcp_analysis_type as "jcp_analysis_type",
    jcp_data_category as "jcp_data_category",
    jcp_date as "jcp_date",
    jcp_cstm_cd as "jcp_cstm_cd",
    jcp_str_cd as "jcp_str_cd",
    jcp_chn_cd as "jcp_chn_cd",
    jcp_chn_offc_cd as "jcp_chn_offc_cd",
    jcp_jan_cd as "jcp_jan_cd",
    jcp_plan_item_type as "jcp_plan_item_type",
    jcp_new_item_type as "jcp_new_item_type",
    jcp_account as "jcp_account",
    jcp_create_date as "jcp_create_date",
    jcp_load_date as "jcp_load_date",
    jcp_unit_prc as "jcp_unit_prc",
    jcp_qty as "jcp_qty",
    jcp_amt as "jcp_amt",
    so_rcv_dt as "so_rcv_dt",
    so_ws_cd as "so_ws_cd",
    so_rtl_type as "so_rtl_type",
    so_rtl_cd as "so_rtl_cd",
    so_trade_type as "so_trade_type",
    so_shp_num as "so_shp_num",
    so_trade_cd as "so_trade_cd",
    so_dep_cd as "so_dep_cd",
    so_chg_cd as "so_chg_cd",
    so_person_in_charge as "so_person_in_charge",
    so_person_name as "so_person_name",
    so_rtl_name as "so_rtl_name",
    so_rtl_ho_cd as "so_rtl_ho_cd",
    so_rtl_address_cd as "so_rtl_address_cd",
    so_data_type as "so_data_type",
    so_opt_fld as "so_opt_fld",
    so_item_nm as "so_item_nm",
    so_item_cd_typ as "so_item_cd_typ",
    so_qty_type as "so_qty_type",
    so_price as "so_price",
    so_price_type as "so_price_type",
    so_shp_ws_cd as "so_shp_ws_cd",
    so_rep_name_kanji as "so_rep_name_kanji",
    so_rep_info as "so_rep_info",
    so_rtl_name_kanji as "so_rtl_name_kanji",
    so_item_nm_kanji as "so_item_nm_kanji",
    so_unt_prc as "so_unt_prc",
    so_net_prc as "so_net_prc",
    so_sales_chan_type as "so_sales_chan_type",
    si_calday as "si_calday",
    si_fiscper as "si_fiscper",
    si_material as "si_material",
    si_jcp_create_dt as "si_jcp_create_dt",
    si_jcp_pan_flg as "si_jcp_pan_flg",
    si_jcp_445_ymd_dt as "si_jcp_445_ymd_dt",
    si_jcp_update_flg as "si_jcp_update_flg",
    si_jcp_update_dt as "si_jcp_update_dt",
    pn_cddfc as "pn_cddfc",
    pn_cddmp as "pn_cddmp",
    pn_account_sub_cd as "pn_account_sub_cd",
    pn_plan_type as "pn_plan_type",
    pn_promotion_nm as "pn_promotion_nm",
    pn_item_group as "pn_item_group",
    tp_promo_cd as "tp_promo_cd",
    tp_promo_nm as "tp_promo_nm",
    tp_cstm_cd as "tp_cstm_cd",
    tp_val_fixed_type as "tp_val_fixed_type",
    tp_val_fixed_appl_cnt as "tp_val_fixed_appl_cnt",
    tp_sap_cnt_dt as "tp_sap_cnt_dt",
    tp_sap_cancel_dt as "tp_sap_cancel_dt",
    tp_promo_status_cd as "tp_promo_status_cd",
    tp_approve_status_cd as "tp_approve_status_cd",
    tp_rslt_status_cd as "tp_rslt_status_cd",
    tp_file_status_cd as "tp_file_status_cd",
    tp_cstctr_cd as "tp_cstctr_cd",
    tp_tsp_acnt_cd as "tp_tsp_acnt_cd",
    tp_bme_promo_cd as "tp_bme_promo_cd",
    tp_unit_cost as "tp_unit_cost",
    tp_apl_create_emp_cd as "tp_apl_create_emp_cd",
    tp_apl_apply_emp_cd as "tp_apl_apply_emp_cd",
    tp_apl_approve_emp_cd as "tp_apl_approve_emp_cd",
    tp_apl_target_chn_cd as "tp_apl_target_chn_cd",
    tp_apl_branch_cd as "tp_apl_branch_cd",
    tp_apl_payee_typ_cd as "tp_apl_payee_typ_cd",
    tp_apl_direct_flg as "tp_apl_direct_flg",
    tp_apl_midst_appl_flg as "tp_apl_midst_appl_flg",
    tp_apl_fiscal_year as "tp_apl_fiscal_year",
    tp_apl_create_dt as "tp_apl_create_dt",
    tp_apl_apply_dt as "tp_apl_apply_dt",
    tp_apl_approve_dt as "tp_apl_approve_dt",
    tp_apl_apply_begin_dt as "tp_apl_apply_begin_dt",
    tp_apl_apply_end_dt as "tp_apl_apply_end_dt",
    tp_apl_comment1 as "tp_apl_comment1",
    tp_apl_update_dt as "tp_apl_update_dt",
    tp_apl_fix_emp_cd as "tp_apl_fix_emp_cd",
    tp_apl_fix_dt as "tp_apl_fix_dt",
    tp_apl_update_emp_cd as "tp_apl_update_emp_cd",
    tp_apl_appl_rel_flg as "tp_apl_appl_rel_flg",
    tp_apld_cstm_head_office_cd as "tp_apld_cstm_head_office_cd",
    tp_apld_contract_no as "tp_apld_contract_no",
    tp_apld_update_dt as "tp_apld_update_dt",
    tp_res_apply_emp_cd as "tp_res_apply_emp_cd",
    tp_res_approve_emp_cd as "tp_res_approve_emp_cd",
    tp_res_recognize_emp_cd as "tp_res_recognize_emp_cd",
    tp_res_payee_cd as "tp_res_payee_cd",
    tp_res_branch_cd as "tp_res_branch_cd",
    tp_res_payee_typ_cd as "tp_res_payee_typ_cd",
    tp_res_direct_flg as "tp_res_direct_flg",
    tp_res_fiscal_year as "tp_res_fiscal_year",
    tp_res_create_dt as "tp_res_create_dt",
    tp_res_apply_dt as "tp_res_apply_dt",
    tp_res_approve_dt as "tp_res_approve_dt",
    tp_res_recognize_dt as "tp_res_recognize_dt",
    tp_res_payment_dt as "tp_res_payment_dt",
    tp_res_payment_plan_dt as "tp_res_payment_plan_dt",
    tp_res_trnsfr_dtl_cd as "tp_res_trnsfr_dtl_cd",
    tp_res_comment1 as "tp_res_comment1",
    tp_res_update_dt as "tp_res_update_dt",
    tp_res_tax_flg as "tp_res_tax_flg",
    tp_resd_cstm_head_office_cd as "tp_resd_cstm_head_office_cd",
    tp_resd_payee_typ_cd as "tp_resd_payee_typ_cd",
    tp_resd_contract_no as "tp_resd_contract_no",
    tp_resd_comment1 as "tp_resd_comment1",
    tp_resd_direct_dtl_flg as "tp_resd_direct_dtl_flg",
    tp_resd_servey_total_dt as "tp_resd_servey_total_dt",
    tp_resd_rslt_input_dt as "tp_resd_rslt_input_dt",
    tp_resd_cancel_dt as "tp_resd_cancel_dt",
    tp_resd_update_dt as "tp_resd_update_dt",
    tp_resd_reason_cd as "tp_resd_reason_cd",
    tp_resd_tax_rate as "tp_resd_tax_rate",
    tp_resd_tax_amt as "tp_resd_tax_amt",
    plnt as "plnt",
    sales_grp as "sales_grp",
    so_id as "so_id",
    so_jcp_rec_seq as "so_jcp_rec_seq"
from dev_dna_core.jpnedw_integration.dm_integration_dly;
create or replace view DW_POP6_ANALYSIS_ACTIVE_DATA_JP(
	"data_type",
	"cntry_cd",
	"visit_id",
	"task_group",
	"task_id",
	"task_name",
	"audit_form_id",
	"audit_form",
	"section_id",
	"section",
	"subsection_id",
	"subsection",
	"field_id",
	"field_code",
	"field_label",
	"field_type",
	"dependent_on_field_id",
	"sku_id",
	"sku",
	"response",
	"visit_date",
	"check_in_datetime",
	"check_out_datetime",
	"popdb_id",
	"pop_code",
	"pop_name",
	"address",
	"check_in_longitude",
	"check_in_latitude",
	"check_out_longitude",
	"check_out_latitude",
	"check_in_photo",
	"check_out_photo",
	"username",
	"user_full_name",
	"superior_username",
	"superior_name",
	"planned_visit",
	"cancelled_visit",
	"cancellation_reason",
	"cancellation_note",
	"promotion_plan_id",
	"promotion_code",
	"promotion_name",
	"promotion_mechanics",
	"promotion_type",
	"promotion_price",
	"promotion_compliance",
	"actual_price",
	"non_compliance_reason",
	"photo",
	"product_attribute_id",
	"product_attribute",
	"product_attribute_value_id",
	"product_attribute_value",
	"pop_status",
	"pop_longitude",
	"pop_latitude",
	"country",
	"channel",
	"retail_environment_ps",
	"customer",
	"sales_group_code",
	"sales_group_name",
	"customer_grade",
	"external_pop_code",
	"business_unit_name",
	"territory_or_region",
	"prod_status",
	"productdb_id",
	"barcode",
	"unit_price",
	"display_order",
	"launch_date",
	"largest_uom_quantity",
	"middle_uom_quantity",
	"smallest_uom_quantity",
	"company",
	"sku_english",
	"sku_code",
	"ps_category",
	"ps_segment",
	"ps_category_segment",
	"country_l1",
	"regional_franchise_l2",
	"franchise_l3",
	"brand_l4",
	"sub_category_l5",
	"platform_l6",
	"variance_l7",
	"pack_size_l8",
	"sap_matl_num",
	"msl_rank",
	"user_status",
	"userdb_id",
	"first_name",
	"last_name",
	"team",
	"authorisation_group",
	"email_address",
	"user_longitude",
	"user_latitude",
	"display_plan_id",
	"display_type",
	"display_code",
	"display_name",
	"display_start_date",
	"display_end_date",
	"checklist_method",
	"display_number",
	"display_comments",
	"y/n_flag",
	"mkt_share",
	"planned_visit_date",
	"visited_flag",
	"facing",
	"is_eyelevel"
) as
select data_type as "data_type",
    cntry_cd as "cntry_cd",
    visit_id as "visit_id",
    task_group as "task_group",
    task_id as "task_id",
    task_name as "task_name",
    audit_form_id as "audit_form_id",
    audit_form as "audit_form",
    section_id as "section_id",
    section as "section",
    subsection_id as "subsection_id",
    subsection as "subsection",
    field_id as "field_id",
    field_code as "field_code",
    field_label as "field_label",
    field_type as "field_type",
    dependent_on_field_id as "dependent_on_field_id",
    sku_id as "sku_id",
    sku as "sku",
    response as "response",
    visit_date as "visit_date",
    check_in_datetime as "check_in_datetime",
    check_out_datetime as "check_out_datetime",
    popdb_id as "popdb_id",
    pop_code as "pop_code",
    pop_name as "pop_name",
    address as "address",
    check_in_longitude as "check_in_longitude",
    check_in_latitude as "check_in_latitude",
    check_out_longitude as "check_out_longitude",
    check_out_latitude as "check_out_latitude",
    check_in_photo as "check_in_photo",
    check_out_photo as "check_out_photo",
    username as "username",
    user_full_name as "user_full_name",
    superior_username as "superior_username",
    superior_name as "superior_name",
    planned_visit as "planned_visit",
    cancelled_visit as "cancelled_visit",
    cancellation_reason as "cancellation_reason",
    cancellation_note as "cancellation_note",
    promotion_plan_id as "promotion_plan_id",
    promotion_code as "promotion_code",
    promotion_name as "promotion_name",
    promotion_mechanics as "promotion_mechanics",
    promotion_type as "promotion_type",
    promotion_price as "promotion_price",
    promotion_compliance as "promotion_compliance",
    actual_price as "actual_price",
    non_compliance_reason as "non_compliance_reason",
    photo as "photo",
    product_attribute_id as "product_attribute_id",
    product_attribute as "product_attribute",
    product_attribute_value_id as "product_attribute_value_id",
    product_attribute_value as "product_attribute_value",
    pop_status as "pop_status",
    pop_longitude as "pop_longitude",
    pop_latitude as "pop_latitude",
    country as "country",
    channel as "channel",
    retail_environment_ps as "retail_environment_ps",
    customer as "customer",
    sales_group_code as "sales_group_code",
    sales_group_name as "sales_group_name",
    customer_grade as "customer_grade",
    external_pop_code as "external_pop_code",
    business_unit_name as "business_unit_name",
    territory_or_region as "territory_or_region",
    prod_status as "prod_status",
    productdb_id as "productdb_id",
    barcode as "barcode",
    unit_price as "unit_price",
    display_order as "display_order",
    launch_date as "launch_date",
    largest_uom_quantity as "largest_uom_quantity",
    middle_uom_quantity as "middle_uom_quantity",
    smallest_uom_quantity as "smallest_uom_quantity",
    company as "company",
    sku_english as "sku_english",
    sku_code as "sku_code",
    ps_category as "ps_category",
    ps_segment as "ps_segment",
    ps_category_segment as "ps_category_segment",
    country_l1 as "country_l1",
    regional_franchise_l2 as "regional_franchise_l2",
    franchise_l3 as "franchise_l3",
    brand_l4 as "brand_l4",
    sub_category_l5 as "sub_category_l5",
    platform_l6 as "platform_l6",
    variance_l7 as "variance_l7",
    pack_size_l8 as "pack_size_l8",
    sap_matl_num as "sap_matl_num",
    msl_rank as "msl_rank",
    user_status as "user_status",
    userdb_id as "userdb_id",
    first_name as "first_name",
    last_name as "last_name",
    team as "team",
    authorisation_group as "authorisation_group",
    email_address as "email_address",
    user_longitude as "user_longitude",
    user_latitude as "user_latitude",
    display_plan_id as "display_plan_id",
    display_type as "display_type",
    display_code as "display_code",
    display_name as "display_name",
    display_start_date as "display_start_date",
    display_end_date as "display_end_date",
    checklist_method as "checklist_method",
    display_number as "display_number",
    display_comments as "display_comments",
    "y/n_flag",
    mkt_share as "mkt_share",
    planned_visit_date as "planned_visit_date",
    visited_flag as "visited_flag",
    facing as "facing",
    is_eyelevel as "is_eyelevel"
from dev_dna_core.jpnedw_integration.dw_pop6_analysis_active_data_jp;
create or replace view DW_SO_PLANET_ERR(
	"jcp_rec_seq",
	"id",
	"rcv_dt",
	"test_flag",
	"bgn_sndr_cd",
	"ws_cd",
	"rtl_type",
	"rtl_cd",
	"trade_type",
	"shp_date",
	"shp_num",
	"trade_cd",
	"dep_cd",
	"chg_cd",
	"person_in_charge",
	"person_name",
	"rtl_name",
	"rtl_ho_cd",
	"rtl_address_cd",
	"data_type",
	"opt_fld",
	"item_nm",
	"item_cd_typ",
	"item_cd",
	"qty",
	"qty_type",
	"price",
	"price_type",
	"bgn_sndr_cd_gln",
	"rcv_cd_gln",
	"ws_cd_gln",
	"shp_ws_cd",
	"shp_ws_cd_gln",
	"rep_name_kanji",
	"rep_info",
	"trade_cd_gln",
	"rtl_cd_gln",
	"rtl_name_kanji",
	"rtl_ho_cd_gln",
	"item_cd_gtin",
	"item_nm_kanji",
	"unt_prc",
	"net_prc",
	"sales_chan_type",
	"jcp_create_date",
	"jcp_rcv_dt_dupli",
	"jcp_test_flag_dupli",
	"jcp_qty_dupli",
	"jcp_price_dupli",
	"jcp_unt_prc_dupli",
	"jcp_net_prc_dupli",
	"export_flag"
) as
select jcp_rec_seq as "jcp_rec_seq",
    id as "id",
    rcv_dt as "rcv_dt",
    test_flag as "test_flag",
    bgn_sndr_cd as "bgn_sndr_cd",
    ws_cd as "ws_cd",
    rtl_type as "rtl_type",
    rtl_cd as "rtl_cd",
    trade_type as "trade_type",
    shp_date as "shp_date",
    shp_num as "shp_num",
    trade_cd as "trade_cd",
    dep_cd as "dep_cd",
    chg_cd as "chg_cd",
    person_in_charge as "person_in_charge",
    person_name as "person_name",
    rtl_name as "rtl_name",
    rtl_ho_cd as "rtl_ho_cd",
    rtl_address_cd as "rtl_address_cd",
    data_type as "data_type",
    opt_fld as "opt_fld",
    item_nm as "item_nm",
    item_cd_typ as "item_cd_typ",
    item_cd as "item_cd",
    qty as "qty",
    qty_type as "qty_type",
    price as "price",
    price_type as "price_type",
    bgn_sndr_cd_gln as "bgn_sndr_cd_gln",
    rcv_cd_gln as "rcv_cd_gln",
    ws_cd_gln as "ws_cd_gln",
    shp_ws_cd as "shp_ws_cd",
    shp_ws_cd_gln as "shp_ws_cd_gln",
    rep_name_kanji as "rep_name_kanji",
    rep_info as "rep_info",
    trade_cd_gln as "trade_cd_gln",
    rtl_cd_gln as "rtl_cd_gln",
    rtl_name_kanji as "rtl_name_kanji",
    rtl_ho_cd_gln as "rtl_ho_cd_gln",
    item_cd_gtin as "item_cd_gtin",
    item_nm_kanji as "item_nm_kanji",
    unt_prc as "unt_prc",
    net_prc as "net_prc",
    sales_chan_type as "sales_chan_type",
    jcp_create_date as "jcp_create_date",
    jcp_rcv_dt_dupli as "jcp_rcv_dt_dupli",
    jcp_test_flag_dupli as "jcp_test_flag_dupli",
    jcp_qty_dupli as "jcp_qty_dupli",
    jcp_price_dupli as "jcp_price_dupli",
    jcp_unt_prc_dupli as "jcp_unt_prc_dupli",
    jcp_net_prc_dupli as "jcp_net_prc_dupli",
    export_flag as "export_flag"
from dev_dna_core.jpnitg_integration.dw_so_planet_err;
create or replace view DW_SO_PLANET_ERR_CD_2(
	"jcp_rec_seq",
	"error_cd",
	"exec_flag",
	"export_flag",
	"jcp_create_date"
) as
select jcp_rec_seq as "jcp_rec_seq",
    error_cd as "error_cd",
    exec_flag as "exec_flag",
    export_flag as "export_flag",
    jcp_create_date as "jcp_create_date"
from dev_dna_core.jpnitg_integration.dw_so_planet_err_cd_2;
create or replace view EDI_CHN_M(
	"create_dt",
	"create_user",
	"update_dt",
	"update_user",
	"reg_dt",
	"chn_cd",
	"lgl_nm",
	"cmmn_nm",
	"adrs",
	"acnt_prsn_cd",
	"rank",
	"chn_offc_cd",
	"frnc",
	"sgmt",
	"an_typ",
	"pj_typ",
	"sales_group",
	"scnd_acnt_prsn"
) as
select create_dt as "create_dt",
    create_user as "create_user",
    update_dt as "update_dt",
    update_user as "update_user",
    reg_dt as "reg_dt",
    chn_cd as "chn_cd",
    lgl_nm as "lgl_nm",
    cmmn_nm as "cmmn_nm",
    adrs as "adrs",
    acnt_prsn_cd as "acnt_prsn_cd",
    rank as "rank",
    chn_offc_cd as "chn_offc_cd",
    frnc as "frnc",
    sgmt as "sgmt",
    an_typ as "an_typ",
    pj_typ as "pj_typ",
    sales_group as "sales_group",
    scnd_acnt_prsn as "scnd_acnt_prsn"
from dev_dna_core.jpnedw_integration.edi_chn_m;
create or replace view EDI_CSTM_M(
	"create_dt",
	"create_user",
	"update_dt",
	"update_user",
	"reg_dt",
	"cstm_cd",
	"cstm_nm",
	"cstm_nm_kn",
	"cstm_nm_knj",
	"adrs",
	"adrs_kn",
	"adrs_knj",
	"pst_cd",
	"tel_num",
	"fax_nun",
	"plnt_cd",
	"ship_dpt",
	"ship_ld_tm",
	"jis_prfct_cd",
	"jis_city_cd",
	"cstm_typ",
	"bl_cls_dt",
	"trd_typ_cg_flg",
	"jrsd_dpt_cd",
	"acnt_prsn_cd",
	"buy_from_cd",
	"rebate_rep_cd",
	"updateflg",
	"cust_tfi_num",
	"transporter_id",
	"transport_fee_id",
	"transport_timing",
	"cmnt1"
) as
select create_dt as "create_dt",
    create_user as "create_user",
    update_dt as "update_dt",
    update_user as "update_user",
    reg_dt as "reg_dt",
    cstm_cd as "cstm_cd",
    cstm_nm as "cstm_nm",
    cstm_nm_kn as "cstm_nm_kn",
    cstm_nm_knj as "cstm_nm_knj",
    adrs as "adrs",
    adrs_kn as "adrs_kn",
    adrs_knj as "adrs_knj",
    pst_cd as "pst_cd",
    tel_num as "tel_num",
    fax_nun as "fax_nun",
    plnt_cd as "plnt_cd",
    ship_dpt as "ship_dpt",
    ship_ld_tm as "ship_ld_tm",
    jis_prfct_cd as "jis_prfct_cd",
    jis_city_cd as "jis_city_cd",
    cstm_typ as "cstm_typ",
    bl_cls_dt as "bl_cls_dt",
    trd_typ_cg_flg as "trd_typ_cg_flg",
    jrsd_dpt_cd as "jrsd_dpt_cd",
    acnt_prsn_cd as "acnt_prsn_cd",
    buy_from_cd as "buy_from_cd",
    rebate_rep_cd as "rebate_rep_cd",
    updateflg as "updateflg",
    cust_tfi_num as "cust_tfi_num",
    transporter_id as "transporter_id",
    transport_fee_id as "transport_fee_id",
    transport_timing as "transport_timing",
    cmnt1 as "cmnt1"
from dev_dna_core.jpnedw_integration.edi_cstm_m;
create or replace view EDI_ITEM_M(
	"create_dt",
	"create_user",
	"update_dt",
	"update_user",
	"reg_dt",
	"item_cd",
	"item_nm",
	"iten_nm_kn",
	"iten_nm_knj",
	"jan_cd",
	"itf_cd",
	"pc",
	"unt_prc",
	"sub_frnch",
	"jan_cd_so",
	"itf_cd_so",
	"updateflg",
	"base_prod",
	"variant",
	"put_up",
	"mega_brnd",
	"brnd",
	"dlt_flg",
	"base_uom",
	"item_cd_jd",
	"sap_cstm_type",
	"mega_brnd_chkflg",
	"planet_l3_flg",
	"rel_dt",
	"discon_dt",
	"new_prod_type",
	"prom_goods_flg",
	"parent_item_cd",
	"imp_item_flg",
	"succeeding_item_cd",
	"ldw_flg01",
	"ldw_flg02",
	"ldw_flg03"
) as
select create_dt as "create_dt",
    create_user as "create_user",
    update_dt as "update_dt",
    update_user as "update_user",
    reg_dt as "reg_dt",
    item_cd as "item_cd",
    item_nm as "item_nm",
    iten_nm_kn as "iten_nm_kn",
    iten_nm_knj as "iten_nm_knj",
    jan_cd as "jan_cd",
    itf_cd as "itf_cd",
    pc as "pc",
    unt_prc as "unt_prc",
    sub_frnch as "sub_frnch",
    jan_cd_so as "jan_cd_so",
    itf_cd_so as "itf_cd_so",
    updateflg as "updateflg",
    base_prod as "base_prod",
    variant as "variant",
    put_up as "put_up",
    mega_brnd as "mega_brnd",
    brnd as "brnd",
    dlt_flg as "dlt_flg",
    base_uom as "base_uom",
    item_cd_jd as "item_cd_jd",
    sap_cstm_type as "sap_cstm_type",
    mega_brnd_chkflg as "mega_brnd_chkflg",
    planet_l3_flg as "planet_l3_flg",
    rel_dt as "rel_dt",
    discon_dt as "discon_dt",
    new_prod_type as "new_prod_type",
    prom_goods_flg as "prom_goods_flg",
    parent_item_cd as "parent_item_cd",
    imp_item_flg as "imp_item_flg",
    succeeding_item_cd as "succeeding_item_cd",
    ldw_flg01 as "ldw_flg01",
    ldw_flg02 as "ldw_flg02",
    ldw_flg03 as "ldw_flg03"
from dev_dna_core.jpnedw_integration.edi_item_m;
create or replace view EDI_STORE_M(
	"create_dt",
	"create_user",
	"update_dt",
	"update_user",
	"reg_dt",
	"str_cd",
	"lgl_nm_knj1",
	"lgl_nm_knj2",
	"lgl_nm_kn",
	"cmmn_nm_knj",
	"cmmn_nm_kn",
	"adrs_knj1",
	"adrs_knj2",
	"adrs_kn",
	"pst_co",
	"tel_no",
	"jis_prfct_c",
	"jis_city_cd",
	"trd_cd",
	"trd_offc_cd",
	"chn_cd",
	"chn_offc_cd",
	"chn_cd_oth",
	"emp_cd_kk",
	"all_str_ass",
	"agrm_str",
	"pj_ass",
	"emp_cd_roc"
) as
select create_dt as "create_dt",
    create_user as "create_user",
    update_dt as "update_dt",
    update_user as "update_user",
    reg_dt as "reg_dt",
    str_cd as "str_cd",
    lgl_nm_knj1 as "lgl_nm_knj1",
    lgl_nm_knj2 as "lgl_nm_knj2",
    lgl_nm_kn as "lgl_nm_kn",
    cmmn_nm_knj as "cmmn_nm_knj",
    cmmn_nm_kn as "cmmn_nm_kn",
    adrs_knj1 as "adrs_knj1",
    adrs_knj2 as "adrs_knj2",
    adrs_kn as "adrs_kn",
    pst_co as "pst_co",
    tel_no as "tel_no",
    jis_prfct_c as "jis_prfct_c",
    jis_city_cd as "jis_city_cd",
    trd_cd as "trd_cd",
    trd_offc_cd as "trd_offc_cd",
    chn_cd as "chn_cd",
    chn_offc_cd as "chn_offc_cd",
    chn_cd_oth as "chn_cd_oth",
    emp_cd_kk as "emp_cd_kk",
    all_str_ass as "all_str_ass",
    agrm_str as "agrm_str",
    pj_ass as "pj_ass",
    emp_cd_roc as "emp_cd_roc"
from dev_dna_core.jpnedw_integration.edi_store_m;
create or replace view MT_ACCOUNT_KEY(
	"accounting_code",
	"key_figure_#",
	"key_figure_nm",
	"key_figure_nm_knj",
	"key_figure_nm_dsp",
	"csw_category_1",
	"csw_sort_key",
	"sign_of_number",
	"reference_table",
	"reference_column",
	"reference_field",
	"feild_condition",
	"delete_flag",
	"update_dt",
	"update_user"
) as
select accounting_code as "accounting_code",
    "key_figure_#",
    key_figure_nm as "key_figure_nm",
    key_figure_nm_knj as "key_figure_nm_knj",
    key_figure_nm_dsp as "key_figure_nm_dsp",
    csw_category_1 as "csw_category_1",
    csw_sort_key as "csw_sort_key",
    sign_of_number as "sign_of_number",
    reference_table as "reference_table",
    reference_column as "reference_column",
    reference_field as "reference_field",
    feild_condition as "feild_condition",
    delete_flag as "delete_flag",
    update_dt as "update_dt",
    update_user as "update_user"
from dev_dna_core.jpnedw_integration.mt_account_key;
create or replace view MT_CHN_MJP_EMP_CHG(
	"chn_cd",
	"fc_cd",
	"emp_cd",
	"division",
	"division_group",
	"update_dt",
	"update_user"
) as
select chn_cd as "chn_cd",
    fc_cd as "fc_cd",
    emp_cd as "emp_cd",
    division as "division",
    division_group as "division_group",
    update_dt as "update_dt",
    update_user as "update_user"
from dev_dna_core.jpnedw_integration.mt_chn_mjp_emp_chg;
create or replace view MT_CLD(
	"ymd_dt",
	"year",
	"year_445",
	"year_15",
	"year_20",
	"half",
	"half_nm",
	"half_445",
	"half_445_nm",
	"half_15",
	"half_15_nm",
	"half_20",
	"half_20_nm",
	"quarter",
	"quarter_nm",
	"quarter_445",
	"quarter_445_nm",
	"quarter_15",
	"quarter_15_nm",
	"quarter_20",
	"quarter_20_nm",
	"month",
	"month_nm",
	"month_445",
	"month_445_nm",
	"month_15",
	"month_15_nm",
	"month_20",
	"month_20_nm",
	"ymonth_445",
	"ymonth_15",
	"ymonth_20",
	"week_ms",
	"week_ss",
	"mweek_445",
	"mweek_15ms",
	"mweek_15ss",
	"mweek_20",
	"mweek_445_iso",
	"mweek_15ms_iso",
	"mweek_15ss_iso",
	"mweek_20_iso",
	"week",
	"day",
	"day_of_week",
	"opr_flg",
	"sls_flg"
) as
select ymd_dt as "ymd_dt",
    year as "year",
    year_445 as "year_445",
    year_15 as "year_15",
    year_20 as "year_20",
    half as "half",
    half_nm as "half_nm",
    half_445 as "half_445",
    half_445_nm as "half_445_nm",
    half_15 as "half_15",
    half_15_nm as "half_15_nm",
    half_20 as "half_20",
    half_20_nm as "half_20_nm",
    quarter as "quarter",
    quarter_nm as "quarter_nm",
    quarter_445 as "quarter_445",
    quarter_445_nm as "quarter_445_nm",
    quarter_15 as "quarter_15",
    quarter_15_nm as "quarter_15_nm",
    quarter_20 as "quarter_20",
    quarter_20_nm as "quarter_20_nm",
    month as "month",
    month_nm as "month_nm",
    month_445 as "month_445",
    month_445_nm as "month_445_nm",
    month_15 as "month_15",
    month_15_nm as "month_15_nm",
    month_20 as "month_20",
    month_20_nm as "month_20_nm",
    ymonth_445 as "ymonth_445",
    ymonth_15 as "ymonth_15",
    ymonth_20 as "ymonth_20",
    week_ms as "week_ms",
    week_ss as "week_ss",
    mweek_445 as "mweek_445",
    mweek_15ms as "mweek_15ms",
    mweek_15ss as "mweek_15ss",
    mweek_20 as "mweek_20",
    mweek_445_iso as "mweek_445_iso",
    mweek_15ms_iso as "mweek_15ms_iso",
    mweek_15ss_iso as "mweek_15ss_iso",
    mweek_20_iso as "mweek_20_iso",
    week as "week",
    day as "day",
    day_of_week as "day_of_week",
    opr_flg as "opr_flg",
    sls_flg as "sls_flg"
from dev_dna_core.jpnedw_integration.mt_cld;
create or replace view MT_EMP(
	"emp_cd",
	"emp_nm_knj",
	"emp_nm",
	"wwid",
	"slm_cd",
	"org_cd",
	"cstctr_cd",
	"emp_typ",
	"update_dt",
	"update_user"
) as
select emp_cd as "emp_cd",
    emp_nm_knj as "emp_nm_knj",
    emp_nm as "emp_nm",
    wwid as "wwid",
    slm_cd as "slm_cd",
    org_cd as "org_cd",
    cstctr_cd as "cstctr_cd",
    emp_typ as "emp_typ",
    update_dt as "update_dt",
    update_user as "update_user"
from dev_dna_core.jpnedw_integration.mt_emp;
create or replace view MT_JAN_DETAIL(
	"jan_cd",
	"product_name",
	"delete_flag",
	"update_dt",
	"update_user"
) as
select jan_cd as "jan_cd",
    product_name as "product_name",
    delete_flag as "delete_flag",
    update_dt as "update_dt",
    update_user as "update_user"
from dev_dna_core.jpnedw_integration.mt_jan_detail;
create or replace view MT_PRF(
	"prf_cd",
	"prf_nm_knj",
	"update_dt",
	"update_user"
) as
select prf_cd as "prf_cd",
    prf_nm_knj as "prf_nm_knj",
    update_dt as "update_dt",
    update_user as "update_user"
from dev_dna_core.jpnedw_integration.mt_prf;
create or replace view MT_SGMT(
	"sgmt",
	"sgmt_nm",
	"sgmt_nm_rep",
	"update_dt",
	"update_user"
) as
select sgmt as "sgmt",
    sgmt_nm as "sgmt_nm",
    sgmt_nm_rep as "sgmt_nm_rep",
    update_dt as "update_dt",
    update_user as "update_user"
from dev_dna_core.jpnedw_integration.mt_sgmt;
create or replace view MT_TP_STATUS_MAPPING(
	"jcp_data_category",
	"direct_flg",
	"promo_status_cd",
	"approve_status_cd",
	"rslt_status_cd",
	"mapping_status_cd1",
	"mapping_status_nm1",
	"mapping_status_cd2",
	"mapping_status_nm2",
	"mapping_status_cd3",
	"mapping_status_nm3",
	"mapping_status_cd4",
	"mapping_status_nm4",
	"mapping_status_cd5",
	"mapping_status_nm5",
	"delete_flag",
	"update_dt",
	"update_user"
) as
select jcp_data_category as "jcp_data_category",
    direct_flg as "direct_flg",
    promo_status_cd as "promo_status_cd",
    approve_status_cd as "approve_status_cd",
    rslt_status_cd as "rslt_status_cd",
    mapping_status_cd1 as "mapping_status_cd1",
    mapping_status_nm1 as "mapping_status_nm1",
    mapping_status_cd2 as "mapping_status_cd2",
    mapping_status_nm2 as "mapping_status_nm2",
    mapping_status_cd3 as "mapping_status_cd3",
    mapping_status_nm3 as "mapping_status_nm3",
    mapping_status_cd4 as "mapping_status_cd4",
    mapping_status_nm4 as "mapping_status_nm4",
    mapping_status_cd5 as "mapping_status_cd5",
    mapping_status_nm5 as "mapping_status_nm5",
    delete_flag as "delete_flag",
    update_dt as "update_dt",
    update_user as "update_user"
from dev_dna_core.jpnedw_integration.mt_tp_status_mapping;
create or replace view SDL_MDS_JP_JAN_DESCRIPTION(
	"id",
	"muid",
	"versionname",
	"versionnumber",
	"version_id",
	"versionflag",
	"name",
	"code",
	"changetrackingmask",
	"iten_nm_knj",
	"enterdatetime",
	"enterusername",
	"enterversionnumber",
	"lastchgdatetime",
	"lastchgusername",
	"lastchgversionnumber",
	"validationstatus"
) as
select id as "id",
    muid as "muid",
    versionname as "versionname",
    versionnumber as "versionnumber",
    version_id as "version_id",
    versionflag as "versionflag",
    name as "name",
    code as "code",
    changetrackingmask as "changetrackingmask",
    iten_nm_knj as "iten_nm_knj",
    enterdatetime as "enterdatetime",
    enterusername as "enterusername",
    enterversionnumber as "enterversionnumber",
    lastchgdatetime as "lastchgdatetime",
    lastchgusername as "lastchgusername",
    lastchgversionnumber as "lastchgversionnumber",
    validationstatus as "validationstatus"
from dev_dna_load.jpnsdl_raw.sdl_mds_jp_jan_description;
create or replace view SDL_MDS_JP_PRODUCT_CA_RATIO(
	"id",
	"muid",
	"versionname",
	"versionnumber",
	"version_id",
	"versionflag",
	"name",
	"code",
	"changetrackingmask",
	"rtl_ho_cd_code",
	"rtl_ho_cd_name",
	"rtl_ho_cd_id",
	"item_cd",
	"default_ca_ratio",
	"new_ca_ratio",
	"enterdatetime",
	"enterusername",
	"enterversionnumber",
	"lastchgdatetime",
	"lastchgusername",
	"lastchgversionnumber",
	"validationstatus"
) as
select id as "id",
    muid as "muid",
    versionname as "versionname",
    versionnumber as "versionnumber",
    version_id as "version_id",
    versionflag as "versionflag",
    name as "name",
    code as "code",
    changetrackingmask as "changetrackingmask",
    rtl_ho_cd_code as "rtl_ho_cd_code",
    rtl_ho_cd_name as "rtl_ho_cd_name",
    rtl_ho_cd_id as "rtl_ho_cd_id",
    item_cd as "item_cd",
    default_ca_ratio as "default_ca_ratio",
    new_ca_ratio as "new_ca_ratio",
    enterdatetime as "enterdatetime",
    enterusername as "enterusername",
    enterversionnumber as "enterversionnumber",
    lastchgdatetime as "lastchgdatetime",
    lastchgusername as "lastchgusername",
    lastchgversionnumber as "lastchgversionnumber",
    validationstatus as "validationstatus"
from dev_dna_load.jpnsdl_raw.sdl_mds_jp_product_ca_ratio;
create or replace view VW_JAN_CHANGE(
	"jan_cd",
	"item_cd"
) as
select jan_cd as "jan_cd",
    item_cd as "item_cd"
from dev_dna_core.jpnedw_integration.vw_jan_change;
create or replace view VW_M_ITEM_FRNCH_CDD(
	"item_cd",
	"frnch_group_cd",
	"frnch_group_nm",
	"frnch_group_srt",
	"frnch_cd",
	"frnch_nm",
	"frnch_srt",
	"mjr_prod_cd",
	"mjr_prod_nm",
	"mjr_prod_srt",
	"mjr_prod_cd2",
	"mjr_prod_nm2",
	"mjr_prod_srt2",
	"min_prod_cd",
	"min_prod_nm",
	"min_prod_srt"
) as
select item_cd as "item_cd",
    frnch_group_cd as "frnch_group_cd",
    frnch_group_nm as "frnch_group_nm",
    frnch_group_srt as "frnch_group_srt",
    frnch_cd as "frnch_cd",
    frnch_nm as "frnch_nm",
    frnch_srt as "frnch_srt",
    mjr_prod_cd as "mjr_prod_cd",
    mjr_prod_nm as "mjr_prod_nm",
    mjr_prod_srt as "mjr_prod_srt",
    mjr_prod_cd2 as "mjr_prod_cd2",
    mjr_prod_nm2 as "mjr_prod_nm2",
    mjr_prod_srt2 as "mjr_prod_srt2",
    min_prod_cd as "min_prod_cd",
    min_prod_nm as "min_prod_nm",
    min_prod_srt as "min_prod_srt"
from dev_dna_core.jpnedw_integration.vw_m_item_frnch_cdd;
create or replace view VW_POS_DLY(
	"account_key",
	"accounting_date",
	"mdsproductname",
	"year_445",
	"day",
	"half_445",
	"quarter_445",
	"ymonth_445",
	"month_445",
	"mweek_445",
	"jan_code",
	"mdsaccname",
	"frnch_nm",
	"mjr_prod_nm",
	"mjr_prod_nm2",
	"min_prod_nm",
	"prom_goods_flg",
	"category_code",
	"sub_category_code",
	"marker_code",
	"brand_code",
	"sub_brand_code",
	"size_id",
	"size_code",
	"form_type_code",
	"oral_function_code",
	"other_1",
	"other_2",
	"other_3",
	"other_4",
	"other_5",
	"planet_store_code",
	"cmmn_nm_knj",
	"prf_nm_knj",
	"chn_cd",
	"chn_offc_cd",
	"chn_lgl_nm",
	"chn1_lgl_nm",
	"sales_cat_1",
	"sales_cat_2",
	"quantity",
	"amount",
	"upload_dt"
) as
select account_key as "account_key",
    accounting_date as "accounting_date",
    mdsproductname as "mdsproductname",
    year_445 as "year_445",
    day as "day",
    half_445 as "half_445",
    quarter_445 as "quarter_445",
    ymonth_445 as "ymonth_445",
    month_445 as "month_445",
    mweek_445 as "mweek_445",
    jan_code as "jan_code",
    mdsaccname as "mdsaccname",
    frnch_nm as "frnch_nm",
    mjr_prod_nm as "mjr_prod_nm",
    mjr_prod_nm2 as "mjr_prod_nm2",
    min_prod_nm as "min_prod_nm",
    prom_goods_flg as "prom_goods_flg",
    category_code as "category_code",
    sub_category_code as "sub_category_code",
    marker_code as "marker_code",
    brand_code as "brand_code",
    sub_brand_code as "sub_brand_code",
    size_id as "size_id",
    size_code as "size_code",
    form_type_code as "form_type_code",
    oral_function_code as "oral_function_code",
    other_1 as "other_1",
    other_2 as "other_2",
    other_3 as "other_3",
    other_4 as "other_4",
    other_5 as "other_5",
    planet_store_code as "planet_store_code",
    cmmn_nm_knj as "cmmn_nm_knj",
    prf_nm_knj as "prf_nm_knj",
    chn_cd as "chn_cd",
    chn_offc_cd as "chn_offc_cd",
    chn_lgl_nm as "chn_lgl_nm",
    chn1_lgl_nm as "chn1_lgl_nm",
    sales_cat_1 as "sales_cat_1",
    sales_cat_2 as "sales_cat_2",
    quantity as "quantity",
    amount as "amount",
    upload_dt as "upload_dt"
from dev_dna_core.jpnedw_integration.vw_pos_dly;
