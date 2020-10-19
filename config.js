const configs = {
  pg: {
    pgIp: process.env.POSTGRES_IP || '192.168.100.234',
    pgPort: process.env.POSTGRES_PORT || 5432,
    pgDb: process.env.POSTGRES_DB || 'docker',
    pgName: process.env.POSTGRES_USER || 'swpc-user',
    pgPw: process.env.POSTGRES_PASSWORD || 'eprocurement',
    pgSchema: process.env.POSTGRES_SCHEMA || 'wiprocurement',
    idleTimeoutMillis: 180000,
  },
  forceProcess: (process.env.FORCE == 'true') || false,
  msgInterval: process.env.MSG_INTERVAL || 60000,
  kafkaHost: process.env.KAFKA_HOST && process.env.KAFKA_PORT ? `${process.env.KAFKA_HOST}:${process.env.KAFKA_PORT}` : '10.37.36.96:9092',
  kafka: {
    topic: {
      'alt': ['whq.plm.com.sapalt'],
      'pcb': ['whq.plm.com.pcbform'],
      'vendor': ['whq.sap.mm.ww_vendor_general'],
      'mat_doc': [
        'whq.sap.mm.wzs_mat_doc',
        'whq.sap.mm.wks_mat_doc',
        'whq.sap.mm.wtz_mat_doc',
        'whq.sap.mm.wcz_mat_doc',
        'whq.sap.mm.wmx_mat_doc',
        'whq.sap.mm.wih_mat_doc',
        'whq.sap.mm.wmt_mat_doc',
        'whq.sap.mm.wcq_mat_doc',
        'whq.sap.mm.wmcq_mat_doc',
        'whq.sap.mm.wcd_mat_doc',
        'whq.sap.mm.whq_mat_doc',
        'whq.sap.mm.aiih_mat_doc',
        'whq.sap.mm.witx_mat_doc',
      ],
      'po': [
        'whq.sap.mm.wzs_po',
        'whq.sap.mm.wks_po',
        'whq.sap.mm.wtz_po',
        'whq.sap.mm.wcz_po',
        'whq.sap.mm.wmx_po',
        'whq.sap.mm.wih_po',
        'whq.sap.mm.wmt_po',
        'whq.sap.mm.wcq_po',
        'whq.sap.mm.wmcq_po',
        'whq.sap.mm.wcd_po',
        'whq.sap.mm.whq_po',
        'whq.sap.mm.aiih_po',
        'whq.sap.mm.witx_po',
      ],
      'exchange_rate': ['whq.sap.fico.ww_exch_rate'],
      'mat_master': ['whq.sap.mm.ww_mat_master'],
    },
    //kafkaHost: process.env.PLM_KAFKA_HOST || '10.37.35.115', //dev
    //kafkaHost: process.env.PLM_KAFKA_HOST || '10.37.35.120', //qas
    kafkaHost: process.env.PLM_KAFKA_HOST || '10.37.34.110', //prd
    kafkaPort: process.env.PLM_KAFKA_PORT || 9092,
    //RegistryUrl: process.env.PLM_REGISTRY_URL || 'http://10.37.35.115:8081', //dev
    //RegistryUrl: process.env.PLM_REGISTRY_URL || 'http://10.37.35.120:8081', //qas
    RegistryUrl: process.env.PLM_REGISTRY_URL || 'http://10.37.34.110:8081', //prd
    saslName: process.env.PLM_SASL_NAME || 'whq.swpc',
    //saslPw: process.env.PLM_SASL_PASSWORD || 'eRYu#d2019', //dev
    //saslPw: process.env.PLM_SASL_PASSWORD || 'eRYu%q2019', //qas
    saslPw: process.env.PLM_SASL_PASSWORD || 'eRYu@p2019', //prd
  },
  saveMessage: (process.env.SAVE_MESSAGE == 'true') || false,
  requestTimeout: 1800000,
  env: process.env.NODE_ENV || 'test',
  port: process.env.DATA_PROCESS || 3010,
  scheduleTime: '0 0 0 * * *',
  DATAPROCESS_ZIP_DIR: process.env.DATAPROCESS_ZIP_DIR || 'back',
  DATAPROCESS_MESSAGE_DIR: process.env.DATAPROCESS_MESSAGE_DIR || 'Message',
  forceFromBegin: (process.env.FORCEFROMBEGIN == 'true') || false,
  topics: {
    info_record: {
      'wzs.sap.mm.info_record': [0, 1, 2],
      'wks.sap.mm.info_record': [0, 1, 2],
      'wtz.sap.mm.info_record': [0, 1, 2],
      'wcz.sap.mm.info_record': [0, 1, 2],
      'wmx.sap.mm.info_record': [0, 1, 2],
      'wih.sap.mm.info_record': [0, 1, 2],
      'wmt.sap.mm.info_record': [0, 1, 2],
      'wcq.sap.mm.info_record': [0, 1, 2],
      'wmcq.sap.mm.info_record': [0, 1, 2],
      'wcd.sap.mm.info_record': [0, 1, 2],
    },
    img: {
      'SAP_Outbound_Others': [0, 1, 2],
    },
  },
  mailConfig: {
    host: process.env.MONITOR_MAIL_HOST || 'email-smtp.us-east-1.amazonaws.com',
    port: 25,
    smtp_user: process.env.MONITOR_SMTP_USER || 'AKIAJQNAPPHWQWEMKXRQ',
    smtp_password: process.env.MONITOR_SMTP_PASSWORD || 'BFws8xp38DhhihqP3dZhSIBADKhSJjkspwKcMC3kHy/U',
  },
  sender: process.env.MONITOR_SENDER || 'warning@devpack.cc',
  recevier: process.env.MONITOR_RECEIVER || 'Susan_Hsieh@wistron.com, Zoe_JY_Chen@wistron.com, Mike_Liu@wistron.com',
}

module.exports = configs
