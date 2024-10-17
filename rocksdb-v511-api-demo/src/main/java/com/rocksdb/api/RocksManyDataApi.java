package com.rocksdb.api;

import com.alibaba.fastjson.JSONObject;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class RocksManyDataApi {

    private static final Logger logger = LoggerFactory.getLogger(RocksManyDataApi.class);


    // 因为RocksDB是由C++编写的，在Java中使用首先需要加载Native库
    static {
        // Loads the necessary library files.
        // Calling this method twice will have no effect.
        // By default the method extracts the shared library for loading at
        // java.io.tmpdir, however, you can override this temporary location by
        // setting the environment variable ROCKSDB_SHAREDLIB_DIR.
        // 默认这个方法会加压一个共享库到java.io.tmpdir
        RocksDB.loadLibrary();
    }

    public void writeRocksManyData(Long count) throws RocksDBException {
        // 1. 打开数据库
        // 1.1 创建数据库配置
        Options dbOpt = new Options();
        // 1.2 配置当数据库不存在时自动创建
        dbOpt.setCreateIfMissing(true);

        // 1.3 打开数据库。因为RocksDB默认是保存在本地磁盘，所以需要指定位置
        RocksDB rdb = RocksDB.open(dbOpt, "/Users/lcc/temp/rocksdb");

        System.out.println("打开数据库完成！");
        logger.info("准备写入数据量:{}",count);
        writeRocksData(rdb, count);
        logger.info("写入数据量完成:{}",count);

        for (int i = 10000; i < 10010; i++) {
            String key = "6511184208578554691"+"_"+i;
            byte[] bytes = rdb.get(String.valueOf(key).getBytes());
            System.out.println("key = " + key + " value = " + new String(bytes));
        }

        // 关闭资源
        rdb.close();
        dbOpt.close();
    }

    private void writeRocksData(RocksDB rdb, Long count) throws RocksDBException {
        String data = "{\"inner_reference\":\"AiNTA_Malware\",\"subCategory\":\"/Malware/BotTrojWorm\",\"sclass\":\"/C2/Trojan\",\"sendHostAddress\":\"172.24.187.115\",\"baas_sink_process_time\":1722843293097,\"srcPort\":50357,\"baas_internal_save_dynList\":false,\"techniquesId\":\"T1572\",\"destAddress\":\"60.191.244.2\",\"alarmSource\":\"安恒威胁情报\",\"ruleId\":\"99999999\",\"deviceCat\":\"/IDS/Network\",\"TIName\":\"ThreatIntelligenceCentreSource\",\"machineCode\":\"C15E5196459BD9D74DA8D60EDB6B03BA\",\"modelType\":\"signature\",\"srcAddress\":\"172.24.190.201\",\"productVendorName\":\"安恒\",\"transProtocol\":\"UDP\",\"catObject\":\"/Host/Application/Service\",\"deviceSendProductName\":\"AiNTA\",\"logSessionId\":\"698072927883271\",\"catOutcome\":\"OK\",\"killChain\":\"KC_Profit\",\"collectorReceiptTime\":\"2024-08-05 11:20:04\",\"appProtocol\":\"dns\",\"destSecurityZone\":\"outer\",\"destMacAddress\":\"C4-5E-5C-E4-10-6E\",\"eventIDs\":\"5091123961441550368\",\"attackerSecurityZone\":[\"outer\"],\"deviceAddress\":\"172.24.187.115\",\"destGeoCountry\":\"中国\",\"startTime\":\"2024-08-05 11:20:04\",\"baas_internal_save_event\":false,\"suggestion\":\"defaultTemplate\",\"familyId\":\"08b38ee7-2f97-11eb-9593-ac1f6b480078\",\"destGeoLatitude\":\"29.089524\",\"modelName\":\"AiNTA_Malware\",\"@timestamp\":\"2024-08-05T03:20:04.000Z\",\"baas_engineInfo\":\"info:78.118.1.93,7.datanode1\",\"catBehavior\":\"/Access\",\"IoCLevel\":\"High\",\"endTime\":\"2024-08-05 11:20:04\",\"srcGeoRegion\":\"局域网\",\"fromCustomStrategy\":false,\"IoCType\":\"domain\",\"AiLPHAPartID\":\"000001\",\"IoCHash\":\"09038f77aff92e3e\",\"txId\":\"0\",\"eventCount\":1,\"deviceReceiptTime\":\"2024-08-05 11:20:04\",\"deviceName\":\"安恒AiNTA\",\"mclass\":\"/C2\",\"inner_report\":\"aggLabled\",\"alarmDescription\":\"defaultTemplate\",\"baasAlarmUuid\":\"589a08b6ef8085211ebd61552e0636b43a1fa3ba12c43bce192b6cf8746f217e\",\"eventId\":\"6511184208578554691\",\"netId\":\"a4801294-c6d5-11ec-be31-02424e76011b_1651138143\",\"vlanId\":\"2004\",\"destGeoLongitude\":\"119.649506\",\"logname\":\"AsyncRAT家族远控木马活动事件\",\"dataType\":\"ids\",\"alarmName\":\"AsyncRAT家族远控木马活动事件\",\"windowId\":\"20210101000000-86400sec-1312\",\"srcGeoCountry\":\"局域网\",\"srcOrgId\":\"565f62d3-c19f-4676-84c7-74874cadc1e5\",\"baas_platform_id\":[\"a4801294-c6d5-11ec-be31-02424e76011b_1651138143\"],\"sourceEventIds\":\"5091123961441550368\",\"catTechnique\":\"/Exploit/Vulnerability\",\"name\":\"AsyncRAT家族远控木马活动事件\",\"destGeoRegion\":\"浙江\",\"tacticId\":\"TA0011\",\"IoC\":\"xred.mooo.com\",\"dataSource\":\"security_devicealarm\",\"logType\":\"alert\",\"gid\":8,\"deviceHostAssetId\":\"asset_709ce941-589e-4b47-881f-0c758f4044fe_1686244801189\",\"dataSubType\":\"attackAlert\",\"victimSecurityZone\":[\"inner_3423a2ff-f261-44f3-bf74-12f7727313fd_1651221037181\"],\"destPort\":53,\"chineseModelName\":\"恶意程序 AiNTA\",\"deviceProductType\":\"入侵检测系统\",\"ruleType\":\"/Malware/BotTrojWorm\",\"baas_internal_save_alarm\":true,\"ruleName\":\"AsyncRAT家族远控木马活动事件\",\"timeInterval\":\"86400\",\"victim\":[\"172.24.190.201\"],\"interfaceName\":\"eth10\",\"direction\":\"01\",\"destGeoCity\":\"金华\",\"severity\":7,\"aggCondition\":\"b431ea84e2b99e2b\",\"destOrgId\":\"outer\",\"confidence\":\"High\",\"attacker\":[\"xred.mooo.com\"],\"threatSeverity\":\"High\",\"srcSecurityZone\":\"inner_3423a2ff-f261-44f3-bf74-12f7727313fd_1651221037181\",\"deviceVersion\":\"v1.2.5_release_ctag_2.5.15_ruletag_1.1.1508\",\"message\":\"AsyncRAT家族远控木马活动事件。来源：172.24.190.201/50357，目的：60.191.244.2/53\",\"requestDomain\":\"xred.mooo.com\",\"catSignificance\":\"/Infomational/Alert\",\"threatName\":\"AsyncRAT家族远控木马活动事件\",\"alarmResults\":\"OK\",\"srcMacAddress\":\"28-FB-AE-50-7C-D5\",\"category\":\"/Malware\"}";

        for (int i = 0; i < count; i++) {
            JSONObject jsonObject = JSONObject.parseObject(data);
            String eventId = jsonObject.getString("eventId");
            String key = eventId + "_" + i;
            jsonObject.put("eventId", key);

            // 2.1 RocksDB都是以字节流的方式写入数据库中，所以我们需要将字符串转换为字节流再写入。这点类似于HBase
            byte[] keyByte = key.getBytes();
            byte[] value = data.getBytes();

            // 2.2 调用put方法写入数据
            rdb.put(keyByte, value);

            if(i % 100000 == 0){
                System.out.println("写入数据到RocksDB完成！count="+i);
            }
        }

    }

    public void writeRocksSameData(long count) throws RocksDBException {
        // 1. 打开数据库
        // 1.1 创建数据库配置
        Options dbOpt = new Options();
        // 1.2 配置当数据库不存在时自动创建
        dbOpt.setCreateIfMissing(true);

        // 1.3 打开数据库。因为RocksDB默认是保存在本地磁盘，所以需要指定位置
        RocksDB rdb = RocksDB.open(dbOpt, "/Users/lcc/temp/rocksdb");

        System.out.println("打开数据库完成！");
        logger.info("准备写入数据量:{}",count);
//        writeRocksSameData(rdb, count);
        logger.info("写入数据量完成:{}",count);

        for (int i = 10; i < 20; i++) {
            String key = "6511184208578554691"+"_"+i;
            byte[] bytes = rdb.get(String.valueOf(key).getBytes());
            if(bytes == null){
                continue;
            }
            System.out.println("key = " + key + " value = " + new String(bytes));
        }
        rdb.compactRange();

        // 关闭资源
        rdb.close();
        dbOpt.close();
    }

    private void writeRocksSameData(RocksDB rdb, long count) throws RocksDBException {
        String data = "{\"inner_reference\":\"AiNTA_Malware\",\"subCategory\":\"/Malware/BotTrojWorm\",\"sclass\":\"/C2/Trojan\",\"sendHostAddress\":\"172.24.187.115\",\"baas_sink_process_time\":1722843293097,\"srcPort\":50357,\"baas_internal_save_dynList\":false,\"techniquesId\":\"T1572\",\"destAddress\":\"60.191.244.2\",\"alarmSource\":\"安恒威胁情报\",\"ruleId\":\"99999999\",\"deviceCat\":\"/IDS/Network\",\"TIName\":\"ThreatIntelligenceCentreSource\",\"machineCode\":\"C15E5196459BD9D74DA8D60EDB6B03BA\",\"modelType\":\"signature\",\"srcAddress\":\"172.24.190.201\",\"productVendorName\":\"安恒\",\"transProtocol\":\"UDP\",\"catObject\":\"/Host/Application/Service\",\"deviceSendProductName\":\"AiNTA\",\"logSessionId\":\"698072927883271\",\"catOutcome\":\"OK\",\"killChain\":\"KC_Profit\",\"collectorReceiptTime\":\"2024-08-05 11:20:04\",\"appProtocol\":\"dns\",\"destSecurityZone\":\"outer\",\"destMacAddress\":\"C4-5E-5C-E4-10-6E\",\"eventIDs\":\"5091123961441550368\",\"attackerSecurityZone\":[\"outer\"],\"deviceAddress\":\"172.24.187.115\",\"destGeoCountry\":\"中国\",\"startTime\":\"2024-08-05 11:20:04\",\"baas_internal_save_event\":false,\"suggestion\":\"defaultTemplate\",\"familyId\":\"08b38ee7-2f97-11eb-9593-ac1f6b480078\",\"destGeoLatitude\":\"29.089524\",\"modelName\":\"AiNTA_Malware\",\"@timestamp\":\"2024-08-05T03:20:04.000Z\",\"baas_engineInfo\":\"info:78.118.1.93,7.datanode1\",\"catBehavior\":\"/Access\",\"IoCLevel\":\"High\",\"endTime\":\"2024-08-05 11:20:04\",\"srcGeoRegion\":\"局域网\",\"fromCustomStrategy\":false,\"IoCType\":\"domain\",\"AiLPHAPartID\":\"000001\",\"IoCHash\":\"09038f77aff92e3e\",\"txId\":\"0\",\"eventCount\":1,\"deviceReceiptTime\":\"2024-08-05 11:20:04\",\"deviceName\":\"安恒AiNTA\",\"mclass\":\"/C2\",\"inner_report\":\"aggLabled\",\"alarmDescription\":\"defaultTemplate\",\"baasAlarmUuid\":\"589a08b6ef8085211ebd61552e0636b43a1fa3ba12c43bce192b6cf8746f217e\",\"eventId\":\"6511184208578554691\",\"netId\":\"a4801294-c6d5-11ec-be31-02424e76011b_1651138143\",\"vlanId\":\"2004\",\"destGeoLongitude\":\"119.649506\",\"logname\":\"AsyncRAT家族远控木马活动事件\",\"dataType\":\"ids\",\"alarmName\":\"AsyncRAT家族远控木马活动事件\",\"windowId\":\"20210101000000-86400sec-1312\",\"srcGeoCountry\":\"局域网\",\"srcOrgId\":\"565f62d3-c19f-4676-84c7-74874cadc1e5\",\"baas_platform_id\":[\"a4801294-c6d5-11ec-be31-02424e76011b_1651138143\"],\"sourceEventIds\":\"5091123961441550368\",\"catTechnique\":\"/Exploit/Vulnerability\",\"name\":\"AsyncRAT家族远控木马活动事件\",\"destGeoRegion\":\"浙江\",\"tacticId\":\"TA0011\",\"IoC\":\"xred.mooo.com\",\"dataSource\":\"security_devicealarm\",\"logType\":\"alert\",\"gid\":8,\"deviceHostAssetId\":\"asset_709ce941-589e-4b47-881f-0c758f4044fe_1686244801189\",\"dataSubType\":\"attackAlert\",\"victimSecurityZone\":[\"inner_3423a2ff-f261-44f3-bf74-12f7727313fd_1651221037181\"],\"destPort\":53,\"chineseModelName\":\"恶意程序 AiNTA\",\"deviceProductType\":\"入侵检测系统\",\"ruleType\":\"/Malware/BotTrojWorm\",\"baas_internal_save_alarm\":true,\"ruleName\":\"AsyncRAT家族远控木马活动事件\",\"timeInterval\":\"86400\",\"victim\":[\"172.24.190.201\"],\"interfaceName\":\"eth10\",\"direction\":\"01\",\"destGeoCity\":\"金华\",\"severity\":7,\"aggCondition\":\"b431ea84e2b99e2b\",\"destOrgId\":\"outer\",\"confidence\":\"High\",\"attacker\":[\"xred.mooo.com\"],\"threatSeverity\":\"High\",\"srcSecurityZone\":\"inner_3423a2ff-f261-44f3-bf74-12f7727313fd_1651221037181\",\"deviceVersion\":\"v1.2.5_release_ctag_2.5.15_ruletag_1.1.1508\",\"message\":\"AsyncRAT家族远控木马活动事件。来源：172.24.190.201/50357，目的：60.191.244.2/53\",\"requestDomain\":\"xred.mooo.com\",\"catSignificance\":\"/Infomational/Alert\",\"threatName\":\"AsyncRAT家族远控木马活动事件\",\"alarmResults\":\"OK\",\"srcMacAddress\":\"28-FB-AE-50-7C-D5\",\"category\":\"/Malware\"}";

        for (int i = 0; i < count; i++) {
            JSONObject jsonObject = JSONObject.parseObject(data);
            String eventId = jsonObject.getString("eventId");

            byte[] keyByte = eventId.getBytes();
            byte[] value = data.getBytes();

            // 2.2 调用put方法写入数据
            rdb.put(keyByte, value);
        }
    }
}
