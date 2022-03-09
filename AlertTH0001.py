### os & sys
import os
import sys

### third party
import pytz
from datetime import datetime

### custom
from Config.Config import Config
from DBConnect.MysqlConnection import MysqlConnection
from DBConnect.MongoConnection import MongoConnection
from Models.WorkerSleep import WorkerSleep
from Models.RabbitMqModel import RabbitMqModel
from Models.DeviceTH0001Model import DeviceTH0001Model

class DeviceCheckAlertTH0001 :

    config = None
    mongo = None
    rabbitMqModel = None
    deviceModelTH0001 = None

    def __init__ (self) :
        self.config = Config()
        self.mongo = MongoConnection()
        self.rabbitMqModel = RabbitMqModel()
        self.deviceTH0001Model = DeviceTH0001Model()

    ## listen RabbitMQ
    def listener (self) :
        queueName = self.config.rabbitQueue["name"]["alertTH0001"]
        connection = self.rabbitMqModel.getConnect()
        channel = connection.channel()
        channel.queue_declare(queue = queueName, durable = self.config.rabbitQueue["durable"])
        channel.basic_qos(prefetch_count = 1)
        channel.basic_consume(queueName, self.startUp)
        channel.start_consuming()

    def startUp (self, ch, method, properties, body) :
        #update by vivian
        print('{nowDateTime} -- startUp-before_decode'.format(nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")))
        
        mysql = MysqlConnection()
        
        try :
            message = body.decode("utf-8")
            print("\n{nowDateTime} -- receive message".format(
                nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
            ))
        except :
            print("\n{nowDateTime} -- message decode error!!".format(
                nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
            ))

        result_checkAlert = self.checkAlert(mysql, message)
        result_checkDisconnect = self.checkDisconnect(mysql, message)
        mysql.close()
        WorkerSleep().checkAlertSleep()
        ch.basic_ack(delivery_tag = method.delivery_tag)

    # main thread
    def checkAlert (self, mysql, message) :
        #update by vivian
        print(message)

        if message == "" :
            print("\n{nowDateTime} -- No message".format(
                nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
            ))
            return False

        deviceData = self.mongo.getDeviceData(message)
        #update by vivian 0904
        print('recordAt:', deviceData["recordAt"])
        print('branchId:', deviceData["branchId"])
        print('companyId:', deviceData["companyId"])
        print('deviceId:', deviceData["deviceId"])
        print('temperature', deviceData["temperature"])

        miscellaneous = mysql.getMiscellaneous()

        batteryDefaultAlertValue = int(miscellaneous["deviceData"]["deviceDataBatteryDefaultAlertValue"]) \
            if "deviceData" in miscellaneous \
               and "deviceDataBatteryDefaultAlertValue" in miscellaneous["deviceData"] \
            else 30

        temperatureRoundLength = int(miscellaneous["deviceData"]["deviceDataTemperatureRoundLength"]) \
            if "deviceData" in miscellaneous \
               and "deviceDataTemperatureRoundLength" in miscellaneous["deviceData"] \
            else 2

        humidityRoundLength = int(miscellaneous["deviceData"]["deviceDataHumidityRoundLength"]) \
            if "deviceData" in miscellaneous \
               and "deviceDataHumidityRoundLength" in miscellaneous["deviceData"] \
            else 0

        batteryRoundLength = int(miscellaneous["deviceData"]["deviceDataBatteryRoundLength"]) \
            if "deviceData" in miscellaneous \
               and "deviceDataBatteryRoundLength" in miscellaneous["deviceData"] \
            else 0

        batteryTemperatureRoundLength = int(miscellaneous["deviceData"]["deviceDataBatteryTemperatureRoundLength"]) \
            if "deviceData" in miscellaneous \
               and "deviceDataBatteryTemperatureRoundLength" in miscellaneous["deviceData"] \
            else 2

        if deviceData == None :
            print("\n{nowDateTime} -- Not find deviceData, message : {message}".format(
                nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S"),
                message = message
            ))
            return False

        deviceGroupList = self.getDeviceGroupList(mysql, deviceData["deviceId"])
        # print(deviceGroupList)

        # reset new list with device group
        tempDeviceGroupId = ""
        tempDeviceGroupList = []
        tempDeviceGroupListIndex = -1
        for value in deviceGroupList :
            if tempDeviceGroupId != value["groupId"] :
                tempDeviceGroupId = value["groupId"]
                tempDeviceGroupListIndex = tempDeviceGroupListIndex + 1
                tempDeviceGroupList.append({
                    "id" : value["groupId"],
                    "companyId" : value["companyId"],
                    "companyName" : value["companyName"],
                    "branchId" : value["branchId"],
                    "branchName" : value["branchName"],
                    "alertId" : value["alertId"],
                    "temperatureUnit" : value["temperatureUnit"],
                    "value" : []
                })
            value.pop('groupId', None)
            value.pop('companyId', None)
            value.pop('branchId', None)
            value.pop('companyName', None)
            value.pop('branchName', None)
            value.pop('alertId', None)
            value.pop('temperatureUnit', None)
            tempDeviceGroupList[tempDeviceGroupListIndex]["value"].append(value)

        # check value with setting
        deviceGroupList = None

        temperatureUpper = self.config.alertType["temperature"]["upper"]
        temperauterLower = self.config.alertType["temperature"]["lower"]
        humidityUpper = self.config.alertType["humidity"]["upper"]
        humidityLower = self.config.alertType["humidity"]["lower"]
        batteryLower = self.config.alertType["battery"]["lower"]

        queueName = self.config.rabbitQueue["name"]["deviceNoticeTH0001"]

        for deviceGroup in tempDeviceGroupList :

            deviceAlertLogDict = self.deviceTH0001Model.getDeviceAlertLogDict()
            deviceAlertLogDict["deviceDataId"] = str(deviceData["_id"])
            deviceAlertLogDict["sigfoxCode"] = deviceData["sigfoxCode"]
            deviceAlertLogDict["deviceId"] = deviceData["deviceId"]
            deviceAlertLogDict["deviceGroupId"] = deviceGroup["id"]
            deviceAlertLogDict["companyId"] = deviceGroup["companyId"]
            deviceAlertLogDict["companyName"] = deviceGroup["companyName"]
            deviceAlertLogDict["branchId"] = deviceGroup["branchId"]
            deviceAlertLogDict["branchName"] = deviceGroup["branchName"]
            deviceAlertLogDict["temperatureUnit"] = deviceGroup["temperatureUnit"]
            deviceAlertLogDict["temperature"] = round(float(deviceData["temperature"]), temperatureRoundLength) if deviceData["temperature"] != None else None
            deviceAlertLogDict["humidity"] = round(float(deviceData["humidity"]), humidityRoundLength) if deviceData["humidity"] != None else None
            deviceAlertLogDict["battery"] = round(float(deviceData["battery"]), batteryRoundLength) if deviceData["battery"] != None else None
            deviceAlertLogDict["batteryTemperature"] = round(float(deviceData["batteryTemperature"]), batteryTemperatureRoundLength) if deviceData["batteryTemperature"] != None else None
            deviceAlertLogDict["compensateTemperature"] = round(float(deviceData["compensateTemperature"]), temperatureRoundLength)
            deviceAlertLogDict["compensateHumidity"] = round(float(deviceData["compensateHumidity"]), humidityRoundLength)
            deviceAlertLogDict["compensateBattery"] = round(float(deviceData["compensateBattery"]), batteryRoundLength)
            deviceAlertLogDict["compensateBatteryTemperature"] = round(float(deviceData["compensateBatteryTemperature"]), batteryTemperatureRoundLength)

            # print(deviceGroup["value"])
            haveBatterySetting = False

            for row in deviceGroup["value"] :

                if row["dataType"] == "battery" and deviceAlertLogDict["battery"] != None :
                    haveBatterySetting = True

                if row["isOn"] != 1 :
                    continue

                # check setting
                # row["dataValue"] = alert setting value
                if row["dataType"] == "temperature" and deviceAlertLogDict["temperature"] != None :
                    lastTemperature = round(float(deviceAlertLogDict["temperature"] + deviceAlertLogDict["compensateTemperature"]), temperatureRoundLength)
                    if row["limitType"] == "upper" and lastTemperature > row["dataValue"] :
                        deviceAlertLogDict["temperatureSetting"] = row["dataValue"]
                        deviceAlertLogDict["alertType"].append(temperatureUpper)
                    elif row["limitType"] == "lower" and lastTemperature < row["dataValue"] :
                        deviceAlertLogDict["temperatureSetting"] = row["dataValue"]
                        deviceAlertLogDict["alertType"].append(temperauterLower)
                elif row["dataType"] == "humidity" and deviceAlertLogDict["humidity"] != None :
                    lastHumidity = deviceAlertLogDict["humidity"] + deviceAlertLogDict["compensateHumidity"]
                    if row["limitType"] == "upper" and lastHumidity > row["dataValue"] :
                        deviceAlertLogDict["humiditySetting"] = row["dataValue"]
                        deviceAlertLogDict["alertType"].append(humidityUpper)
                    elif row["limitType"] == "lower" and lastHumidity < row["dataValue"] :
                        deviceAlertLogDict["humiditySetting"] = row["dataValue"]
                        deviceAlertLogDict["alertType"].append(humidityLower)
                elif row["dataType"] == "battery" and deviceAlertLogDict["battery"] != None :
                    lastBattery = deviceAlertLogDict["battery"] + deviceAlertLogDict["compensateBattery"]
                    if lastBattery < row["dataValue"] :
                        deviceAlertLogDict["batterySetting"] = row["dataValue"]
                        deviceAlertLogDict["alertType"].append(batteryLower)
                else :
                    continue

            # check default battery alert
            if haveBatterySetting != True and deviceAlertLogDict["battery"] != None :
                if deviceAlertLogDict["battery"] + deviceAlertLogDict["compensateBattery"] < batteryDefaultAlertValue :
                    deviceAlertLogDict["batterySetting"] = batteryDefaultAlertValue
                    deviceAlertLogDict["alertType"].append(batteryLower)

            
            # print(deviceAlertLogDict)
            if len(deviceAlertLogDict["alertType"]) > 0 :

                deviceAlertLogId = str(self.mongo.insertDeviceAlertLog(deviceAlertLogDict))

                deviceAlertId = self.insertOrUpdateDeviceAlert(mysql, deviceGroup["alertId"], deviceAlertLogDict)   #存入mongo後就會直接記當時的timestamp

                # send checkAlert use RabbitMQ
                connection = self.rabbitMqModel.getConnect()
                connection = self.rabbitMqModel.channelBasicPublish(connection, queueName, deviceAlertLogId)
                connection.close()
                print("\n{nowDateTime} -- deviceAlertLogId : {deviceAlertLogId}".format(
                    nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    deviceAlertLogId = deviceAlertLogId
                ))
        return True

    def checkDisconnect (self, mysql, message) :

        if message == "" :
            print("\n{nowDateTime} -- Disconnect No message".format(
                nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
            ))
            return False

        disconnectText = self.config.alertType["disconnect"]["alertText"]
        miscellaneous = mysql.getMiscellaneous()

        sleepTime = int(miscellaneous["scheduling"]["schedulingSleepTimeByDeviceDisconnect"]) \
            if "scheduling" in miscellaneous \
                and "schedulingSleepTimeByDeviceDisconnect" in miscellaneous["scheduling"] \
            else 600

        deviceDisconnectList = self.getDeviceDisconnectList(mysql)
        print(deviceDisconnectList)

        queueName = self.config.rabbitQueue["name"]["deviceNoticeTH0001"]

        tempDeviceId = ""
        for value in deviceDisconnectList :

            if value["recordedAt"] == None :
                continue

            if value["deviceId"] != tempDeviceId :

                tempDeviceId = value["deviceId"]
                deviceAlertLogDict= self.deviceTH0001Model.getDeviceAlertLogDict()
                deviceAlertLogDict["sigfoxCode"] = value["sigfoxCode"]
                deviceAlertLogDict["companyId"] = value["companyId"]
                deviceAlertLogDict["branchId"] = value["branchId"]
                deviceAlertLogDict["deviceId"] = value["deviceId"]
                deviceAlertLogDict["deviceGroupId"] = ""
                deviceAlertLogDict["alertType"].append(disconnectText)
                deviceAlertLogDict["recordedAt"] = datetime.fromtimestamp(int(value["recordedAt"]), pytz.utc)
                deviceAlertLogId = str(self.mongo.insertDeviceAlertLog(deviceAlertLogDict))
                deviceAlertId = self.insertOrUpdateDeviceAlert_disconnect(mysql, deviceAlertLogDict)

                print("\n{nowDateTime} -- Disconnect Pass : {deviceAlertLogDict}".format(
                    nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    deviceAlertLogDict = deviceAlertLogDict
                ))

                # send checkDisconnect use RabbitMQ
                connection = self.rabbitMqModel.getConnect()
                connection = self.rabbitMqModel.channelBasicPublish(connection, queueName, deviceAlertLogId)
                connection.close()
                print("\n{nowDateTime} -- Disconnect deviceAlertLogId : {deviceAlertLogId}".format(
                    nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    deviceAlertLogId = deviceAlertLogId
                ))

        return True

    # get device group to check alert status
    def getDeviceGroupList (self, mysql, deviceId) :
        sql = """SELECT `dg`.`id` AS `groupId`,
                `dgas`.`dataType` AS `dataType`,
                `dgas`.`limitType` AS `limitType`,
                `dgas`.`dataValue` AS `dataValue`,
                `dgas`.`isOn` AS `isOn`,
                `dg`.`branchId` AS `branchId`,
                IFNULL((SELECT `name` FROM `branches` WHERE `id` = `dg`.`branchId` LIMIT 1), '') AS `branchName`,
                IFNULL((SELECT `companyId` FROM `branches` WHERE `id` = `dg`.`branchId` LIMIT 1), '') AS `companyId`,
                IFNULL((SELECT `name` FROM `companies` WHERE `id` = IFNULL((
                    SELECT `companyId` FROM `branches` WHERE `id` = `dg`.`branchId` LIMIT 1
                ), '') LIMIT 1), '') AS `companyName`,
                IFNULL((SELECT `temperatureUnit` FROM `companies` WHERE `id` = IFNULL((
                    SELECT `companyId` FROM `branches` WHERE `id` = `dg`.`branchId` LIMIT 1
                ), '') LIMIT 1), 'C') AS `temperatureUnit`,
                (SELECT `id` FROM `device_alerts` WHERE `deviceId` = `dgd`.`deviceId` AND `deviceGroupId` = `dg`.`id` LIMIT 1) AS `alertId`
            FROM `devices` AS `d`
                INNER JOIN `device_groups_devices` AS `dgd` ON `d`.`id` = `dgd`.`deviceId`
                INNER JOIN `device_groups` AS `dg` ON `dg`.`id` = `dgd`.`deviceGroupId`
                INNER JOIN `device_group_alert_settings` AS `dgas` ON `dg`.`id` = `dgas`.`deviceGroupId`
            WHERE `dgd`.`deviceId` = '{deviceId}'
                AND `d`.`returnAt` IS NULL
                AND `d`.`deleted_at` IS NULL
                AND `d`.`branchId` != ''
                AND `d`.`activateExpiredAt` >= '{nowDateTime}'
            ORDER BY `dg`.`id`;""".format(
            deviceId = deviceId,
            nowDateTime = mysql.getNowDateTimeUTC()
        )
        return mysql.queryData(sql)

    # update alert status
    def insertOrUpdateDeviceAlert (self, mysql, alertId, deviceAlertLogDict) :
        alertType = ""
        nowDateTime = mysql.getNowDateTimeUTC()

        temperatureUpper = self.config.alertType["temperature"]["upper"]
        temperauterLower = self.config.alertType["temperature"]["lower"]
        humidityUpper = self.config.alertType["humidity"]["upper"]
        humidityLower = self.config.alertType["humidity"]["lower"]
        batteryLower = self.config.alertType["battery"]["lower"]

        temperatureText = self.config.alertType["temperature"]["alertText"]
        humidityText = self.config.alertType["humidity"]["alertText"]
        batteryText = self.config.alertType["battery"]["alertText"]
        multiText = self.config.alertType["multi"]["alertText"]

        if len(deviceAlertLogDict["alertType"]) == 1 :
            tempAlertType = deviceAlertLogDict["alertType"][0]
            if tempAlertType == temperatureUpper or tempAlertType == temperauterLower :
                alertType = temperatureText
            elif tempAlertType == humidityUpper or tempAlertType == humidityLower :
                alertType = humidityText
            elif tempAlertType == batteryLower :
                alertType = batteryText
        elif len(deviceAlertLogDict["alertType"]) > 1:
            haveTemperature = 0
            haveHumidity = 0
            haveBattery = 0
            for value in deviceAlertLogDict["alertType"] :
                if value == temperatureUpper or value == temperauterLower :
                    haveTemperature = 1
                elif value == humidityUpper or value == humidityLower :
                    haveHumidity = 1
                elif value == batteryLower :
                    haveBattery = 1
            if (haveTemperature + haveHumidity + haveBattery) > 1 :
                alertType = multiText
            else :
                if haveTemperature > 0 :
                    alertType = temperatureText
                elif haveHumidity > 0 :
                    alertType = humidityText
                elif haveBattery > 0 :
                    alertType = batteryText

        if alertId == None :
            alertId = mysql.createPrimaryKey()
            sql = """INSERT INTO `device_alerts` (
                    `id`, `deviceId`, `deviceGroupId`, `alertType`, `alertAt`, `created_at`, `updated_at`
                ) VALUES(
                    '{id}', '{deviceId}', '{deviceGroupId}', '{alertType}', '{alertAt}', '{created_at}', '{updated_at}'
                );""".format(
                id = alertId,
                deviceId = deviceAlertLogDict["deviceId"],
                deviceGroupId = deviceAlertLogDict["deviceGroupId"],
                alertType = alertType,
                alertAt = nowDateTime,
                created_at = nowDateTime,
                updated_at = nowDateTime
            )
            ret = mysql.insertData(sql)
        else :
            sql = """UPDATE `device_alerts` SET
                `deviceId` = '{deviceId}',
                `deviceGroupId` = '{deviceGroupId}',
                `alertType` = '{alertType}',
                `alertAt` = '{alertAt}',
                `updated_at` = '{updated_at}'
                WHERE `id` = '{id}'""".format(
                id = alertId,
                deviceId = deviceAlertLogDict["deviceId"],
                deviceGroupId = deviceAlertLogDict["deviceGroupId"],
                alertType = alertType,
                alertAt = nowDateTime,
                updated_at = nowDateTime
            )
            ret = mysql.updateData(sql)
        return alertId
    # update by vivian-add the deviceDisconnectList sql
    def getDeviceDisconnectList (self, mysql) :
        sql = """SELECT `d`.`id` AS `deviceId`,
                `d`.`companyId` AS `companyId`,
                `d`.`branchId` AS `branchId`,
                `d`.`sigfoxCode` AS `sigfoxCode`,
                `dls`.`id` AS `lastStatusId`,
                IFNULL((SELECT `typeNO` FROM `device_types` WHERE `id` = `d`.`deviceTypeId` LIMIT 1), '') AS `typeNO`,
                UNIX_TIMESTAMP(STR_TO_DATE(`dls`.`recordedAt`, '%Y-%m-%d %H:%i:%s')) AS `recordedAt`
            FROM `devices` AS `d`
                INNER JOIN `device_last_status` AS `dls` ON `d`.`id` = `dls`.`deviceId`
            WHERE `d`.`returnAt` IS NULL
                AND `d`.`deleted_at` IS NULL
                AND `d`.`activatedAt` IS NOT NULL
                AND `d`.`branchId` != ''
                AND `d`.`id` IN (SELECT `deviceId` FROM `device_groups_devices` WHERE `deviceGroupId` IN (
                    SELECT `id` FROM `device_groups` WHERE `deleted_at` IS NULL AND `branchId` = `d`.`branchId`
                ))
                AND `dls`.`recordedAt` IS NOT NULL
                AND `d`.`activateExpiredAt` >= '{nowDateTime}'
                AND DATE_ADD(`dls`.`recordedAt`, INTERVAL IFNULL(
                    (SELECT `disconnectSecond` FROM `device_types` WHERE `id` = `d`.`deviceTypeId` LIMIT 1)
                , '1500') SECOND) <= '{nowDateTime}'
                AND `dls`.`recordedAt` >=  DATE_SUB('{nowDateTime}', INTERVAL '1559' SECOND);""".format(
            nowDateTime = mysql.getNowDateTimeUTC()
        )

        print(sql)
        result = mysql.queryData(sql)
        # print(result)

        return result
    
    def insertOrUpdateDeviceAlert_disconnect (self, mysql, deviceAlertLogDict) :
        alertType = self.config.alertType["disconnect"]["alertText"]
        alertId = mysql.createPrimaryKey()
        nowDate = mysql.getNowDateTimeUTC()

        sql = """SELECT `id` FROM `device_alerts`
            WHERE `alertType` = '{alertType}' AND `deviceId` = '{deviceId}' AND `deviceGroupId` = '{deviceGroupId}'""".format(
                alertType = alertType,
                deviceId = deviceAlertLogDict["deviceId"],
                deviceGroupId = deviceAlertLogDict["deviceGroupId"]
            )
        deviceAlert = mysql.getData(sql)
        if deviceAlert == None :
            sql = """INSERT INTO `device_alerts` (
                    `id`, `deviceId`, `deviceGroupId`, `alertType`, `alertAt`, `created_at`, `updated_at`
                ) VALUES(
                    '{id}', '{deviceId}', '{deviceGroupId}', '{alertType}', '{alertAt}', '{created_at}', '{updated_at}'
                );""".format(
                id = alertId,
                deviceId = deviceAlertLogDict["deviceId"],
                deviceGroupId = deviceAlertLogDict["deviceGroupId"],
                alertType = alertType,
                alertAt = nowDate,
                created_at = nowDate,
                updated_at = nowDate
            )
            ret = mysql.insertData(sql)
        else :
            alertId = deviceAlert["id"]
            sql = """UPDATE `device_alerts`
                SET `alertType` = '{alertType}', `alertAt` = '{alertAt}', `updated_at` = '{updated_at}'
                WHERE `id` = '{id}';""".format(
                id = deviceAlert["id"],
                alertType = alertType,
                alertAt = nowDate,
                updated_at = nowDate
            )
            ret = mysql.updateData(sql)
        return alertId

######################################## start ########################################

# if True :
try :
    helpIsShow = False
    helpText = """
    -h, --help : Display this help
    """

    if len(sys.argv) > 1 :
        if sys.argv[1].startswith("--help") or sys.argv[1].startswith("-h") :
            helpIsShow = True

    if helpIsShow :
        print(helpText)
    else :
        app = DeviceCheckAlertTH0001()
        app.listener()

except KeyboardInterrupt :
    print("\n{nowDateTime} -- User close scheduling".format(
        nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    ))
    sys.exit()
except :
    print("\n{nowDateTime} -- Error => Main thread error:{sysExcInfo}".format(
        nowDateTime = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S"),
        sysExcInfo = sys.exc_info()[0]
    ))
    sys.exit()
