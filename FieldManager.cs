using MD.CloudConnect.CouchBaseProvider;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudConnect.BackgroundWorker
{
    public class FieldManager
    {
        Dictionary<string, FieldDefinition> _cache = new Dictionary<string, FieldDefinition>();
        private DateTime _lastCacheUpdated = DateTime.MinValue;

        #region singleton
        protected static readonly FieldManager _instance = new FieldManager();
        public static FieldManager Instance
        {
            get
            {
                lock (_instance)
                {
                    return _instance;
                }
            }
        }

        static FieldManager()
        {

        }
        #endregion

        public void Initialize()
        {
            LoadFieldDefinition();
        }

        public bool MustBeRebuild(string key)
        {
            if ((DateTime.UtcNow.Ticks - _lastCacheUpdated.Ticks) > (TimeSpan.TicksPerMinute * 15))
                LoadFieldDefinition();
            if (_cache.ContainsKey(key))
                return !_cache[key].IgnoreInHistory;
            else return false;
        }

        public void BackupField()
        {
            LoadFieldDefinition();
            string json = JsonConvert.SerializeObject(_cache.Values.ToList());
            StreamWriter sw = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory + "/field_definition.json");
            sw.Write(json);
            sw.Close();
        }

        private void GenerateDefaultField()
        {
            //_cache = new Dictionary<string, FieldDefinition>()
            //{
            //    { "GPRMC_VALID",  new FieldDefinition() { Key = "GPRMC_VALID", FieldType = FieldType.String, DisplayName = "Valid", SortId = 1 }},
            //    { "GPS_SPEED",  new FieldDefinition() { Key = "GPS_SPEED", FieldType = FieldType.Speed , DisplayName = "Gps Speed (Km/h)", SortId = 2}},
            //    { "GPS_DIR",  new  FieldDefinition() { Key = "GPS_DIR",  FieldType = FieldType.Int100, DisplayName = "Dir", SortId = 3 }},
            //    { "DIO_IGNITION",  new  FieldDefinition() { Key = "DIO_IGNITION",  FieldType = FieldType.Boolean, DisplayName="Ignition", SortId = 4 }},
            //    { "BATT",new FieldDefinition() { Key = "BATT", FieldType = FieldType.Integer, SortId = 5 }},
            //    { "GPRS_HEADER", new FieldDefinition() { Key = "GPRS_HEADER", FieldType = FieldType.Integer , SortId = 6 }},
            //    { "RSSI", new FieldDefinition() { Key = "RSSI", FieldType = FieldType.Integer, SortId = 7 }},
            //    { "TACHOGRAPH_FIRST_DRIVER_STATE", new FieldDefinition() { Key = "TACHOGRAPH_FIRST_DRIVER_STATE", FieldType = FieldType.String , SortId =8 }},
            //    { "TACHOGRAPH_FIRST_DRIVER_DRIVING_STATE", new FieldDefinition() { Key = "TACHOGRAPH_FIRST_DRIVER_DRIVING_STATE",  FieldType = FieldType.String , SortId = 9}},
            //    { "TACHOGRAPH_DAILYMETER", new FieldDefinition() { Key = "TACHOGRAPH_DAILYMETER",  FieldType = FieldType.Integer , SortId = 10 }},
            //    { "TACHOGRAPH_ODOMETER", new FieldDefinition() { Key = "TACHOGRAPH_ODOMETER",  FieldType = FieldType.Integer, SortId = 11  }},            
            //    { "ODO_FULL",new FieldDefinition() { Key = "ODO_FULL",  FieldType = FieldType.Integer ,DisplayName = "Odo. Full", SortId = 12 }},
            //    { "TACHOGRAPH_DRIVING_TIME", new FieldDefinition() { Key = "TACHOGRAPH_DRIVING_TIME",  FieldType = FieldType.Integer , SortId = 13 }},
            //    { "DIO_ALARM", new FieldDefinition() { Key = "DIO_ALARM", FieldType = FieldType.Boolean , SortId = 14 }},
            //    { "DRIVER_ID",new FieldDefinition() { Key = "DRIVER_ID", FieldType = FieldType.String, SortId = 15 }},
            //    { "TEMP_1",new FieldDefinition() { Key = "TEMP_1", FieldType = FieldType.Integer, SortId = 16 }},
            //    { "TEMP_2",new FieldDefinition() { Key = "TEMP_2", FieldType = FieldType.Integer, SortId = 17 }},
            //    { "TEMP_3",new FieldDefinition() { Key = "TEMP_3", FieldType = FieldType.Integer, SortId = 18 }},
            //    { "TEMP_4",new FieldDefinition() { Key = "TEMP_4", FieldType = FieldType.Integer, SortId = 19 }},
            //    { "TEMP_5",new FieldDefinition() { Key = "TEMP_5", FieldType = FieldType.Integer, SortId = 20 }},
            //    { "TEMP_6",new FieldDefinition() { Key = "TEMP_6",  FieldType = FieldType.Integer, SortId = 21 }},
            //    { "TEMP_7",new FieldDefinition() { Key = "TEMP_7", FieldType = FieldType.Integer, SortId = 22 }},
            //    { "TEMP_8",new FieldDefinition() { Key = "TEMP_8",  FieldType = FieldType.Integer, SortId = 23 }},
            //    { "DIO_IN_TOR", new FieldDefinition() { Key = "DIO_IN_TOR", FieldType = FieldType.Integer , SortId = 24 }},
            //    { "GPS_HDOP", new FieldDefinition() { Key = "GPS_HDOP",  FieldType = FieldType.Integer, SortId = 25  }},
            //    { "GPS_VDOP", new FieldDefinition() { Key = "GPS_VDOP",  FieldType = FieldType.Integer , SortId = 26 }},
            //    { "GPS_PDOP", new FieldDefinition() { Key = "GPS_PDOP",  FieldType = FieldType.Integer , SortId = 27 }},
            //    { "BATT_TEMP", new FieldDefinition() { Key = "BATT_TEMP",  FieldType = FieldType.Int1000 , SortId = 28 }},
            //    { "CASE_TEMP", new FieldDefinition() { Key = "CASE_TEMP",  FieldType = FieldType.Int1000 , SortId = 29 }},
            //    { "OBD_CONNECTED_PROTOCOL", new FieldDefinition() { Key = "OBD_CONNECTED_PROTOCOL", FieldType = FieldType.Integer, SortId = 30  }},
            //    { "MDI_LAST_VALID_GPS_LATITUDE", new FieldDefinition() { Key = "MDI_LAST_VALID_GPS_LATITUDE", FieldType = FieldType.Integer , SortId = 31 }},
            //    { "MDI_LAST_VALID_GPS_LONGITUDE", new FieldDefinition() { Key = "MDI_LAST_VALID_GPS_LONGITUDE",  FieldType = FieldType.Integer, SortId = 32  }},
            //    { "BATT_VOLT", new FieldDefinition() { Key = "BATT_VOLT",  FieldType = FieldType.Integer, DisplayName = "Batt. volt(mV)" , SortId = 33 }},
            //    { "MDI_AREA_LIST", new FieldDefinition() { Key = "MDI_AREA_LIST",  FieldType = FieldType.String, DisplayName = "Area List" , SortId = 34 }},
            //    { "GPS_FIXED_SAT_NUM", new FieldDefinition() { Key = "GPS_FIXED_SAT_NUM",  FieldType = FieldType.Integer, SortId = 35  }},
            //    { "MVT_STATE", new FieldDefinition() { Key = "MVT_STATE", FieldType = FieldType.Boolean, SortId = 36  }},
            //    { "BEHAVE_ID", new FieldDefinition() { Key = "BEHAVE_ID",  FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "Behv. ID", SortId = 37  }},
            //    { "BEHAVE_LONG", new FieldDefinition() { Key = "BEHAVE_LONG",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Long.", SortId = 38 }},
            //    { "BEHAVE_LAT", new FieldDefinition() { Key = "BEHAVE_LAT",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Lat.", SortId = 39 }},
            //    { "BEHAVE_DAY_OF_YEAR", new FieldDefinition() { Key = "BEHAVE_DAY_OF_YEAR", FieldType = FieldType.DateDbehav , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Date", SortId = 40 }},
            //    { "BEHAVE_TIME_OF_DAY", new FieldDefinition() { Key = "BEHAVE_TIME_OF_DAY",  FieldType = FieldType.TimeDbehav , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Time", SortId = 41 }},
            //    { "BEHAVE_GPS_SPEED_BEGIN", new FieldDefinition() { Key = "BEHAVE_GPS_SPEED_BEGIN",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Speed Begin", SortId =42  }},
            //    { "BEHAVE_GPS_SPEED_PEAK", new FieldDefinition() { Key = "BEHAVE_GPS_SPEED_PEAK", FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Speed Peak", SortId = 43  }},
            //    { "BEHAVE_GPS_SPEED_END", new FieldDefinition() { Key = "BEHAVE_GPS_SPEED_END",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Speed End", SortId = 44 }},
            //    { "BEHAVE_GPS_HEADING_BEGIN", new FieldDefinition() { Key = "BEHAVE_GPS_HEADING_BEGIN", FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Heading Begin", SortId = 45 }},
            //    { "BEHAVE_GPS_HEADING_PEAK", new FieldDefinition() { Key = "BEHAVE_GPS_HEADING_PEAK",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Heading Peak", SortId = 46 }},
            //    { "BEHAVE_GPS_HEADING_END", new FieldDefinition() { Key = "BEHAVE_GPS_HEADING_END",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Heading End", SortId = 47 }},
            //    { "BEHAVE_ACC_X_BEGIN", new FieldDefinition() { Key = "BEHAVE_ACC_X_BEGIN", FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. AccX Begin" , SortId = 48 }},
            //    { "BEHAVE_ACC_X_PEAK", new FieldDefinition() { Key = "BEHAVE_ACC_X_PEAK", FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. AccX Peak" , SortId =49 }},
            //    { "BEHAVE_ACC_X_END", new FieldDefinition() { Key = "BEHAVE_ACC_X_END",  FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID" , DisplayName = "B. AccX End", SortId = 50 }},
            //    { "BEHAVE_ACC_Y_BEGIN", new FieldDefinition() { Key = "BEHAVE_ACC_Y_BEGIN",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. AccY Begin", SortId = 51 }},
            //    { "BEHAVE_ACC_Y_PEAK", new FieldDefinition() { Key = "BEHAVE_ACC_Y_PEAK",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. AccY Peak", SortId = 52 }},
            //    { "BEHAVE_ACC_Y_END", new FieldDefinition() { Key = "BEHAVE_ACC_Y_END",  FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID" , DisplayName = "B. AccY End", SortId = 53 }},
            //    { "BEHAVE_ACC_Z_BEGIN", new FieldDefinition() { Key = "BEHAVE_ACC_Z_BEGIN", FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID" , DisplayName = "B. AccZ Begin", SortId = 54 }},
            //    { "BEHAVE_ACC_Z_PEAK", new FieldDefinition() { Key = "BEHAVE_ACC_Z_PEAK",  FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. AccZ Peak", SortId =55  }},
            //    { "BEHAVE_ACC_Z_END", new FieldDefinition() { Key = "BEHAVE_ACC_Z_END",  FieldType = FieldType.Integer, FieldDependency ="BEHAVE_UNIQUE_ID" , DisplayName = "B. AccZ Ends", SortId = 56 }},
            //    { "BEHAVE_ELAPSED", new FieldDefinition() { Key = "BEHAVE_ELAPSED",  FieldType = FieldType.Integer , FieldDependency ="BEHAVE_UNIQUE_ID", DisplayName = "B. Elapsed", SortId = 57 }},
            //    { "BEHAVE_UNIQUE_ID", new FieldDefinition() { Key = "BEHAVE_UNIQUE_ID",  FieldType = FieldType.Integer, DisplayName = "B. UniqueID", FieldDependency ="BEHAVE_UNIQUE_ID" , SortId = 58 }},
            //    { "MDI_CRASH_DETECTED", new FieldDefinition() { Key = "MDI_CRASH_DETECTED", FieldType = FieldType.String , DisplayName = "Crash Detected", IgnoreInHistory = true, SortId = 59 }},
            //    { "EVENT", new FieldDefinition() { Key = "EVENT",  FieldType = FieldType.String , DisplayName = "Event", IgnoreInHistory = true, SortId = 60 }},
            //    { "MDI_EXT_BATT_LOW", new FieldDefinition() { Key = "MDI_EXT_BATT_LOW",  FieldType = FieldType.Boolean , DisplayName = "Ext. Batt. Low", SortId = 61 }},
            //    { "MDI_EXT_BATT_VOLTAGE", new FieldDefinition() { Key = "MDI_EXT_BATT_VOLTAGE",  FieldType = FieldType.Int1000 , DisplayName = "Ext. Batt. Voltage", SortId = 62 }},
            //    { "MDI_DTC_MIL", new FieldDefinition() { Key = "MDI_DTC_MIL",  FieldType = FieldType.Boolean , DisplayName = "Malfunction Indicator Lamp (MIL)", SortId = 63 }},
            //    { "MDI_DTC_NUMBER", new FieldDefinition() { Key = "MDI_DTC_NUMBER",  FieldType = FieldType.Integer , DisplayName = "Number of DTC", SortId = 64 }},
            //    { "MDI_DTC_LIST", new FieldDefinition() { Key = "MDI_DTC_LIST",  FieldType = FieldType.String , DisplayName = "List of DTC(s)", SortId =65 }},
            //    { "MDI_RPM_MAX", new FieldDefinition() { Key = "MDI_RPM_MAX", FieldType = FieldType.Integer , DisplayName = "Max. Rpm", SortId = 66 }},
            //    { "MDI_RPM_MIN", new FieldDefinition() { Key = "MDI_RPM_MIN",  FieldType = FieldType.Integer , DisplayName = "Min. Rpm", SortId = 67 }},
            //    { "MDI_RPM_AVERAGE", new FieldDefinition() { Key = "MDI_RPM_AVERAGE",FieldType = FieldType.Integer , DisplayName = "Avg. Rpm", SortId = 68 }},
            //    { "MDI_RPM_OVER", new FieldDefinition() { Key = "MDI_RPM_OVER",  FieldType = FieldType.Boolean , DisplayName = "Over Rpm", SortId = 69 }},
            //    { "MDI_RPM_AVERAGE_RANGE_1", new FieldDefinition() { Key = "MDI_RPM_AVERAGE_RANGE_1", FieldType = FieldType.Integer , DisplayName = "Rpm Average Range 1", SortId =70 }},
            //    { "MDI_RPM_AVERAGE_RANGE_2", new FieldDefinition() { Key = "MDI_RPM_AVERAGE_RANGE_2", FieldType = FieldType.Integer , DisplayName = "Rpm Average Range 2", SortId = 71 }},
            //    { "MDI_RPM_AVERAGE_RANGE_3", new FieldDefinition() { Key = "MDI_RPM_AVERAGE_RANGE_3", FieldType = FieldType.Integer , DisplayName = "Rpm Average Range 3", SortId = 72 }},
            //    { "MDI_RPM_AVERAGE_RANGE_4", new FieldDefinition() { Key = "MDI_RPM_AVERAGE_RANGE_4", FieldType = FieldType.Integer , DisplayName = "Rpm Average Range 4", SortId = 73 }},
            //    { "MDI_SENSORS_RECORDER_DATA", new FieldDefinition() { Key = "MDI_SENSORS_RECORDER_DATA", FieldType = FieldType.Sensor4hz , DisplayName = "Sensors recorder data", SortId =74 }},
            //    { "MDI_SENSORS_RECORDER_CALIBRATION", new FieldDefinition() { Key = "MDI_SENSORS_RECORDER_CALIBRATION",  FieldType = FieldType.Sensor4hz , DisplayName = "Sensors recorder calibration", SortId =75 }},            
            //    { "MDI_GPS_ANTENNA", new FieldDefinition() { Key = "MDI_GPS_ANTENNA", FieldType = FieldType.Boolean , DisplayName = "Gps Antenna", SortId = 76 }},
            //    { "MDI_OBD_PID_1", new FieldDefinition() { Key = "MDI_OBD_PID_1", FieldType = FieldType.String, DisplayName = "OBD PID 1" , SortId = 77 }},
            //    { "MDI_OBD_PID_2", new FieldDefinition() { Key = "MDI_OBD_PID_2", FieldType = FieldType.String, DisplayName = "OBD PID 2" , SortId = 78 }},
            //    { "MDI_OBD_PID_3", new FieldDefinition() { Key = "MDI_OBD_PID_3", FieldType = FieldType.String, DisplayName = "OBD PID 3" , SortId = 79 }},
            //    { "MDI_OBD_PID_4", new FieldDefinition() { Key = "MDI_OBD_PID_4", FieldType = FieldType.String, DisplayName = "OBD PID 4" , SortId = 80 }},
            //    { "MDI_OBD_PID_5", new FieldDefinition() { Key = "MDI_OBD_PID_5", FieldType = FieldType.String, DisplayName = "OBD PID 5" , SortId = 81 }},
            //    { "MDI_SQUARELL_LAST_RECORDED_MESSAGE_PART_1", new FieldDefinition() { Key = "MDI_SQUARELL_LAST_RECORDED_MESSAGE_PART_1", FieldType = FieldType.String, DisplayName = "SQUARELL 1", SortId = 82  }},
            //    { "MDI_SQUARELL_LAST_RECORDED_MESSAGE_PART_2", new FieldDefinition() { Key = "MDI_SQUARELL_LAST_RECORDED_MESSAGE_PART_2", FieldType = FieldType.String, DisplayName = "SQUARELL 2" , SortId = 83 }},
            //    { "MDI_SQUARELL_LAST_RECORDED_MESSAGE_PART_3", new FieldDefinition() { Key = "MDI_SQUARELL_LAST_RECORDED_MESSAGE_PART_3", FieldType = FieldType.String, DisplayName = "SQUARELL 3", SortId = 84  }},
            //    { "MDI_DASHBOARD_MILEAGE", new FieldDefinition() { Key = "MDI_DASHBOARD_MILEAGE", FieldType = FieldType.Integer, DisplayName = "MDI_DASHBOARD_MILEAGE)" , SortId = 85 }},
            //    { "MDI_DASHBOARD_FUEL", new FieldDefinition() { Key = "MDI_DASHBOARD_FUEL", FieldType = FieldType.Integer, DisplayName = "MDI_DASHBOARD_FUEL", SortId = 86  }},
            //    { "MDI_DASHBOARD_FUEL_LEVEL", new FieldDefinition() { Key = "MDI_DASHBOARD_FUEL_LEVEL", FieldType = FieldType.Integer , DisplayName = "MDI_DASHBOARD_FUEL_LEVEL", SortId =87 }},           
            //    { "MDI_DIAG_1", new FieldDefinition() { Key = "MDI_DIAG_1", FieldType = FieldType.String, DisplayName = "MDI_DIAG_1", SortId = 88  }},
            //    { "MDI_DIAG_2", new FieldDefinition() { Key = "MDI_DIAG_2", FieldType = FieldType.String, DisplayName = "MDI_DIAG_2" , SortId = 89 }},
            //    { "MDI_DIAG_3", new FieldDefinition() { Key = "MDI_DIAG_3", FieldType = FieldType.String , DisplayName = "MDI_DIAG_3", SortId = 90 }},
            //    { "MDI_OBD_SPEED", new FieldDefinition() { Key = "MDI_OBD_SPEED", FieldType = FieldType.Integer, DisplayName = "Obd Speed (km/h)" , SortId = 91 }},
            //    { "MDI_OBD_RPM", new FieldDefinition() { Key = "MDI_OBD_RPM", FieldType = FieldType.Integer, DisplayName = "Obd Rpm", SortId = 92  }},
            //    { "MDI_OBD_FUEL", new FieldDefinition() { Key = "MDI_OBD_FUEL", FieldType = FieldType.Integer , DisplayName = "Obd Fuel", SortId = 93 }},
            //    { "MDI_OBD_VIN", new FieldDefinition() { Key = "MDI_OBD_VIN", FieldType = FieldType.String , DisplayName = "Obd Vin", SortId = 94 }},
            //    { "MDI_OBD_MILEAGE", new FieldDefinition() { Key = "MDI_OBD_MILEAGE", FieldType = FieldType.Integer , DisplayName = "Obd Mileage", SortId = 95 }},
            //    { "MDI_JOURNEY_TIME", new FieldDefinition() { Key = "MDI_JOURNEY_TIME", FieldType = FieldType.Second , DisplayName = "Journey Time", SortId = 96 }},
            //    { "MDI_IDLE_JOURNEY", new FieldDefinition() { Key = "MDI_IDLE_JOURNEY", FieldType = FieldType.Second , DisplayName = "Idle journey", SortId = 97 }},
            //    { "MDI_DRIVING_JOURNEY", new FieldDefinition() { Key = "MDI_DRIVING_JOURNEY", FieldType = FieldType.Second , DisplayName = "Driving journey", SortId = 98 }},
            //    { "MDI_MAX_SPEED_IN_LAST_OVERSPEED", new FieldDefinition() { Key = "MDI_MAX_SPEED_IN_LAST_OVERSPEED", FieldDependency = "MDI_OVERSPEED", FieldType = FieldType.Integer , DisplayName = "Max speed in last overspeed", SortId = 99 }},
            //    { "MDI_OVERSPEED_COUNTER", new FieldDefinition() { Key = "MDI_OVERSPEED_COUNTER", FieldType = FieldType.Integer , DisplayName = "Overspeed Counter", SortId = 100 }},
            //    { "MDI_TOW_AWAY", new FieldDefinition() { Key = "MDI_TOW_AWAY", FieldType = FieldType.Boolean , DisplayName = "Tow Away", SortId = 101 }},
            //    { "MDI_ODO_JOURNEY", new FieldDefinition() { Key = "MDI_ODO_JOURNEY", FieldType = FieldType.Integer , DisplayName = "Odo Journey", SortId = 102 }},
            //    { "MDI_OVERSPEED", new FieldDefinition() { Key = "MDI_OVERSPEED", FieldType = FieldType.Boolean , DisplayName = "Overspeed Status", SortId = 103 }},
            //    { "MDI_MAX_SPEED_JOURNEY", new FieldDefinition() { Key = "MDI_MAX_SPEED_JOURNEY", FieldType = FieldType.Integer , DisplayName = "Overspeed Max", SortId = 104 }},
            //    { "MDI_JOURNEY_STATE", new FieldDefinition() { Key = "MDI_JOURNEY_STATE", FieldType = FieldType.Integer , DisplayName = "Journey State", SortId = 105 }},
            //    { "MDI_RECORD_REASON", new FieldDefinition() { Key = "MDI_RECORD_REASON", FieldType = FieldType.String , DisplayName = "Record Reason", IgnoreInHistory = true, SortId = 106 }},
            //    { "MDI_VEHICLE_STATE", new FieldDefinition() { Key = "MDI_VEHICLE_STATE", FieldType = FieldType.String , DisplayName = "Vehicle State", SortId = 107 }},
            //    { "BOOT_REASON", new FieldDefinition() { Key = "BOOT_REASON", FieldType = FieldType.String , DisplayName = "Boot Reason", IgnoreInHistory = true, SortId = 108 }},
            //    { "MDI_SHUTDOWN_REASON", new FieldDefinition() { Key = "MDI_SHUTDOWN_REASON", FieldType = FieldType.String , DisplayName = "Shutd. Reason", IgnoreInHistory = true, SortId = 109 }},
            //    { "MDI_PANIC_STATE", new FieldDefinition() { Key = "MDI_PANIC_STATE", FieldType = FieldType.Boolean , DisplayName = "Panic state", SortId = 110 }},
            //    { "MDI_PANIC_MESSAGE", new FieldDefinition() { Key = "MDI_PANIC_MESSAGE",FieldType = FieldType.String , DisplayName = "Panic Message", SortId = 111 }},        
            //    { "ENH_DASHBOARD_MILEAGE", new FieldDefinition() { Key = "ENH_DASHBOARD_MILEAGE", FieldType = FieldType.String, DisplayName = "ENH_DASHBOARD_MILEAGE)", Source = "cloud" , SortId = 112 }},
            //    { "ENH_DASHBOARD_FUEL", new FieldDefinition() { Key = "ENH_DASHBOARD_FUEL",FieldType = FieldType.String, DisplayName = "ENH_DASHBOARD_FUEL", Source = "cloud" , SortId = 113 }},
            //    { "ENH_DASHBOARD_FUEL_LEVEL", new FieldDefinition() { Key = "ENH_DASHBOARD_FUEL_LEVEL", FieldType = FieldType.String , DisplayName = "ENH_DASHBOARD_FUEL_LEVEL", Source = "cloud" , SortId = 114 }}
            //};

            string json = "";
            StreamReader sr = new StreamReader(AppDomain.CurrentDomain.BaseDirectory + "/field_definition.json");
            json = sr.ReadToEnd();
            sr.Close();

            List<FieldDefinition> fields = JsonConvert.DeserializeObject<List<FieldDefinition>>(json);
            CouchbaseManager.Instance.FieldDefinitionRepository.BulkUpsert(fields);
            LoadFieldDefinition();
        }

        private void LoadFieldDefinition()
        {
            List<FieldDefinition> fields = CouchbaseManager.Instance.FieldDefinitionRepository.GetAllSortedFieldDefinition();

            if (fields.Count == 0)
                GenerateDefaultField();
            else
            {
                foreach (FieldDefinition field in fields)
                {
                    if (_cache.ContainsKey(field.Key))
                        _cache[field.Key] = field;
                    else
                        _cache.Add(field.Key, field);
                }
            }

            _lastCacheUpdated = DateTime.UtcNow;
        }
    }
}
