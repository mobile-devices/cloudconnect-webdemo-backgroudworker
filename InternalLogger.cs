using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace CloudConnect.BackgroundWorker
{
    public static class InternalLogger
    {
        private static DateTime _lastFlush = DateTime.MinValue;

        public static void WriteLog(string log)
        {
            if (Environment.UserInteractive)
                Console.WriteLine(String.Format("[{0}] log : {1}", DateTime.UtcNow, log));
            else
            {
                bool append = true;

                if (DateTime.UtcNow.Ticks - _lastFlush.Ticks > (TimeSpan.TicksPerHour))
                {
                    append = false;
                    _lastFlush = DateTime.UtcNow;
                }
                StreamWriter sw = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory + @"\LogFile.txt", append);
                sw.WriteLine(String.Format("[{0}] log : {1}", DateTime.UtcNow, log));
                sw.Flush();
                sw.Close();
            }

        }
    }
}
