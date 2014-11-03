using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace CloudConnect.BackgroundWorker
{
    internal static class InternalLogger
    {

        public static void WriteLog(string log)
        {
            StreamWriter sw = new StreamWriter(AppDomain.CurrentDomain.BaseDirectory + @"\LogFile.txt", true);
            sw.WriteLine(String.Format("[{0}] log : {1}", DateTime.UtcNow, log));
            sw.Flush();
            sw.Close();
        }
    }
}
