using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;

namespace CloudConnect.BackgroundWorker
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            //ServiceBase[] ServicesToRun;
            //ServicesToRun = new ServiceBase[] 
            //{ 
            //    new NotificationWorker() 
            //};
            //ServiceBase.Run(ServicesToRun);

            NotificationWorker n = new NotificationWorker();
            System.Threading.Thread.Sleep(10000);
        }
    }
}
