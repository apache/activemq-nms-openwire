using System;
using System.Collections.Generic;
namespace Apache.NMS.ActiveMQ.Transport.Tcp
{
    class SslContext
    {
        private String sslProtocol;

        public SslContext() : this("None")
        {
        }

        public SslContext(String protocol)
        {
            this.sslProtocol = protocol;
        }

        public String SslProtocol
        {
            get { return this.sslProtocol; }
            set { this.sslProtocol = value; }
        }

        [ThreadStatic]
        static private SslContext current;

        static public SslContext GetCurrent()
        {
            if (current == null)
            {
                current = new SslContext();
            }
            return current;
        }

        static public void SetCurrent(SslContext context)
        {
            current = context;
        }
    }
}
