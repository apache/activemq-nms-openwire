using Apache.NMS.ActiveMQ.Commands;
using System;
using System.Reflection;


namespace Apache.NMS.ActiveMQ.Util
{
    internal class ExceptionFromBrokerError
    {
        public static NMSException CreateExceptionFromBrokerError(BrokerError brokerError)
        {
            String exceptionClassName = brokerError.ExceptionClass;

            if (String.IsNullOrEmpty(exceptionClassName))
            {
                return new BrokerException(brokerError);
            }

            NMSException exception = null;
            String message = brokerError.Message;

            // We only create instances of exceptions from the NMS API
            Assembly nmsAssembly = Assembly.GetAssembly(typeof(NMSException));

            // First try and see if it's one we populated ourselves in which case
            // it will have the correct namespace and exception name.
            Type exceptionType = nmsAssembly.GetType(exceptionClassName, false, true);

            // Exceptions from the broker don't have the same namespace, so we
            // trim that and try using the NMS namespace to see if we can get an
            // NMSException based version of the same type.  We have to convert
            // the JMS prefixed exceptions to NMS also.
            if (null == exceptionType)
            {
                if (exceptionClassName.StartsWith("java.lang.SecurityException"))
                {
                    exceptionClassName = "Apache.NMS.NMSSecurityException";
                }
                else if (!exceptionClassName.StartsWith("Apache.NMS"))
                {
                    string transformClassName;

                    if (exceptionClassName.Contains("."))
                    {
                        int pos = exceptionClassName.LastIndexOf(".");
                        transformClassName = exceptionClassName.Substring(pos + 1).Replace("JMS", "NMS");
                    }
                    else
                    {
                        transformClassName = exceptionClassName;
                    }

                    exceptionClassName = "Apache.NMS." + transformClassName;
                }

                exceptionType = nmsAssembly.GetType(exceptionClassName, false, true);
            }

            if (exceptionType != null)
            {
                object[] args = null;
                if (!String.IsNullOrEmpty(message))
                {
                    args = new object[1];
                    args[0] = message;
                }

                exception = Activator.CreateInstance(exceptionType, args) as NMSException;
            }
            else
            {
                exception = new BrokerException(brokerError);
            }

            return exception;
        }
    }
}
