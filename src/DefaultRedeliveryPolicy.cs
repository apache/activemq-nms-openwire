using Apache.NMS.Policies;

namespace Apache.NMS.ActiveMQ;

public class DefaultRedeliveryPolicy : RedeliveryPolicy
{
    public override int GetOutcome(IDestination destination)
    {
        return (int) AckType.PoisonAck;
    }
}