# Stateful deduplication logic with state time-to-live (TTL)
from pyflink.common import Time
from pyflink.datastream.state import StateTtlConfig

def configure_state_ttl() -> StateTtlConfig:
    # 24-hour expiration prevents unbounded off-heap state growth
    return StateTtlConfig.new_builder(Time.hours(24)) \
        .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
        .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
        .build()
