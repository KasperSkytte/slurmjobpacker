"""Elastic limits, as a short pulse.

The QOS caps exist to stop one user swallowing the cluster inside a minute, and
they must stay in place almost all the time: a user who submits a large pool of
jobs while the caps are raised can hold nodes for days, because jobs are not
preemptible. So the caps are never raised for long. When jobs are held only by
the per-user or per-account CPU cap, and they would fit in idle hardware that
nobody else is waiting for, the caps are raised for a minute -- long enough for
the scheduler to start some of those jobs -- and then put back to base. Between
pulses there is a cooldown. The raise is uniform, the same for every user, so
fair-share still decides who gets the room.
"""
from __future__ import annotations
import time


class LimitPulse:
    """Moves MaxTRESPU/MaxTRESPA on one QOS: base, briefly up, back to base.
    One per QOS that holds jobs; its base caps come from the QOS (set_base)."""

    def __init__(self, cfg, qos, base=(None, None)):
        c = cfg["limits"]
        self.base_u, self.base_a = base                    # None: the QOS has no cap
        self.ceiling = c["ceiling"]
        self.raise_above = c["raise_above"]
        self.lower_below = c["lower_below"]
        self.hysteresis = c["hysteresis"]
        self.pulse = c["pulse_seconds"]
        self.cooldown = c["cooldown_seconds"]
        self.qos = qos
        self.cur_u, self.cur_a = self.base_u, self.base_a
        self.streak = 0
        self.raised_at = None
        self.lowered_at = float("-inf")
        self.why = ""

    @property
    def raised(self) -> bool:
        return self.raised_at is not None

    @property
    def known(self) -> bool:
        """Whether there are base caps to pulse from (the QOS may have none)."""
        return bool(self.base_u and self.base_a)

    def set_base(self, per_user, per_account):
        self.base_u, self.base_a = per_user, per_account
        if not self.raised:
            self.cur_u, self.cur_a = per_user, per_account

    def observe(self, idle_fraction: float, held_that_fit: int, now: float | None = None):
        """Return (per_user, per_account) if the caps should change, else None.

        held_that_fit: pending jobs held by a CPU cap that fit in free space now.
        """
        now = time.time() if now is None else now
        if not self.known:
            return None
        if self.raised:
            if now - self.raised_at >= self.pulse:
                self.why = f"the {self.pulse:.0f} s pulse is over; back to base"
            elif idle_fraction < self.lower_below:
                self.why = (f"the cluster filled up (idle {idle_fraction:.0%} < "
                            f"{self.lower_below:.0%}); back to base early")
            else:
                return None
            return self.reset(now)

        idle = idle_fraction >= self.raise_above
        self.streak = self.streak + 1 if idle and held_that_fit else 0
        if self.streak < self.hysteresis or now - self.lowered_at < self.cooldown:
            return None
        self.streak = 0
        self.raised_at = now
        self.cur_u = int(self.base_u * self.ceiling)
        self.cur_a = int(self.base_a * self.ceiling)
        self.why = (f"{held_that_fit} jobs held only by the CPU cap would fit in idle "
                    f"hardware, and {idle_fraction:.0%} of the cluster has been idle for "
                    f"{self.hysteresis} checks; raise for {self.pulse:.0f} s")
        return self.cur_u, self.cur_a

    def reset(self, now: float | None = None):
        """Back to base. Returns the base caps."""
        self.raised_at = None
        self.lowered_at = time.time() if now is None else now
        self.cur_u, self.cur_a = self.base_u, self.base_a
        return self.cur_u, self.cur_a

    def state(self) -> dict:
        return dict(qos=self.qos, per_user=self.cur_u, per_account=self.cur_a,
                    raised=self.raised, streak=self.streak)
