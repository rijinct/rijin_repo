
package com.rijin.analytics.scheduler.k8s;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class HealthCheck implements HealthIndicator {

	private boolean ready = false;

	@Override
	public Health health() {
		int errorCode = check();
		if (errorCode != 0) {
			return Health.down()
					.withDetail(
							"scheduler engine is gracefully shutting down",
							errorCode)
					.build();
		}
		return Health.up().build();
	}

	public int check() {
		return ready ? 0 : 1;
	}

	public void setReady(boolean state) {
		ready = state;
	}
}
