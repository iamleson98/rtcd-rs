/**
 * Connection quality from the peer connection's stats (round-trip time,
 * jitter, packet loss), sampled every few seconds and smoothed — the Calls plugin's
 * RTC monitor, reduced to a coarse good/fair/poor level.
 */

import type { CallQuality } from './call-state.svelte';

const INTERVAL_MS = 4_000;
/** A new level shows only after this many samples agree, so it never flickers. */
const CONFIRM_SAMPLES = 2;

/** Coarse level from raw stats (ms, ms, 0..1). */
export function classifyQuality(
	rtt: number | null,
	jitter: number | null,
	loss: number | null
): CallQuality {
	if (rtt === null && jitter === null && loss === null) return 'unknown';
	let score = 5;
	if (rtt !== null) score -= Math.max(0, (rtt - 150) / 150) * 1.2;
	if (jitter !== null) score -= Math.max(0, (jitter - 20) / 40);
	if (loss !== null) score -= Math.max(0, loss - 0.02) * 30;
	if (score >= 4) return 'good';
	if (score >= 3) return 'fair';
	return 'poor';
}

export class QualityMonitor {
	#timer: ReturnType<typeof setInterval> | undefined;
	#last: { lost: number; received: number } | null = null;
	#shown: CallQuality | null = null;
	#candidate: CallQuality | null = null;
	#streak = 0;

	constructor(
		private readonly pc: RTCPeerConnection,
		private readonly onSample: (quality: CallQuality) => void
	) {}

	start(): void {
		this.stop();
		this.#timer = setInterval(() => void this.#poll(), INTERVAL_MS);
	}

	stop(): void {
		clearInterval(this.#timer);
		this.#last = null;
		this.#shown = this.#candidate = null;
		this.#streak = 0;
	}

	/** Forget loss counters (after mute/camera changes the window restarts). */
	reset(): void {
		this.#last = null;
	}

	async #poll() {
		let rtt: number | null = null;
		let jitter: number | null = null;
		let loss: number | null = null;
		let lost = 0;
		let received = 0;
		try {
			const stats = await this.pc.getStats();
			stats.forEach((report) => {
				const r = report as Record<string, unknown>;
				if (r.type === 'candidate-pair' && (r.selected === true || r.nominated === true)) {
					if (typeof r.currentRoundTripTime === 'number')
						rtt = r.currentRoundTripTime * 1000;
				}
				if (r.type === 'inbound-rtp') {
					if (typeof r.jitter === 'number')
						jitter = Math.max(jitter ?? 0, r.jitter * 1000);
					lost += Number(r.packetsLost ?? 0);
					received += Number(r.packetsReceived ?? 0);
				}
			});
		} catch {
			return; // the connection is closing; the next sample decides
		}
		if (this.#last && received > this.#last.received) {
			const dLost = Math.max(0, lost - this.#last.lost);
			const dReceived = received - this.#last.received;
			loss = dLost / (dLost + dReceived);
		}
		this.#last = { lost, received };
		this.onSample(this.#settle(classifyQuality(rtt, jitter, loss)));
	}

	/** The level to show: the first sample at once, then only confirmed changes. */
	#settle(sample: CallQuality): CallQuality {
		if (this.#shown === null || sample === this.#shown) {
			this.#shown ??= sample;
			this.#candidate = null;
			this.#streak = 0;
		} else {
			this.#streak = this.#candidate === sample ? this.#streak + 1 : 1;
			this.#candidate = sample;
			if (this.#streak >= CONFIRM_SAMPLES) {
				this.#shown = sample;
				this.#candidate = null;
				this.#streak = 0;
			}
		}
		return this.#shown;
	}
}
