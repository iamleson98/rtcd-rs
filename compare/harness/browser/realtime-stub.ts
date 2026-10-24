// Stands in for the LMS web app's realtime socket ($lib/realtime/socket.svelte): the call client
// only ever calls notify(), and the harness feeds the server's events back in by hand.
type Sender = (message: string) => void;

class StubRealtime {
	#send: Sender | null = null;
	attach(send: Sender): void {
		this.#send = send;
	}
	notify(action: string, data: Record<string, unknown> = {}): void {
		this.#send?.(JSON.stringify({ action, data }));
	}
}

export const realtime = new StubRealtime();
