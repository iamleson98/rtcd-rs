/**
 * The data channel rtcd expects every participant to open ("calls-dc").
 * rtcd starts forwarding other people's tracks only once it is open, and
 * uses it as a signaling lock: a side about to send an offer locks first,
 * so the SFU and the browser never offer at the same time (glare), and
 * unlocks once the answer is in.
 *
 * It also carries the session's SDP once open (the server registers every
 * session with dcSignaling): an offer lists every media line of the call
 * and outgrows what the websocket takes in one message.
 *
 * Messages are msgpack (rtcd service/rtc/dc/msg.go): a type number,
 * optionally followed by a payload. We send lock/unlock (no payload) and
 * SDP (binary: the zlib-compressed JSON description), and read the SFU's
 * lock answers (a bool) and SDP; everything else is ignored.
 */

export const SFU_CHANNEL_LABEL = 'calls-dc';

const SDP = 3;
const LOCK = 7;
const UNLOCK = 8;

const LOCK_REPLY_TIMEOUT_MS = 3_000;
const LOCK_RETRY_MS = 100;
const LOCK_GIVE_UP_MS = 10_000;

/** msgpack positive fixint: the type numbers fit in one byte. */
export function encodeMessage(type: number): Uint8Array<ArrayBuffer> {
	return new Uint8Array([type]);
}

/** An SDP message: the type, then the payload as msgpack bin 8/16/32. */
export function encodeSdpMessage(payload: Uint8Array): Uint8Array<ArrayBuffer> {
	const n = payload.length;
	const header =
		n < 0x100
			? [SDP, 0xc4, n]
			: n < 0x10000
				? [SDP, 0xc5, n >> 8, n & 0xff]
				: [SDP, 0xc6, n >>> 24, (n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff];
	const out = new Uint8Array(header.length + n);
	out.set(header);
	out.set(payload, header.length);
	return out;
}

/** Reads a msgpack bin 8/16/32 at i (null when it is something else). */
function readBin(bytes: Uint8Array, i: number): Uint8Array | null {
	const code = bytes[i];
	const size = code === 0xc4 ? 1 : code === 0xc5 ? 2 : code === 0xc6 ? 4 : 0;
	if (size === 0 || i + 1 + size > bytes.length) return null;
	let n = 0;
	for (let k = 0; k < size; k++) n = n * 0x100 + bytes[i + 1 + k];
	const start = i + 1 + size;
	return start + n <= bytes.length ? bytes.subarray(start, start + n) : null;
}

/** zlib (RFC 1950), as rtcd's Go side reads and writes it. */
async function transform(
	data: Uint8Array,
	stream: CompressionStream | DecompressionStream
): Promise<Uint8Array> {
	const writer = stream.writable.getWriter();
	// A failure surfaces on the readable side, read below.
	writer.write(data as Uint8Array<ArrayBuffer>).catch(() => {});
	writer.close().catch(() => {});
	return new Uint8Array(await new Response(stream.readable).arrayBuffer());
}

export const deflate = (text: string) =>
	transform(new TextEncoder().encode(text), new CompressionStream('deflate'));

export const inflate = async (data: Uint8Array) =>
	new TextDecoder().decode(await transform(data, new DecompressionStream('deflate')));

/** Decodes the type and, for lock answers, the granted flag; for SDP,
 * the compressed payload. */
export function decodeMessage(
	data: ArrayBuffer | Uint8Array
): { type: number; granted?: boolean; payload?: Uint8Array } | null {
	const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
	if (bytes.length === 0) return null;
	let i = 0;
	let type: number;
	if (bytes[0] <= 0x7f) {
		type = bytes[i++]; // positive fixint
	} else if (bytes[0] === 0xcc && bytes.length > 1) {
		type = bytes[1]; // uint8
		i = 2;
	} else {
		return null;
	}
	if (type === LOCK && i < bytes.length) {
		return { type, granted: bytes[i] === 0xc3 };
	}
	if (type === SDP) {
		const payload = readBin(bytes, i);
		return payload ? { type, payload } : null;
	}
	return { type };
}

/** The lock protocol over an RTCDataChannel. */
export class SfuChannel {
	#channel: RTCDataChannel;
	#pendingLock: ((granted: boolean) => void) | null = null;
	#held = false;

	/** onSdp gets each description the SFU sends here (its JSON). */
	constructor(channel: RTCDataChannel, onSdp: (description: string) => void) {
		this.#channel = channel;
		channel.binaryType = 'arraybuffer';
		// Descriptions are inflated in order: an answer must not overtake
		// the offer before it.
		let received = Promise.resolve();
		channel.addEventListener('message', (e) => {
			if (!(e.data instanceof ArrayBuffer)) return;
			const msg = decodeMessage(e.data);
			if (msg?.type === LOCK && this.#pendingLock) {
				this.#pendingLock(msg.granted === true);
				this.#pendingLock = null;
			} else if (msg?.type === SDP && msg.payload) {
				const payload = msg.payload;
				received = received
					.then(() => inflate(payload))
					.then(onSdp, (err) => console.warn('[calls] unreadable SDP', err));
			}
		});
	}

	get open(): boolean {
		return this.#channel.readyState === 'open';
	}

	/**
	 * Takes the signaling lock before we offer. Resolves when granted —
	 * or immediately before the channel opens (the first offer creates it)
	 * and after giving up, so negotiation is never stuck.
	 */
	async lock(): Promise<void> {
		if (!this.open || this.#held) return;
		const deadline = Date.now() + LOCK_GIVE_UP_MS;
		while (this.open && Date.now() < deadline) {
			const granted = await new Promise<boolean>((resolve) => {
				const timer = setTimeout(() => resolve(false), LOCK_REPLY_TIMEOUT_MS);
				this.#pendingLock = (value) => {
					clearTimeout(timer);
					resolve(value);
				};
				this.#channel.send(encodeMessage(LOCK));
			});
			if (granted) {
				this.#held = true;
				return;
			}
			await new Promise((resolve) => setTimeout(resolve, LOCK_RETRY_MS));
		}
	}

	/** Sends a description (its JSON) to the SFU. */
	async sendSdp(description: string): Promise<void> {
		this.#channel.send(encodeSdpMessage(await deflate(description)));
	}

	/** Releases the lock (after the SFU answered our offer). */
	unlock(): void {
		if (!this.#held) return;
		this.#held = false;
		if (this.open) this.#channel.send(encodeMessage(UNLOCK));
	}
}
