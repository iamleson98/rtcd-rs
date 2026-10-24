/**
 * Local capture and remote playback for calls: getting a microphone and
 * camera that work on real classroom hardware, remembering the chosen
 * devices, and playing everyone else's audio.
 */

import type { DeviceKind, MediaDevice, MediaProblem } from './call-state.svelte';

/** Echo cancellation & co. (the Calls plugin's defaults). */
const AUDIO: MediaTrackConstraints = {
	echoCancellation: true,
	noiseSuppression: true,
	autoGainControl: true
};

/** 640×360@24 keeps CPU and bandwidth sane on classroom hardware. */
const VIDEO_QUALITY: MediaTrackConstraints = {
	frameRate: { ideal: 24 },
	width: { ideal: 640 },
	height: { ideal: 360 }
};

/**
 * How a camera is encoded: two simulcast layers, and the SFU forwards each
 * viewer the one it shows (a viewer receives it all, and the SFU's outbound
 * traffic is what a call costs). The small layer (320×180 at 150 kbit/s)
 * fills tiles; the full one is as small as 640×360 allows without visible
 * loss: 600 kbit/s at 24 fps for the teacher, whose camera every student
 * watches, 400 kbit/s at 20 fps for a student, rarely seen large.
 */
export function cameraEncodings(teacher: boolean): RTCRtpEncodingParameters[] {
	return [
		{ rid: 'l', scaleResolutionDownBy: 2, maxBitrate: 150_000, maxFramerate: 15 },
		teacher
			? { rid: 'h', maxBitrate: 600_000, maxFramerate: 24 }
			: { rid: 'h', maxBitrate: 400_000, maxFramerate: 20 }
	];
}

/**
 * How our camera is watched, as the server works it out from what everyone
 * receives: by nobody, only small (tiles), or large somewhere.
 */
export type VideoDemand = 'none' | 'low' | 'high';

export function isVideoDemand(value: unknown): value is VideoDemand {
	return value === 'none' || value === 'low' || value === 'high';
}

/**
 * The camera's layers, switched on for what is watched: none uploads nothing
 * (the camera stays on and our own preview keeps playing), low only the
 * small layer, high both, the SFU still picking per viewer.
 */
export function withVideoDemand(
	encodings: RTCRtpEncodingParameters[],
	demand: VideoDemand
): RTCRtpEncodingParameters[] {
	return encodings.map((encoding) => ({
		...encoding,
		active: demand === 'high' || (demand === 'low' && encoding.rid === 'l')
	}));
}

/** The SFU labels forwarded tracks "<kind>_<originSessionId>_<random>". */
export type TrackKind = 'voice' | 'video' | 'screen' | 'screen-audio';

/**
 * Who the SFU is sending on each of our video slots, from its session
 * description: mid → the sender's kind and session, read from `a=msid`,
 * for the slots it currently sends on. This, not the browser track's id,
 * is the truth: when the SFU removes a camera and adds another, the browser
 * reuses the free slot and its track keeps the first sender's id.
 */
export function remoteVideoSenders(
	sdp: string
): Map<string, { kind: TrackKind; sessionId: string }> {
	const out = new Map<string, { kind: TrackKind; sessionId: string }>();
	for (const section of sdp.split(/\r?\n(?=m=)/).slice(1)) {
		if (!section.startsWith('m=video')) continue;
		const mid = /^a=mid:(\S+)/m.exec(section)?.[1];
		const msidTrack = /^a=msid:\S+ (\S+)/m.exec(section)?.[1];
		const sending = /^a=(sendonly|sendrecv)\b/m.test(section);
		const origin = msidTrack ? parseTrackId(msidTrack) : null;
		if (mid && sending && origin) out.set(mid, origin);
	}
	return out;
}

export function parseTrackId(trackId: string): { kind: TrackKind; sessionId: string } | null {
	const first = trackId.indexOf('_');
	const last = trackId.lastIndexOf('_');
	if (first === -1 || last === first) return null;
	const kind = trackId.slice(0, first);
	if (!['voice', 'video', 'screen', 'screen-audio'].includes(kind)) return null;
	return { kind: kind as TrackKind, sessionId: trackId.slice(first + 1, last) };
}

/**
 * Opens one capture track, relaxing constraints on failure: a stale saved
 * device id (browsers rotate them), a camera that can't do the preferred
 * mode, or a driver rejecting processing flags all fail on machines whose
 * device works fine. Only the last failure is reported.
 */
export async function captureTrack(
	kind: 'audio' | 'video',
	deviceId: string
): Promise<MediaStreamTrack> {
	const device = deviceId ? { deviceId: { exact: deviceId } } : {};
	const attempts: MediaStreamConstraints[] =
		kind === 'audio'
			? [{ audio: { ...AUDIO, ...device } }, { audio: AUDIO }, { audio: true }]
			: [
					{ video: { ...VIDEO_QUALITY, facingMode: 'user', ...device } },
					{ video: VIDEO_QUALITY },
					{ video: true }
				];
	let last: unknown;
	for (const constraints of attempts) {
		try {
			const stream = await navigator.mediaDevices.getUserMedia(constraints);
			const track =
				kind === 'audio' ? stream.getAudioTracks()[0] : stream.getVideoTracks()[0];
			if (track) return track;
		} catch (err) {
			last = err;
		}
	}
	throw last ?? new Error(`no ${kind} input`);
}

/** Which banner a capture failure deserves. */
export function captureProblem(kind: 'audio' | 'video', err: unknown): MediaProblem {
	const name = (err as { name?: string } | null)?.name ?? '';
	const denied = name === 'NotAllowedError' || name === 'SecurityError';
	if (kind === 'audio') return denied ? 'mic-permission' : 'mic-missing';
	return denied ? 'camera-permission' : 'camera-missing';
}

export async function listDevices(): Promise<Record<DeviceKind, MediaDevice[]>> {
	const out: Record<DeviceKind, MediaDevice[]> = {
		audioinput: [],
		videoinput: [],
		audiooutput: []
	};
	if (!navigator.mediaDevices?.enumerateDevices) return out;
	const all = await navigator.mediaDevices.enumerateDevices();
	for (const d of all) {
		const list = out[d.kind as DeviceKind];
		// Without permission, devices come back unnamed and with empty ids.
		if (list && d.deviceId) list.push({ deviceId: d.deviceId, label: d.label });
	}
	// Speakers need no permission to play through, so a machine whose
	// microphone was never granted (or has none) still offers its default.
	if (out.audiooutput.length === 0 && all.some((d) => d.kind === 'audiooutput')) {
		out.audiooutput.push({ deviceId: 'default', label: '' });
	}
	return out;
}

const DEVICE_KEYS: Record<DeviceKind, string> = {
	audioinput: 'calls.device.audioinput',
	videoinput: 'calls.device.videoinput',
	audiooutput: 'calls.device.audiooutput'
};

/** The saved device choice (a convenience: empty when storage is unavailable). */
export function savedDevice(kind: DeviceKind): string {
	try {
		return localStorage.getItem(DEVICE_KEYS[kind]) ?? '';
	} catch {
		return '';
	}
}

export function saveDevice(kind: DeviceKind, deviceId: string): void {
	try {
		localStorage.setItem(DEVICE_KEYS[kind], deviceId);
	} catch {
		// storage unavailable: the choice lasts for this call only
	}
}

type SinkAudio = HTMLAudioElement & { setSinkId?: (id: string) => Promise<void> };

/**
 * Plays remote voice and screen audio through detached <audio> elements
 * (one per track), on the chosen speaker where the browser allows it.
 */
export class RemoteAudio {
	#elements = new Map<string, SinkAudio>();
	#sink = '';

	play(track: MediaStreamTrack, stream: MediaStream): void {
		if (this.#elements.has(track.id)) return;
		const el = document.createElement('audio') as SinkAudio;
		el.autoplay = true;
		el.srcObject = stream;
		if (this.#sink) void el.setSinkId?.(this.#sink).catch(() => undefined);
		document.body.appendChild(el);
		this.#elements.set(track.id, el);
		track.addEventListener('ended', () => this.stop(track.id));
	}

	setSink(deviceId: string): void {
		this.#sink = deviceId;
		for (const el of this.#elements.values()) {
			void el.setSinkId?.(deviceId).catch(() => undefined);
		}
	}

	stop(trackId: string): void {
		const el = this.#elements.get(trackId);
		if (!el) return;
		el.srcObject = null;
		el.remove();
		this.#elements.delete(trackId);
	}

	stopAll(): void {
		for (const id of [...this.#elements.keys()]) this.stop(id);
	}
}
