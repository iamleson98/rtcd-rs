/**
 * The WebRTC side of a class call (the Calls plugin's client protocol):
 *
 *   browser ──RTCPeerConnection──▶ rtcd SFU      (audio, camera, screen)
 *   browser ──app websocket──────▶ server ──▶ rtcd (signaling, presence)
 *
 * Join: capture first (the first offer must carry the tracks), send
 * `custom_calls_join`; the server's join ack brings our session id and
 * ICE servers; the peer connection then negotiates with the SFU through
 * `sdp`/`ice` actions and `signal` events (polite peer, ICE candidates
 * queued until the remote description lands).
 *
 * Senders are created once and switched with replaceTrack, so muting or
 * turning the camera off never renegotiates (the SFU wants stable ids).
 * The SFU labels forwarded tracks with their origin session, which routes
 * each video to the right tile and plays each voice aloud.
 *
 * Not reactive on purpose: media and negotiation never touch Svelte
 * state; the UI reads CallState, which this client updates.
 */

import { realtime } from '$lib/realtime/socket.svelte';
import type { CallErrorKind, CallState, DeviceKind } from './call-state.svelte';
import {
	cameraEncodings,
	captureProblem,
	captureTrack,
	isVideoDemand,
	listDevices,
	remoteVideoSenders,
	RemoteAudio,
	saveDevice,
	savedDevice,
	withVideoDemand,
	type VideoDemand
} from './media';
import { QualityMonitor } from './quality';
import { SFU_CHANNEL_LABEL, SfuChannel } from './sfu-channel';

/**
 * Two simulcast layers: the SFU picks per viewer. Slides need sharp text,
 * not motion: 15 fps is plenty and costs each viewer less.
 */
const SCREEN_ENCODINGS: RTCRtpEncodingParameters[] = [
	{ rid: 'l', maxBitrate: 500_000, maxFramerate: 5 },
	{ rid: 'h', maxBitrate: 2_500_000, maxFramerate: 15 }
];

const JOIN_ACK_TIMEOUT_MS = 15_000;
const CONNECT_TIMEOUT_MS = 30_000;
const ICE_RESTART_GRACE_MS = 10_000;

/** How long the set of cameras on screen must hold before it is sent. */
export const WANTED_VIDEO_SETTLE_MS = 250;

/**
 * How long less demand on our camera must hold before its upload shrinks: a
 * viewer flicking between views does not make the camera stop and restart.
 * More demand applies at once, so a view that shows us is never kept waiting.
 */
export const VIDEO_DEMAND_DROP_MS = 3_000;
const DEMAND_RANK: Record<VideoDemand, number> = { none: 0, low: 1, high: 2 };

function send(type: string, data: Record<string, unknown> = {}): void {
	realtime.notify(`custom_calls_${type}`, data);
}

export class CallClient {
	#state: CallState;
	/** Teaching this class: the camera everyone watches (see cameraEncodings). */
	#teacher: boolean;
	#pc: RTCPeerConnection | null = null;
	#iceServers: RTCIceServer[] = [];
	#local = new MediaStream();
	#screen: MediaStream | null = null;
	#audioSender: RTCRtpSender | null = null;
	#videoSender: RTCRtpSender | null = null;
	#screenSender: RTCRtpSender | null = null;
	#screenAudioSender: RTCRtpSender | null = null;
	#remoteAudio = new RemoteAudio();
	/** One stream per receiving video slot (mid), see #syncRemoteVideo. */
	#slotStreams = new Map<string, MediaStream>();
	#quality: QualityMonitor | null = null;
	#sfu: SfuChannel | null = null;

	// Perfect negotiation (we are the polite peer).
	#makingOffer = false;
	#ignoreOffer = false;
	#answerPending = false;
	#queuedCandidates: RTCIceCandidateInit[] = [];

	#joinTimer: ReturnType<typeof setTimeout> | undefined;
	#connectTimer: ReturnType<typeof setTimeout> | undefined;
	#failTimer: ReturnType<typeof setTimeout> | undefined;
	/** Join as muted (the server registers new sessions unmuted). */
	#joinMuted = false;
	/** Whose cameras to receive, which small (null: every camera in full, the server default). */
	#wantedVideo: { ids: string[]; low: string[] } | null = null;
	/** What the server was last told, to skip repeats. */
	#sentWantedVideo = '';
	#wantedVideoTimer: ReturnType<typeof setTimeout> | undefined;
	/** How our camera is watched (applied to its layers); every layer until told. */
	#videoDemand: VideoDemand = 'high';
	#videoDemandTimer: ReturnType<typeof setTimeout> | undefined;
	/**
	 * Counts teardowns. Opening a device can take seconds (a permission
	 * prompt); a capture that finishes after the person left belongs to no
	 * call and is stopped at once, so no camera or microphone stays on.
	 */
	#epoch = 0;

	constructor(state: CallState, options: { teacher: boolean }) {
		this.#state = state;
		this.#teacher = options.teacher;
		for (const kind of ['audioinput', 'videoinput', 'audiooutput'] as const) {
			state.selected[kind] = savedDevice(kind);
		}
		this.#remoteAudio.setSink(state.selected.audiooutput);
	}

	// ─── Lifecycle ────────────────────────────────────────────────────

	/**
	 * Joins the channel's call (the teacher's join starts it). listenOnly
	 * opens no device at all: the session recorder only receives.
	 */
	async join(options: { mic: boolean; camera: boolean; listenOnly?: boolean }): Promise<void> {
		const s = this.#state;
		if (s.joined) return;
		if (window.isSecureContext === false) {
			s.error = 'insecure';
			return;
		}
		s.error = null;
		s.status = 'joining';

		// The microphone is always opened (unmuting must be instant), the
		// camera only when asked; each failure degrades that device only.
		const epoch = this.#epoch;
		if (!options.listenOnly) {
			await this.#capture('audio');
			if (options.camera) await this.#capture('video');
		}
		// Left while the devices were opening: the server never hears of it.
		if (epoch !== this.#epoch) return;
		this.#joinMuted = options.listenOnly || !options.mic;
		void this.refreshDevices();

		send('join', { channelID: s.channelId });
		this.#joinTimer = setTimeout(() => this.#fail('timeout'), JOIN_ACK_TIMEOUT_MS);
	}

	/** The server accepted the join: build the peer connection. */
	handleJoinAck(connId: string, iceServers: unknown): void {
		const s = this.#state;
		if (s.status !== 'joining') return;
		clearTimeout(this.#joinTimer);
		s.mySessionId = connId;
		this.#sentWantedVideo = '';
		this.#iceServers = Array.isArray(iceServers)
			? (iceServers as RTCIceServer[]).filter((server) => server?.urls)
			: [];
		this.#createPeerConnection();
		this.#connectTimer = setTimeout(() => {
			if (this.#pc?.connectionState !== 'connected') this.#fail('timeout');
		}, CONNECT_TIMEOUT_MS);

		const mic = this.#local.getAudioTracks()[0];
		if (mic && !this.#joinMuted) {
			s.micOn = true;
		} else {
			if (mic) mic.enabled = false;
			void this.#audioSender?.replaceTrack(null);
			send('mute');
		}
		if (this.#local.getVideoTracks()[0]) this.#announceVideo();
	}

	/**
	 * Names the cameras to receive — those on screen — and those of them to
	 * receive at the small simulcast layer (shown small). The SFU forwards
	 * no other camera, so a board view receives one stream rather than one
	 * per participant. Sent now if connected, else once connected.
	 */
	setWantedVideo(sessionIds: string[], low: string[] = []): void {
		const ids = [...new Set(sessionIds)].sort();
		this.#wantedVideo = { ids, low: [...new Set(low)].filter((id) => ids.includes(id)).sort() };
		// A view change passes through intermediate sets (one surface hides
		// before the next shows); only the settled one goes out, so no
		// camera is dropped and re-added on the way.
		clearTimeout(this.#wantedVideoTimer);
		this.#wantedVideoTimer = setTimeout(() => this.#sendWantedVideo(), WANTED_VIDEO_SETTLE_MS);
	}

	#sendWantedVideo(force = false): void {
		if (this.#wantedVideo === null || this.#state.status !== 'connected') return;
		const { ids, low } = this.#wantedVideo;
		const key = `${ids.join(',')}|${low.join(',')}`;
		if (!force && key === this.#sentWantedVideo) return;
		this.#sentWantedVideo = key;
		send('video_subscribe', { data: JSON.stringify({ session_ids: ids, low }) });
	}

	/**
	 * The server says how our camera is watched (see VideoDemand): the board
	 * view watches the teacher's only, so a student's camera uploads nothing
	 * until someone opens a view that shows it.
	 */
	setVideoDemand(demand: unknown): void {
		if (!isVideoDemand(demand)) return;
		clearTimeout(this.#videoDemandTimer);
		const apply = () => {
			this.#videoDemand = demand;
			void this.#applyVideoDemand();
		};
		if (DEMAND_RANK[demand] >= DEMAND_RANK[this.#videoDemand]) apply();
		else this.#videoDemandTimer = setTimeout(apply, VIDEO_DEMAND_DROP_MS);
	}

	/** The demand our camera's layers follow now. */
	get videoDemand(): VideoDemand {
		return this.#videoDemand;
	}

	/** Switches the camera's layers on and off without renegotiating. */
	async #applyVideoDemand(): Promise<void> {
		const sender = this.#videoSender;
		if (!sender) return;
		const params = sender.getParameters();
		if (!params.encodings?.length) return;
		params.encodings = withVideoDemand(params.encodings, this.#videoDemand);
		await sender.setParameters(params).catch(() => undefined);
	}

	/** Leaves the call and releases every device. */
	leave(): void {
		if (!this.#state.joined) return;
		send('leave', { channelID: this.#state.channelId });
		this.#teardown();
	}

	/** The websocket reconnected: point our session at the new connection. */
	handleReconnect(): void {
		const s = this.#state;
		if (!s.mySessionId || s.status === 'joining') return;
		send('reconnect', {
			channelID: s.channelId,
			originalConnID: s.mySessionId,
			prevConnID: s.mySessionId
		});
		send('call_state', { channelID: s.channelId });
	}

	/** The server ended our participation (call ended, removed, error). */
	end(error: CallErrorKind | null): void {
		this.#teardown();
		this.#state.error = error;
	}

	#fail(error: CallErrorKind) {
		if (!this.#state.joined) return;
		send('leave', { channelID: this.#state.channelId });
		this.end(error);
	}

	#teardown() {
		this.#epoch++;
		clearTimeout(this.#joinTimer);
		clearTimeout(this.#connectTimer);
		clearTimeout(this.#failTimer);
		clearTimeout(this.#videoDemandTimer);
		this.#videoDemand = 'high';
		this.#quality?.stop();
		this.#quality = null;
		window.removeEventListener('online', this.#onNetworkBack);
		for (const track of this.#local.getTracks()) track.stop();
		this.#screen?.getTracks().forEach((track) => track.stop());
		this.#local = new MediaStream();
		this.#screen = null;
		this.#audioSender = this.#videoSender = this.#screenSender = this.#screenAudioSender = null;
		this.#remoteAudio.stopAll();
		this.#pc?.close();
		this.#pc = null;
		this.#slotStreams.clear();
		this.#sfu = null;
		this.#makingOffer = this.#ignoreOffer = this.#answerPending = false;
		this.#queuedCandidates = [];
		this.#state.resetSession();
	}

	// ─── Capture & devices ────────────────────────────────────────────

	async #capture(kind: 'audio' | 'video'): Promise<MediaStreamTrack | null> {
		const s = this.#state;
		const epoch = this.#epoch;
		try {
			const track = await captureTrack(
				kind,
				s.selected[kind === 'audio' ? 'audioinput' : 'videoinput']
			);
			if (epoch !== this.#epoch) {
				track.stop();
				return null;
			}
			for (const old of kind === 'audio'
				? this.#local.getAudioTracks()
				: this.#local.getVideoTracks()) {
				old.stop();
				this.#local.removeTrack(old);
			}
			this.#local.addTrack(track);
			if (kind === 'video') s.localCamera = new MediaStream([track]);
			s.dismissProblem(kind === 'audio' ? 'mic-missing' : 'camera-missing');
			s.dismissProblem(kind === 'audio' ? 'mic-permission' : 'camera-permission');
			return track;
		} catch (err) {
			s.addProblem(captureProblem(kind, err));
			return null;
		}
	}

	/** Device lists (named once a capture permission was granted). */
	async refreshDevices(): Promise<void> {
		try {
			this.#state.devices = await listDevices();
		} catch {
			// enumerateDevices unavailable: the pickers stay empty
		}
	}

	/** Switches a device; live tracks move to it without renegotiating. */
	async selectDevice(kind: DeviceKind, deviceId: string): Promise<void> {
		const s = this.#state;
		s.selected[kind] = deviceId;
		saveDevice(kind, deviceId);
		if (kind === 'audiooutput') {
			this.#remoteAudio.setSink(deviceId);
			return;
		}
		if (!s.joined) return;
		if (kind === 'audioinput') {
			const track = await this.#capture('audio');
			if (track) {
				track.enabled = s.micOn;
				await this.#audioSender?.replaceTrack(s.micOn ? track : null);
			}
		} else if (s.cameraOn) {
			const track = await this.#capture('video');
			if (track) await this.#videoSender?.replaceTrack(track);
		}
	}

	// ─── Controls ─────────────────────────────────────────────────────

	async setMic(on: boolean): Promise<void> {
		const s = this.#state;
		let track: MediaStreamTrack | null = this.#local.getAudioTracks()[0] ?? null;
		if (on && (!track || track.readyState === 'ended')) {
			track = await this.#capture('audio');
			if (!track) return;
			if (!this.#audioSender && this.#pc)
				this.#audioSender = this.#pc.addTrack(track, this.#local);
		}
		if (track) track.enabled = on;
		// Detach the track while muted so no audio leaves at all.
		await this.#audioSender?.replaceTrack(on ? track : null).catch(() => undefined);
		s.micOn = on;
		this.#quality?.reset();
		send(on ? 'unmute' : 'mute');
	}

	async setCamera(on: boolean): Promise<void> {
		const s = this.#state;
		if (on) {
			const track = await this.#capture('video');
			if (!track) return;
			// Announce first: the SFU classifies the track by this stream id
			// when its first packets arrive, and drops unannounced video.
			this.#announceVideo();
			if (this.#videoSender) await this.#videoSender.replaceTrack(track);
			this.#quality?.reset();
		} else {
			// Stop the track so the camera light goes off.
			for (const track of this.#local.getVideoTracks()) {
				track.stop();
				this.#local.removeTrack(track);
			}
			await this.#videoSender?.replaceTrack(null).catch(() => undefined);
			s.cameraOn = false;
			s.localCamera = null;
			send('video_off');
		}
	}

	#announceVideo() {
		this.#state.cameraOn = true;
		// The SFU labels the forwarded track with this stream id.
		send('video_on', { data: JSON.stringify({ videoStreamID: this.#local.id }) });
	}

	async startScreenShare(): Promise<void> {
		const s = this.#state;
		let stream: MediaStream;
		try {
			stream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
		} catch (err) {
			if ((err as { name?: string })?.name === 'NotAllowedError')
				s.addProblem('screen-permission');
			return;
		}
		const video = stream.getVideoTracks()[0];
		if (!video || !this.#pc) {
			stream.getTracks().forEach((track) => track.stop());
			return;
		}
		this.#screen = stream;
		// Slides and code: keep text sharp, give up frame rate instead.
		video.contentHint = 'detail';
		// Each share is a new stream the SFU must recognize: announce its id,
		// then send it on fresh senders (removed again when sharing stops).
		send('screen_on', { data: JSON.stringify({ screenStreamID: stream.id }) });
		this.#screenSender = this.#pc.addTransceiver(video, {
			direction: 'sendonly',
			sendEncodings: SCREEN_ENCODINGS,
			streams: [stream]
		}).sender;
		const audio = stream.getAudioTracks()[0];
		if (audio) this.#screenAudioSender = this.#pc.addTrack(audio, stream);
		s.screenOn = true;
		s.screen = { sessionId: s.mySessionId, stream };
		// Stopping from the browser's own "Stop sharing" bar.
		video.addEventListener('ended', () => this.stopScreenShare());
	}

	stopScreenShare(): void {
		const s = this.#state;
		if (!this.#screen) return;
		this.#screen.getTracks().forEach((track) => track.stop());
		this.#screen = null;
		for (const sender of [this.#screenSender, this.#screenAudioSender]) {
			if (sender && this.#pc?.signalingState !== 'closed') this.#pc?.removeTrack(sender);
		}
		this.#screenSender = this.#screenAudioSender = null;
		s.screenOn = false;
		if (s.screen?.sessionId === s.mySessionId) s.screen = null;
		send('screen_off');
	}

	setHand(raised: boolean): void {
		this.#state.handRaised = raised;
		send(raised ? 'raise_hand' : 'unraise_hand');
	}

	react(emoji: string, name: string): void {
		send('react', { data: JSON.stringify({ name, literal: emoji, unified: '' }) });
	}

	// ─── Peer connection ──────────────────────────────────────────────

	#createPeerConnection() {
		const pc = new RTCPeerConnection({
			iceServers: this.#iceServers,
			bundlePolicy: 'max-bundle'
		});
		this.#pc = pc;
		// rtcd forwards other participants' media only once this channel is
		// open (it rides the first offer) and uses it as the signaling lock.
		this.#sfu = new SfuChannel(pc.createDataChannel(SFU_CHANNEL_LABEL), (sdp) => {
			void this.signal(sdp);
		});
		const mic = this.#local.getAudioTracks()[0];
		if (mic) this.#audioSender = pc.addTrack(mic, this.#local);
		// The camera m-line exists from the start (even with the camera
		// off), so turning it on later needs no renegotiation.
		this.#videoSender = pc.addTransceiver(this.#local.getVideoTracks()[0] ?? 'video', {
			direction: 'sendonly',
			// Told before the connection exists, the demand already holds.
			sendEncodings: withVideoDemand(cameraEncodings(this.#teacher), this.#videoDemand),
			streams: [this.#local]
		}).sender;

		pc.onnegotiationneeded = () => void this.#negotiate();
		pc.onicecandidate = (e) => {
			if (e.candidate) send('ice', { data: JSON.stringify(e.candidate.toJSON()) });
		};
		pc.onconnectionstatechange = () => this.#onConnectionState();
		pc.ontrack = (e) => this.#onTrack(e);

		this.#quality = new QualityMonitor(pc, (quality) => (this.#state.quality = quality));
		this.#quality.start();
		window.addEventListener('online', this.#onNetworkBack);
	}

	/**
	 * The network came back: a media path that dropped (wifi change, sleep)
	 * is rebuilt now instead of waiting for the browser to give up on it.
	 */
	#onNetworkBack = () => {
		const state = this.#pc?.connectionState;
		if (state === 'disconnected' || state === 'failed') this.#pc?.restartIce();
	};

	async #negotiate() {
		const pc = this.#pc;
		if (!pc) return;
		try {
			// Hold the SFU's signaling lock until it answers (see signal()).
			await this.#sfu?.lock();
			this.#makingOffer = true;
			await pc.setLocalDescription();
			this.#sendDescription(pc.localDescription);
		} catch (err) {
			this.#sfu?.unlock();
			if (!this.#ignoreOffer) console.warn('[calls] negotiation failed', err);
		} finally {
			this.#makingOffer = false;
		}
	}

	/**
	 * Sends a description over the data channel while the connection is up
	 * (offers outgrow a websocket message), else over the websocket: the
	 * first offer creates the channel, and an ICE restart's offer is sent
	 * while the channel's transport is down.
	 */
	#sendDescription(desc: RTCSessionDescription | null) {
		if (!desc) return;
		// Serialize explicitly: some engines stringify the instance to {}.
		const data = JSON.stringify({ type: desc.type, sdp: desc.sdp });
		const sfu = this.#sfu;
		if (sfu?.open && this.#pc?.connectionState === 'connected') {
			sfu.sendSdp(data).catch(() => send('sdp', { data }));
		} else {
			send('sdp', { data });
		}
	}

	#onConnectionState() {
		const s = this.#state;
		switch (this.#pc?.connectionState) {
			case 'connected':
				clearTimeout(this.#connectTimer);
				clearTimeout(this.#failTimer);
				s.status = 'connected';
				// The SFU session exists now: tell it which cameras we show.
				this.#sendWantedVideo(true);
				break;
			case 'disconnected':
				s.status = 'reconnecting';
				break;
			case 'failed':
				// The websocket still works; try a new media path first.
				s.status = 'reconnecting';
				this.#pc?.restartIce();
				clearTimeout(this.#failTimer);
				this.#failTimer = setTimeout(() => {
					if (this.#pc?.connectionState !== 'connected') this.#fail('connection');
				}, ICE_RESTART_GRACE_MS);
				break;
		}
	}

	#onTrack(e: RTCTrackEvent) {
		if (e.track.kind === 'audio') {
			this.#remoteAudio.play(e.track, e.streams[0] ?? new MediaStream([e.track]));
		}
		// Cameras and screens are mapped from the SFU's description once it
		// is applied (#syncRemoteVideo), not from the event's ids.
	}

	/**
	 * Rebuilds who is on each receiving video slot from the SFU's current
	 * description: the cameras by participant, and the shared screen. Each
	 * slot gets its own stream, which moves with the slot to whoever the
	 * SFU now sends on it.
	 */
	#syncRemoteVideo() {
		const pc = this.#pc;
		const sdp = pc?.remoteDescription?.sdp;
		if (!pc || !sdp) return;
		const s = this.#state;
		const senders = remoteVideoSenders(sdp);
		const cameras = new Map<string, MediaStream>();
		let screen: { sessionId: string; stream: MediaStream } | null = null;
		for (const t of pc.getTransceivers()) {
			const origin = t.mid ? senders.get(t.mid) : undefined;
			if (!t.mid || !origin || t.receiver.track.kind !== 'video') continue;
			let stream = this.#slotStreams.get(t.mid);
			if (stream?.getVideoTracks()[0] !== t.receiver.track) {
				stream = new MediaStream([t.receiver.track]);
				this.#slotStreams.set(t.mid, stream);
			}
			if (origin.kind === 'screen') screen = { sessionId: origin.sessionId, stream };
			else if (origin.kind === 'video') cameras.set(origin.sessionId, stream);
		}
		for (const id of [...s.videoStreams.keys()])
			if (!cameras.has(id)) s.videoStreams.delete(id);
		for (const [id, stream] of cameras) {
			if (s.videoStreams.get(id) !== stream) s.videoStreams.set(id, stream);
		}
		// Our own share is local, not from the SFU.
		if (s.screen?.sessionId !== s.mySessionId) {
			if (s.screen?.sessionId !== screen?.sessionId || s.screen?.stream !== screen?.stream) {
				s.screen = screen;
			}
		}
	}

	/** SDP or ICE from the SFU (the `signal` event). */
	async signal(raw: unknown): Promise<void> {
		const pc = this.#pc;
		if (!pc) return;
		try {
			const msg = (typeof raw === 'string' ? JSON.parse(raw) : raw) as Record<
				string,
				unknown
			>;
			if (msg.type === 'offer' || msg.type === 'answer') {
				const description = msg as unknown as RTCSessionDescriptionInit;
				const ready =
					!this.#makingOffer && (pc.signalingState === 'stable' || this.#answerPending);
				this.#ignoreOffer = description.type === 'offer' && !ready;
				if (this.#ignoreOffer) return;
				this.#answerPending = description.type === 'answer';
				await pc.setRemoteDescription(description);
				this.#syncRemoteVideo();
				this.#answerPending = false;
				if (description.type === 'answer') this.#sfu?.unlock();
				for (const candidate of this.#queuedCandidates.splice(0))
					await pc.addIceCandidate(candidate);
				if (description.type === 'offer') {
					await pc.setLocalDescription();
					this.#sendDescription(pc.localDescription);
				}
			} else if (msg.candidate) {
				// rtcd wraps candidates: {"type":"candidate","candidate":{…}};
				// a flat candidate init is accepted too.
				const init = (
					typeof msg.candidate === 'object' ? { ...(msg.candidate as object) } : msg
				) as RTCIceCandidateInit;
				if (init.sdpMid == null && init.sdpMLineIndex == null) init.sdpMLineIndex = 0;
				if (pc.remoteDescription) await pc.addIceCandidate(init);
				else this.#queuedCandidates.push(init);
			}
		} catch (err) {
			console.warn('[calls] bad signal', err);
		}
	}
}
