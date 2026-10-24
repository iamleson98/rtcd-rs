// One call participant in a browser frame, driven by the LMS web app's own CallClient. The page
// that hosts the frames (served by compat/interop in -browser mode) calls window.api.
import { CallClient } from 'LMS_CALL_CLIENT';
import { realtime } from './realtime-stub';

type Role = 'teacher' | 'speaker' | 'viewer' | 'viewer-low' | 'listener';

const params = new URLSearchParams(location.search);
const name = params.get('name') ?? 'anon';
const role = (params.get('role') ?? 'listener') as Role;
const call = params.get('call') ?? 'call';

// Browsers other than Chrome have no flag for fake devices: with ?synthetic=1 capture is a canvas
// that changes every frame and a tone, which any browser can make without permission.
function installSyntheticMedia(): void {
	const audioContext = new AudioContext();
	void audioContext.resume();
	navigator.mediaDevices.getUserMedia = async (constraints?: MediaStreamConstraints) => {
		const stream = new MediaStream();
		if (constraints?.audio) {
			const oscillator = audioContext.createOscillator();
			const destination = audioContext.createMediaStreamDestination();
			oscillator.connect(destination);
			oscillator.start();
			stream.addTrack(destination.stream.getAudioTracks()[0]);
		}
		if (constraints?.video) {
			const canvas = document.createElement('canvas');
			canvas.width = 640;
			canvas.height = 360;
			const context = canvas.getContext('2d')!;
			let frame = 0;
			const draw = () => {
				context.fillStyle = `hsl(${(frame * 7) % 360} 80% 50%)`;
				context.fillRect(0, 0, 640, 360);
				context.fillStyle = '#fff';
				context.font = '48px sans-serif';
				context.fillText(`${name} ${frame++}`, 20, 100);
			};
			draw();
			setInterval(draw, 33);
			stream.addTrack(canvas.captureStream(30).getVideoTracks()[0]);
		}
		return stream;
	};
}
if (params.get('synthetic') === '1') installSyntheticMedia();

// The client keeps its peer connection private; keep a handle to read its statistics.
const connections: RTCPeerConnection[] = [];
const NativePeerConnection = window.RTCPeerConnection;
window.RTCPeerConnection = class extends NativePeerConnection {
	constructor(configuration?: RTCConfiguration) {
		super(configuration);
		connections.push(this);
	}
};

class StubState {
	status = 'idle';
	error: string | null = null;
	channelId = `channel-${call}`;
	mySessionId = '';
	micOn = false;
	cameraOn = false;
	screenOn = false;
	handRaised = false;
	localCamera: MediaStream | null = null;
	screen: unknown = null;
	videoStreams = new Map<string, MediaStream>();
	quality: unknown = null;
	devices: unknown[] = [];
	problems: unknown[] = [];
	selected = { audioinput: '', videoinput: '', audiooutput: '' };
	get joined(): boolean {
		return this.status !== 'idle';
	}
	addProblem(problem: unknown): void {
		this.problems.push(problem);
	}
	dismissProblem(): void {}
	resetSession(): void {
		this.status = 'idle';
	}
}

const state = new StubState();
const client = new CallClient(state as never, { teacher: role === 'teacher' });
const ws = new WebSocket(`ws://${location.host}/ws?name=${name}&call=${call}`);
const opened = new Promise<void>((resolve) => (ws.onopen = () => resolve()));
realtime.attach((message) => ws.send(message));
ws.onmessage = (event) => {
	const { event: kind, data } = JSON.parse(event.data);
	switch (kind) {
		case 'custom_calls_join':
			client.handleJoinAck(data.connID, data.iceServers);
			break;
		case 'custom_calls_signal':
			void client.signal(data.data);
			break;
		case 'custom_calls_video_demand':
			client.setVideoDemand(data.demand);
			break;
	}
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function until(what: string, ms: number, done: () => boolean): Promise<void> {
	const end = performance.now() + ms;
	while (performance.now() < end) {
		if (done()) return;
		await sleep(50);
	}
	throw new Error(`${name}: ${what} (status ${state.status}, error ${state.error})`);
}

(window as never as { api: unknown }).api = {
	name,
	role,
	/** Joins and resolves with the milliseconds it took to be connected. */
	async join(): Promise<number> {
		await opened;
		const start = performance.now();
		await client.join({ mic: role !== 'listener', camera: role === 'teacher' || role === 'speaker' });
		await until('never connected', 30_000, () => state.status === 'connected');
		return performance.now() - start;
	},
	watch(teacherSession: string | null, small: boolean): void {
		client.setWantedVideo(teacherSession ? [teacherSession] : [], small ? [teacherSession!] : []);
	},
	setCamera: (on: boolean) => client.setCamera(on),
	setMic: (on: boolean) => client.setMic(on),
	leave(): void {
		client.leave();
		ws.close();
	},
	async report() {
		const out = {
			name,
			role,
			status: state.status,
			error: state.error,
			connection: connections[0]?.connectionState ?? 'none',
			cameras: [...state.videoStreams.keys()],
			audioIn: 0,
			audioBytesIn: 0,
			videoIn: [] as unknown[],
			videoOut: [] as unknown[],
		};
		const pc = connections[0];
		if (!pc) return out;
		(out as any).remoteSdp = pc.remoteDescription?.sdp ?? '';
		(out as any).localSdp = pc.localDescription?.sdp ?? '';
		// A browser can leave getStats unanswered on a connection in trouble: report what there is.
		const stats = await Promise.race([
			pc.getStats(),
			new Promise<RTCStatsReport>((resolve) => setTimeout(() => resolve(new Map() as never), 5000)),
		]);
		const codecOf = (s: any) => (s.codecId ? (stats.get(s.codecId) as any)?.mimeType : '') ?? '';
		stats.forEach((s: any) => {
			if (s.type === 'inbound-rtp' && s.kind === 'audio') {
				out.audioIn += s.packetsReceived ?? 0;
				out.audioBytesIn += s.bytesReceived ?? 0;
			} else if (s.type === 'inbound-rtp' && s.kind === 'video') {
				out.videoIn.push({
					packets: s.packetsReceived ?? 0,
					frames: s.framesDecoded ?? 0,
					width: s.frameWidth ?? 0,
					height: s.frameHeight ?? 0,
					lost: s.packetsLost ?? 0,
					last: s.lastPacketReceivedTimestamp ?? 0,
					codec: codecOf(s),
					keyframes: s.keyFramesDecoded ?? 0,
					plis: s.pliCount ?? 0,
					nacks: s.nackCount ?? 0,
				});
			} else if (s.type === 'outbound-rtp' && s.kind === 'video') {
				out.videoOut.push({
					rid: s.rid ?? '',
					packets: s.packetsSent ?? 0,
					frames: s.framesEncoded ?? 0,
					width: s.frameWidth ?? 0,
					codec: codecOf(s),
					plis: s.pliCount ?? 0,
					keyframes: s.keyFramesEncoded ?? 0,
				});
			}
		});
		return out;
	},
};
