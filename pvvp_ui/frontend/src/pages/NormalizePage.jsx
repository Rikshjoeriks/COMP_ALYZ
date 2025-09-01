import { useState } from "react";
import { api } from "../api";

export default function NormalizePage({ sessionId, onBack }) {
  const [running, setRunning] = useState(false);
  const [stdout, setStdout] = useState("");
  const [stderr, setStderr] = useState("");
  const [exitCode, setExitCode] = useState(null);
  const [raw, setRaw] = useState("");        // ⬅ NEW
  const [ping, setPing] = useState("");      // ⬅ NEW

  async function run() {
    setRunning(true);
    setStdout(""); setStderr(""); setExitCode(null); setRaw(""); // ⬅ reset raw
    try {
      const res = await api.post("/api/run/normalize", { sessionId });
      setRaw(JSON.stringify(res.data, null, 2));                 // ⬅ show raw
      setStdout(res.data?.stdout ?? "");
      setStderr(res.data?.stderr ?? "");
      setExitCode(
        typeof res.data?.exit === "number" ? res.data.exit : null
      );
    } catch (e) {
      const payload = e.response?.data;
      setRaw(JSON.stringify({ message: e.message, payload }, null, 2)); // ⬅ show error raw
      setStderr((payload?.error) ?? (payload ? JSON.stringify(payload) : e.message));
      setExitCode(-1);
    } finally {
      setRunning(false);
    }
  }

  async function pingBackend() {             // ⬅ tiny connectivity check
    try {
      const res = await api.get("/api/hello");
      setPing(JSON.stringify(res.data));
    } catch (e) {
      setPing(`PING ERROR: ${e.message}`);
    }
  }

  return (
    <div className="bg-white rounded-2xl shadow p-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Normalize (L03)</h2>
        <div className="flex gap-2">
          <button className="px-3 py-1.5 rounded bg-gray-200" onClick={onBack}>← Back</button>
          <button className="px-3 py-1.5 rounded bg-gray-200" onClick={pingBackend}>Ping</button>
          <button className="px-3 py-1.5 rounded bg-blue-600 text-white disabled:opacity-60" disabled={running} onClick={run}>
            {running ? "Running…" : "Run Normalize"}
          </button>
        </div>
      </div>

      <p className="mt-2 text-sm text-gray-600">Session: <span className="font-mono">{String(sessionId)}</span></p>

      <div className="mt-4 grid md:grid-cols-2 gap-4">
        <div>
          <div className="text-sm font-medium mb-1">stdout</div>
          <pre className="bg-gray-100 rounded p-2 h-56 overflow-auto whitespace-pre-wrap">{stdout}</pre>
        </div>
        <div>
          <div className="text-sm font-medium mb-1">stderr</div>
          <pre className="bg-gray-100 rounded p-2 h-56 overflow-auto whitespace-pre-wrap text-red-700">{stderr}</pre>
        </div>
      </div>

      {exitCode !== null && (
        <p className="mt-3 text-sm">
          Exit code: <span className={exitCode===0 ? "text-green-700" : "text-red-700"}>{exitCode}</span>
        </p>
      )}

      <div className="mt-4">
        <div className="text-sm font-medium mb-1">raw response (debug)</div>
        <pre className="bg-gray-100 rounded p-2 h-40 overflow-auto whitespace-pre-wrap">{raw}</pre>
      </div>

      <div className="mt-4">
        <div className="text-sm font-medium mb-1">ping (debug)</div>
        <pre className="bg-gray-100 rounded p-2">{ping}</pre>
      </div>
    </div>
  );
}

