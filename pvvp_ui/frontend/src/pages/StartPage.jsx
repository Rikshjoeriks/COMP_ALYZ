import { useState } from "react";
import { api } from "../api";
const SESSION_OPTIONS = ["MCAICE","MCAFHEV","MCAPHEV","EV","BEV"];

export default function StartPage({ sessionId, setSessionId, carId, setCarId, onContinue }) {
  const [text, setText] = useState("");
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");

  async function handleSave() {
    setMsg(""); setSaving(true);
    try {
      await api.post("/api/session/init", { sessionId, carId: carId || "NA" });
      await api.post("/api/input", { sessionId, text });
      setMsg("Saved input_raw.txt ✔");
    } catch (e) {
      setMsg(`Error: ${e.response?.data?.error || e.message}`);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="bg-white rounded-2xl shadow p-4">
      <div className="grid md:grid-cols-3 gap-4">
        <div>
          <label className="block text-sm font-medium mb-1">Vehicle Type</label>
          <select className="w-full border rounded px-2 py-1" value={sessionId} onChange={e=>setSessionId(e.target.value)}>
            {SESSION_OPTIONS.map(s=> <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Car ID</label>
          <input className="w-full border rounded px-2 py-1" value={carId} onChange={e=>setCarId(e.target.value)} />
        </div>
      </div>
      <div className="mt-4">
        <label className="block text-sm font-medium mb-1">Paste scraped text</label>
        <textarea className="w-full border rounded p-2 h-48 font-mono" value={text} onChange={e=>setText(e.target.value)} />
      </div>
      <div className="mt-4 flex gap-2">
        <button className="px-3 py-1.5 rounded bg-blue-600 text-white disabled:opacity-60" disabled={saving} onClick={handleSave}>
          {saving ? "Saving…" : "Save Input"}
        </button>
        <button className="px-3 py-1.5 rounded bg-gray-200" onClick={onContinue}>Continue → Normalize</button>
      </div>
      {msg && <p className="mt-3 text-sm">{msg}</p>}
    </div>
  );
}
