import { useState } from "react";
import StartPage from "./pages/StartPage";
import NormalizePage from "./pages/NormalizePage";

export default function App() {
  const [sessionId, setSessionId] = useState("MCAFHEV");
  const [carId, setCarId] = useState("");
  const [step, setStep] = useState(0);

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="border-b bg-white">
        <div className="mx-auto max-w-5xl p-4 flex justify-between">
          <h1 className="text-xl font-bold">PVVP Orchestrator</h1>
          <div className="text-sm">Session: {sessionId} · Car ID: {carId || "—"}</div>
        </div>
      </header>
      <main className="mx-auto max-w-5xl p-4">
        <nav className="mb-4 text-sm">
          <ol className="flex gap-2">
            <li className={`px-2 py-1 rounded ${step===0?"bg-blue-100":"bg-gray-200"}`}>Start</li>
            <li className={`px-2 py-1 rounded ${step===1?"bg-blue-100":"bg-gray-200"}`}>Normalize</li>
          </ol>
        </nav>
        {step===0 && (
          <StartPage
            sessionId={sessionId} setSessionId={setSessionId}
            carId={carId} setCarId={setCarId}
            onContinue={()=>setStep(1)}
          />
        )}
        {step===1 && (
          <NormalizePage sessionId={sessionId} onBack={()=>setStep(0)} />
        )}
      </main>
    </div>
  );
}
